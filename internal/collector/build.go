// Copyright (c) F5, Inc.
//
// This source code is licensed under the Apache License, Version 2.0 license found in the
// LICENSE file in the root directory of this source tree.
package collector

import (
	"bytes"
	_ "embed"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"text/template"

	"github.com/nginx/agent/v3/internal/config"
	"go.opentelemetry.io/collector/component"
)

const (
	otelTemplatePath     = "otelcol.tmpl"
	configFilePermission = 0o600
	// tempConfigSuffix is appended to the collector config path while a new
	// configuration is rendered and validated. The temp file is only renamed
	// over the confirmed config path once the candidate is ready, so a crash
	// or a failed switch can never leave a half-written config behind.
	tempConfigSuffix = ".tmp"
)

//go:embed otelcol.tmpl
var otelcolTemplate string

// BuildInfo returns all the OTel collector BuildInfo supported
// based on https://github.com/DataDog/datadog-agent/blob/main/comp/otelcol/collector-contrib/impl/collectorcontrib.go
func BuildInfo(cfg *config.Config) component.BuildInfo {
	return component.BuildInfo{
		Version:     cfg.Version,
		Command:     "otel-nginx-agent",
		Description: "NGINX Agent OpenTelemetry Collector",
	}
}

// configURIs returns the ordered list of config URIs the collector must merge,
// using primary as the generated agent config location.
func configURIs(primary string, additional []string) []string {
	return slices.Concat([]string{primary}, additional)
}

// tempConfigPath returns the staged location used while a candidate config is
// being rendered and validated.
func tempConfigPath(confPath string) string {
	return filepath.Clean(confPath) + tempConfigSuffix
}

// cleanupStaleTempConfig removes a config staging file left behind by an
// aborted switch (for example after an agent crash) so that an agent restart
// only ever reads the last confirmed config.
func cleanupStaleTempConfig(confPath string) {
	stalePath := tempConfigPath(confPath)
	if err := os.Remove(stalePath); err != nil && !os.IsNotExist(err) {
		slog.Warn("Failed to remove stale OTel collector temp config",
			"temp_path", stalePath, "error", err)
	}
}

// normalizeCollectorConfig applies the in-memory pipeline normalizations that
// are implied by the generated collector configuration (default resource
// processor and default OTLP exporter name). It mutates the passed config and
// must only be called on a candidate copy or while rendering a rollback copy.
func normalizeCollectorConfig(conf *config.Collector) {
	if conf.Processors.Resource["default"] != nil {
		addDefaultResourceProcessor(conf.Pipelines.Metrics)
		addDefaultResourceProcessor(conf.Pipelines.Logs)
	}

	for _, pipeline := range conf.Pipelines.Metrics {
		if i := slices.Index(pipeline.Exporters, "otlp/default"); i != -1 {
			pipeline.Exporters[i] = config.DefaultOtlpGrpc
		}
	}
}

// renderCollectorConfig normalizes and renders the collector configuration to
// YAML bytes without touching the confirmed config file.
func renderCollectorConfig(conf *config.Collector) ([]byte, error) {
	normalizeCollectorConfig(conf)

	tmpl, err := template.New(otelTemplatePath).Parse(otelcolTemplate)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if executeErr := tmpl.Execute(&buf, conf); executeErr != nil {
		return nil, executeErr
	}

	return buf.Bytes(), nil
}

// stageCollectorConfig renders the candidate config and writes it next to the
// confirmed config path.
func stageCollectorConfig(conf *config.Collector) (string, error) {
	confPath := filepath.Clean(conf.ConfigPath)

	slog.Info("Writing OTel collector config", "config_path", confPath)

	data, err := renderCollectorConfig(conf)
	if err != nil {
		return "", err
	}

	tempPath := tempConfigPath(confPath)
	if writeErr := writeConfigFile(tempPath, data); writeErr != nil {
		_ = os.Remove(tempPath)

		return "", writeErr
	}

	return tempPath, nil
}

// publishConfig atomically replaces the confirmed config file with the staged
// candidate by renaming it into place (rename is atomic on the same
// filesystem) and syncing the containing directory.
func publishConfig(tempPath, confPath string) error {
	if err := os.Rename(tempPath, confPath); err != nil {
		return fmt.Errorf("unable to publish OTel collector config %q: %w", confPath, err)
	}

	if err := os.Chmod(confPath, configFilePermission); err != nil {
		slog.Warn("Failed to set OTel collector config file permissions",
			"config_path", confPath, "error", err)
	}

	syncParentDirectory(confPath)

	return nil
}

// writeConfigFile creates or truncates path with the given content using
// 0600 permissions and fsyncs the file so that the data is durable.
func writeConfigFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, configFilePermission)
	if err != nil {
		return err
	}

	if _, writeErr := file.Write(data); writeErr != nil {
		_ = file.Close()
		return writeErr
	}

	var fileErr error
	if syncErr := file.Sync(); syncErr != nil {
		fileErr = syncErr
	}

	if closeErr := file.Close(); closeErr != nil && fileErr == nil {
		fileErr = closeErr
	}

	return fileErr
}

// syncParentDirectory fsyncs the directory holding the given path so that a
// rename is persisted across crashes. Failures are best effort only.
func syncParentDirectory(path string) {
	dir, openErr := os.Open(filepath.Dir(path))
	if openErr != nil {
		return
	}
	defer dir.Close()

	if syncErr := dir.Sync(); syncErr != nil {
		slog.Debug("Failed to sync OTel collector config directory", "path", path, "error", syncErr)
	}
}

// writeCollectorConfig renders the configuration and atomically publishes it
// to the configured path.
func writeCollectorConfig(conf *config.Collector) error {
	confPath := filepath.Clean(conf.ConfigPath)

	tempPath, err := stageCollectorConfig(conf)
	if err != nil {
		return err
	}

	if publishErr := publishConfig(tempPath, confPath); publishErr != nil {
		_ = os.Remove(tempPath)
		return publishErr
	}

	return nil
}

func addDefaultResourceProcessor(pipelines map[string]*config.Pipeline) {
	for _, pipeline := range pipelines {
		if !slices.Contains(pipeline.Processors, "resource/default") {
			pipeline.Processors = append(pipeline.Processors, "resource/default")
		}
	}
}

// deepCopyConfig returns a fully independent copy of the agent config. All
// maps, slices and nested structs are duplicated so that a candidate can be
// mutated without affecting the last confirmed configuration.
func deepCopyConfig(src *config.Config) *config.Config {
	if src == nil {
		return nil
	}

	copied := deepCopyValue(reflect.ValueOf(src))
	if dst, ok := copied.Interface().(*config.Config); ok {
		return dst
	}

	return nil
}

// deepCopyValue recursively duplicates the value behind pointers, slices,
// maps, interfaces and structs. Scalar values (including named types such as
// time.Duration) are returned as-is because they are safe to share.
func deepCopyValue(v reflect.Value) reflect.Value {
	if !v.IsValid() {
		return v
	}

	switch v.Kind() {
	//nolint:govet // a shared pointer would alias the source, so the pointer must be deep-copied
	case reflect.Ptr:
		return copyPtr(v)
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}

		return deepCopyValue(v.Elem())
	case reflect.Struct:
		return copyStruct(v)
	case reflect.Map:
		return copyMap(v)
	case reflect.Slice:
		return copySlice(v)
	case reflect.Array:
		return copyArray(v)
	case reflect.Invalid,
		reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.String, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v
	default:
		return v
	}
}

func copyPtr(v reflect.Value) reflect.Value {
	if v.IsNil() {
		return reflect.Zero(v.Type())
	}

	dst := reflect.New(v.Elem().Type())
	dst.Elem().Set(deepCopyValue(v.Elem()))

	return dst
}

func copyStruct(v reflect.Value) reflect.Value {
	dst := reflect.New(v.Type()).Elem()
	for i := range v.NumField() {
		if dst.Field(i).CanSet() {
			dst.Field(i).Set(deepCopyValue(v.Field(i)))
		}
	}

	return dst
}

func copyMap(v reflect.Value) reflect.Value {
	if v.IsNil() {
		return reflect.Zero(v.Type())
	}

	dst := reflect.MakeMapWithSize(v.Type(), v.Len())
	iter := v.MapRange()
	for iter.Next() {
		dst.SetMapIndex(deepCopyValue(iter.Key()), deepCopyValue(iter.Value()))
	}

	return dst
}

func copySlice(v reflect.Value) reflect.Value {
	if v.IsNil() {
		return reflect.Zero(v.Type())
	}

	dst := reflect.MakeSlice(v.Type(), v.Len(), v.Cap())
	for i := range v.Len() {
		dst.Index(i).Set(deepCopyValue(v.Index(i)))
	}

	return dst
}

func copyArray(v reflect.Value) reflect.Value {
	dst := reflect.New(v.Type()).Elem()
	for i := range v.Len() {
		dst.Index(i).Set(deepCopyValue(v.Index(i)))
	}

	return dst
}
