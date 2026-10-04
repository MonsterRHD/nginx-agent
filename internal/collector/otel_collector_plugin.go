// Copyright (c) F5, Inc.
//
// This source code is licensed under the Apache License, Version 2.0 license found in the
// LICENSE file in the root directory of this source tree.

package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-yaml"
	pkgConfig "github.com/nginx/agent/v3/pkg/config"
	"go.opentelemetry.io/collector/confmap"

	"github.com/nginx/agent/v3/api/grpc/mpi/v1"
	"github.com/nginx/agent/v3/internal/backoff"
	"github.com/nginx/agent/v3/internal/bus"
	"github.com/nginx/agent/v3/internal/collector/types"
	"github.com/nginx/agent/v3/internal/config"
	"github.com/nginx/agent/v3/internal/model"
	"go.opentelemetry.io/collector/otelcol"
)

const (
	maxTimeToWaitForShutdown  = 30 * time.Second
	defaultCollectionInterval = 1 * time.Minute
	filePermission            = 0o600
	startupPollInterval       = 10 * time.Millisecond
	// To conform to the rfc3164 spec the timestamp in the logs need to be formatted correctly.
	// RFC 3164 timestamps carry no timezone and the syslog parser interprets them as UTC,
	// so the timestamp is always converted to UTC before it is formatted.
	// Single digit days are padded with a space, as required by rfc3164.
	// Here are some examples of what the timestamp conversions look like.

	// 2024-11-06T17:19:24+00:00 ---> Nov  6 17:19:24
	// 2024-11-16T17:19:24+00:00 ---> Nov 16 17:19:24
	// 2024-11-16T17:19:24+06:00 ---> Nov 16 11:19:24
	// 2024-11-01T03:19:24+06:00 ---> Oct 31 21:19:24
	timestampConversionExpression = `'EXPR(let timestamp = split(split(body, ">")[1], " ")[0]; ` +
		`let newTimestamp = date(timestamp).UTC().Format("Jan _2 15:04:05"); ` +
		`split(body, ">")[0] + ">" + newTimestamp + " " + split(body, " ", 2)[1])'`
	debugOTelConfigFile = "/opentelemetry-collector-agent-debug.yaml"
	insertAction        = "insert"
)

var (
	_                            bus.Plugin = (*Collector)(nil)
	initMutex                               = &sync.Mutex{}
	errNoReceiversConfigured                = errors.New("no receivers configured: waiting for receiver discovery")
	errCollectorShutdownTimedOut            = errors.New("timed out waiting for OTel collector to close")
)

type (
	// Collector The OTel collector plugin start an embedded OTel collector for metrics collection in the OTel format.
	Collector struct {
		service                 types.CollectorInterface
		config                  *config.Config
		cancel                  context.CancelFunc
		previousNAPSysLogServer string
		debugOTelConfigPath     string
		// switchMu serializes every configuration transaction (resource
		// updates, NGINX config updates and management plane header updates)
		// so that concurrent updates converge, in order, onto the latest
		// complete candidate and a switch can never interleave with another.
		switchMu sync.Mutex
		// stopped reports whether the last confirmed pipeline is running.
		stopped bool
		// closed is set by Close so that updates queued during shutdown are
		// discarded instead of starting a new pipeline.
		closed bool
	}
)

// NewCollector is the constructor for the Collector plugin.
func NewCollector(conf *config.Config) (*Collector, error) {
	initMutex.Lock()

	defer initMutex.Unlock()
	if conf == nil {
		return nil, errors.New("nil agent config")
	}

	if conf.Collector == nil {
		return nil, errors.New("nil collector config")
	}

	if conf.Collector.Log != nil && conf.Collector.Log.Path != "" && conf.Collector.Log.Path != "stdout" {
		err := os.WriteFile(conf.Collector.Log.Path, []byte{}, filePermission)
		if err != nil {
			return nil, err
		}
	}

	// An agent restart only reads the last confirmed config: remove a config
	// staging file that may have been left behind by an aborted switch.
	cleanupStaleTempConfig(conf.Collector.ConfigPath)

	settings := OTelCollectorSettings(conf)
	oTelCollector, err := otelcol.NewCollector(settings)
	if err != nil {
		return nil, err
	}

	debugOTelConfigPath := conf.LibDir + debugOTelConfigFile

	return &Collector{
		config:                  conf,
		service:                 oTelCollector,
		stopped:                 true,
		previousNAPSysLogServer: "",
		debugOTelConfigPath:     debugOTelConfigPath,
	}, nil
}

func (oc *Collector) State() otelcol.State {
	oc.switchMu.Lock()
	defer oc.switchMu.Unlock()

	if oc.service == nil {
		return otelcol.StateClosed
	}

	return oc.service.GetState()
}

// Init initializes and starts the plugin
func (oc *Collector) Init(ctx context.Context, _ bus.MessagePipeInterface) error {
	slog.InfoContext(ctx, "Starting OTel Collector plugin")

	runCtx, cancel := context.WithCancel(ctx)

	oc.switchMu.Lock()
	defer oc.switchMu.Unlock()

	oc.cancel = cancel

	if !oc.config.AreReceiversConfigured() {
		slog.InfoContext(runCtx, "No receivers configured for OTel Collector. "+
			"Waiting to discover a receiver before starting OTel collector.")

		return nil
	}

	if oc.config.Collector.Receivers.OtlpReceivers != nil {
		oc.processReceivers(runCtx, oc.config.Collector.Receivers.OtlpReceivers)
	}

	if !oc.stopped {
		return errors.New("OTel collector already running")
	}

	if err := oc.applyCandidate(runCtx, oc.config); err != nil {
		slog.ErrorContext(runCtx, "Unable to start OTel Collector", "error", err)
		return fmt.Errorf("start OTel Collector: %w", err)
	}

	return nil
}

// Info the plugin.
func (oc *Collector) Info() *bus.Info {
	return &bus.Info{
		Name: "collector",
	}
}

// Close the plugin.
func (oc *Collector) Close(ctx context.Context) error {
	slog.InfoContext(ctx, "Closing OTel Collector plugin")

	oc.switchMu.Lock()
	defer oc.switchMu.Unlock()

	oc.closed = true

	return oc.shutdownCollector(ctx)
}

// Process an incoming Message Bus message in the plugin
func (oc *Collector) Process(ctx context.Context, msg *bus.Message) {
	switch msg.Topic {
	case bus.NginxConfigUpdateTopic:
		oc.handleNginxConfigUpdate(ctx, msg)
	case bus.ResourceUpdateTopic:
		oc.handleResourceUpdate(ctx, msg)
	default:
		slog.DebugContext(ctx, "OTel collector plugin unknown topic", "topic", msg.Topic)
	}
}

// Subscriptions returns the list of topics the plugin is subscribed to
func (oc *Collector) Subscriptions() []string {
	return []string{
		bus.ResourceUpdateTopic,
		bus.NginxConfigUpdateTopic,
	}
}

// Reconfigure handles an agent config update coming from the management plane.
// Only collector header changes require a collector restart; the restart uses
// the same validated, reversible switch as runtime updates so that a failed
// restart never replaces or corrupts the last healthy pipeline.
func (oc *Collector) Reconfigure(ctx context.Context, agentConfig *config.Config) error {
	slog.DebugContext(ctx, "OTel collector plugin received agent config update message")

	if agentConfig == nil || agentConfig.Collector == nil {
		return nil
	}

	oc.switchMu.Lock()
	defer oc.switchMu.Unlock()

	if oc.closed {
		return nil
	}

	if !collectorHeadersChanged(oc.config, agentConfig) {
		return nil
	}

	candidate := deepCopyConfig(agentConfig)
	oc.preserveRuntimeState(candidate)

	if !candidate.AreReceiversConfigured() {
		slog.InfoContext(ctx, "OTel collector headers changed but no receivers are configured; "+
			"waiting to discover a receiver before restarting")

		return nil
	}

	slog.InfoContext(ctx, "OTel collector headers have changed, restarting collector")
	if err := oc.applyCandidate(ctx, candidate); err != nil {
		if errors.Is(err, errNoReceiversConfigured) {
			return nil
		}

		slog.ErrorContext(ctx, "Failed to restart OTel collector with updated headers; "+
			"keeping last healthy config", "error", err)
	}

	return nil
}

// collectorHeadersChanged reports whether the headers_setter headers differ
// between two agent configs, treating missing extensions as nil headers.
func collectorHeadersChanged(current, updated *config.Config) bool {
	var currentHeaders, updatedHeaders []config.Header

	if current != nil && current.Collector != nil &&
		current.Collector.Extensions.HeadersSetter != nil {
		currentHeaders = current.Collector.Extensions.HeadersSetter.Headers
	}

	if updated != nil && updated.Collector != nil &&
		updated.Collector.Extensions.HeadersSetter != nil {
		updatedHeaders = updated.Collector.Extensions.HeadersSetter.Headers
	}

	return !reflect.DeepEqual(currentHeaders, updatedHeaders)
}

// preserveRuntimeState carries state that only exists at runtime (discovered
// receivers, resource attributes and the uuid header) from the last confirmed
// config into a management plane candidate, which would otherwise only
// contain statically configured components.
func (oc *Collector) preserveRuntimeState(candidate *config.Config) {
	if oc.config == nil || oc.config.Collector == nil || candidate.Collector == nil {
		return
	}

	lastCollectors := oc.config.Collector.Receivers
	candidateReceivers := &candidate.Collector.Receivers

	candidateReceivers.NginxReceivers = copyOf(lastCollectors.NginxReceivers)
	candidateReceivers.NginxPlusReceivers = copyOf(lastCollectors.NginxPlusReceivers)
	candidateReceivers.CertificateReceivers = copyOf(lastCollectors.CertificateReceivers)

	if napReceiver, ok := lastCollectors.TcplogReceivers["nginx_app_protect"]; ok && napReceiver != nil {
		if candidateReceivers.TcplogReceivers == nil {
			candidateReceivers.TcplogReceivers = make(map[string]*config.TcplogReceiver)
		}

		candidateReceivers.TcplogReceivers["nginx_app_protect"] = copyOf(napReceiver)
	}

	oc.preserveRuntimeResourceAttributes(candidate)
	oc.preserveUUIDHeader(candidate)
}

func (oc *Collector) preserveRuntimeResourceAttributes(candidate *config.Config) {
	lastResourceProcessors := oc.config.Collector.Processors.Resource
	lastDefault, ok := lastResourceProcessors["default"]
	if !ok || lastDefault == nil {
		return
	}

	if candidate.Collector.Processors.Resource == nil {
		candidate.Collector.Processors.Resource = make(map[string]*config.Resource)
	}

	candidateDefault, exists := candidate.Collector.Processors.Resource["default"]
	if !exists || candidateDefault == nil {
		candidate.Collector.Processors.Resource["default"] = copyOf(lastDefault)
		return
	}

	for _, attribute := range lastDefault.Attributes {
		if !containsResourceAttribute(candidateDefault.Attributes, attribute.Key) {
			candidateDefault.Attributes = append(candidateDefault.Attributes, copyOf(attribute))
		}
	}
}

func (oc *Collector) preserveUUIDHeader(candidate *config.Config) {
	if oc.config.Collector.Extensions.HeadersSetter == nil {
		return
	}

	uuidHeader := findHeader(oc.config.Collector.Extensions.HeadersSetter.Headers, "uuid")
	if uuidHeader == nil {
		return
	}

	if candidate.Collector.Extensions.HeadersSetter == nil {
		candidate.Collector.Extensions.HeadersSetter = &config.HeadersSetter{}
	}

	if !containsHeader(candidate.Collector.Extensions.HeadersSetter.Headers, "uuid") {
		candidate.Collector.Extensions.HeadersSetter.Headers = append(
			candidate.Collector.Extensions.HeadersSetter.Headers,
			copyOf(*uuidHeader),
		)
	}
}

func containsResourceAttribute(attributes []config.ResourceAttribute, key string) bool {
	for _, attribute := range attributes {
		if attribute.Key == key {
			return true
		}
	}

	return false
}

func findHeader(headers []config.Header, key string) *config.Header {
	for i := range headers {
		if headers[i].Key == key {
			return &headers[i]
		}
	}

	return nil
}

func containsHeader(headers []config.Header, key string) bool {
	return findHeader(headers, key) != nil
}

// shutdownCollector stops the currently running collector. The caller must
// hold switchMu. Errors are logged but not returned so that plugin shutdown
// cannot be blocked; the state flag is only flipped once the collector is
// closed.
func (oc *Collector) shutdownCollector(ctx context.Context) error {
	if !oc.stopped && oc.service != nil {
		if err := oc.stopService(ctx, oc.service, oc.cancel); err != nil {
			slog.ErrorContext(ctx, "Failed to shutdown OTel Collector", "error", err)
		} else {
			oc.stopped = true
		}
	}

	return nil
}

// stopService asks a running collector to shut down and waits for it to reach
// the closed state. Shutdown is executed in a goroutine so that a stuck
// shutdown cannot block forever; the run context is canceled as well.
func (oc *Collector) stopService(
	ctx context.Context,
	svc types.CollectorInterface,
	cancel context.CancelFunc,
) error {
	if svc == nil {
		return nil
	}

	slog.InfoContext(ctx, "Shutting down OTel Collector", "state", svc.GetState())

	go func() {
		svc.Shutdown()
	}()

	if cancel != nil {
		cancel()
	}

	settings := *oc.config.Client.Backoff
	if settings.MaxElapsedTime <= 0 || settings.MaxElapsedTime > maxTimeToWaitForShutdown {
		settings.MaxElapsedTime = maxTimeToWaitForShutdown
	}
	waitErr := backoff.WaitUntil(ctx, &settings, func() error {
		if svc.GetState() == otelcol.StateClosed {
			return nil
		}

		return errors.New("OTel Collector not in a closed state yet")
	})
	if waitErr != nil {
		slog.ErrorContext(ctx, "Failed to shutdown OTel Collector", "error", waitErr, "state", svc.GetState())
		return errCollectorShutdownTimedOut
	}

	slog.InfoContext(ctx, "OTel Collector shutdown", "state", svc.GetState())

	return nil
}

// Process receivers and log warning for sub-optimal configurations
func (oc *Collector) processReceivers(ctx context.Context, receivers map[string]*config.OtlpReceiver) {
	for _, receiver := range receivers {
		if receiver.OtlpTLSConfig == nil {
			slog.WarnContext(ctx, "OTel receiver is configured without TLS. Connections are unencrypted.")
			continue
		}

		if receiver.OtlpTLSConfig.GenerateSelfSignedCert {
			slog.WarnContext(ctx,
				"Self-signed certificate for OTel receiver requested, "+
					"this is not recommended for production environments.",
			)

			if receiver.OtlpTLSConfig.ExistingCert {
				slog.WarnContext(ctx,
					"Certificate file already exists, skipping self-signed certificate generation",
				)
			}
		} else {
			slog.WarnContext(ctx, "OTel receiver is configured without TLS. Connections are unencrypted.")
		}
	}
}

// bootup starts the provided collector and returns only once it is running.
// It never mutates Collector state: the caller is responsible for committing
// the service once boot succeeds, or for rolling it back on error.
func (oc *Collector) bootup(ctx context.Context, service types.CollectorInterface, conf *config.Config) error {
	if service == nil {
		return errors.New("unable to start OTel collector: service is nil")
	}

	errChan := make(chan error, 1)

	go func() {
		if conf.IsCommandServerProxyConfigured() {
			oc.setProxyIfNeeded(ctx, conf)
		}

		slog.InfoContext(ctx, "Starting OTel collector")
		if err := service.Run(ctx); err != nil {
			errChan <- err
		}
		slog.InfoContext(ctx, "OTel collector has stopped running")
	}()

	ticker := time.NewTicker(startupPollInterval)
	defer ticker.Stop()

	for {
		select {
		case err := <-errChan:
			return err
		case <-ticker.C:
		}

		state := service.GetState()
		switch state {
		case otelcol.StateStarting, otelcol.StateClosing:
			continue
		case otelcol.StateRunning:
			return nil
		case otelcol.StateClosed:
			return errors.New("OTel collector entered closed state while starting")
		default:
			return fmt.Errorf("unable to start, otelcol state is %s", state)
		}
	}
}

// applyUpdate serializes a runtime configuration event onto the switch lock.
// The event is applied to an independent candidate copy; the last confirmed
// config and running pipeline are only replaced after the candidate has been
// validated and switched over successfully.
func (oc *Collector) applyUpdate(ctx context.Context, reason string, mutate func(candidate *config.Config) bool) {
	oc.switchMu.Lock()
	defer oc.switchMu.Unlock()

	if oc.closed {
		slog.DebugContext(ctx, "Ignoring OTel collector config update, plugin is closed")
		return
	}

	previousNAPServer := oc.previousNAPSysLogServer
	candidate := deepCopyConfig(oc.config)

	if !mutate(candidate) {
		return
	}

	if err := oc.applyCandidate(ctx, candidate); err != nil {
		// The NAP syslog probe cache belongs to committed runtime state; a
		// rejected candidate must not move it forward.
		oc.previousNAPSysLogServer = previousNAPServer

		if errors.Is(err, errNoReceiversConfigured) {
			slog.InfoContext(ctx, "Candidate OTel config has no receivers; keeping the last healthy "+
				"pipeline and waiting for receiver discovery", "reason", reason)

			return
		}

		slog.ErrorContext(ctx, "Failed to switch OTel collector config; keeping the last healthy config",
			"reason", reason, "error", err)
	}
}

func (oc *Collector) handleNginxConfigUpdate(ctx context.Context, msg *bus.Message) {
	slog.DebugContext(ctx, "OTel collector plugin received nginx config update message")

	nginxConfigContext, ok := msg.Data.(*model.NginxConfigContext)
	if !ok {
		slog.ErrorContext(ctx, "Unable to cast message payload to *model.NginxConfigContext", "payload", msg.Data)
		return
	}

	oc.applyUpdate(ctx, "nginx config updated", func(candidate *config.Config) bool {
		return oc.checkForNewReceivers(ctx, candidate, nginxConfigContext)
	})
}

func (oc *Collector) handleResourceUpdate(ctx context.Context, msg *bus.Message) {
	slog.DebugContext(ctx, "OTel collector plugin received resource update message")

	resourceUpdateContext, ok := msg.Data.(*v1.Resource)
	if !ok {
		slog.ErrorContext(ctx, "Unable to cast message payload to *v1.Resource", "payload", msg.Data)
		return
	}

	oc.applyUpdate(ctx, "resource updated", func(candidate *config.Config) bool {
		// Call both updaters unconditionally so a resource id update does not
		// short-circuit the uuid header update.
		resourceProcessorUpdated := oc.updateResourceProcessor(candidate, resourceUpdateContext)
		headersSetterExtensionUpdated := oc.updateHeadersSetterExtension(ctx, candidate, resourceUpdateContext)

		return resourceProcessorUpdated || headersSetterExtensionUpdated
	})
}

// applyCandidate performs the validated, reversible configuration switch. The
// caller must hold switchMu. The candidate is rendered, parsed and validated
// first; the last healthy collector keeps running until the candidate is
// ready. If stopping the old collector or starting the new one fails, the last
// healthy config is restored on disk and a fresh collector running that config
// is started, so in-memory config, disk config, receiver set and runtime state
// only ever reflect a single confirmed version.
func (oc *Collector) applyCandidate(ctx context.Context, candidate *config.Config) error {
	if candidate == nil || candidate.Collector == nil {
		return errors.New("nil OTel collector candidate config")
	}

	if !candidate.AreReceiversConfigured() {
		return errNoReceiversConfigured
	}

	confPath := filepath.Clean(candidate.Collector.ConfigPath)

	// Stage 1: render the candidate and stage it next to the confirmed config.
	tempPath, err := stageCollectorConfig(candidate.Collector)
	if err != nil {
		return fmt.Errorf("render OTel collector config candidate: %w", err)
	}

	// Stage 2: parse and validate the staged candidate. The running pipeline
	// is not touched at all at this point.
	if validationErr := oc.validateCandidate(ctx, candidate, tempPath); validationErr != nil {
		_ = os.Remove(tempPath)
		return validationErr
	}

	// Snapshot everything required to roll back to the last healthy version.
	rollback := oc.newRollbackState(ctx, confPath)

	// Stages 3 and 4: retire the running collector and atomically publish the
	// new config, restoring the last healthy pipeline on any failure.
	if switchErr := oc.retireAndPublish(ctx, tempPath, rollback); switchErr != nil {
		return switchErr
	}

	// Stage 5: build and start the new collector.
	newService, cancel, startErr := oc.startCollector(ctx, candidate)
	if startErr != nil {
		restoreErr := oc.rollbackAfterFailedStart(ctx, rollback)

		return fmt.Errorf("%w (restore error: %v)", startErr, restoreErr)
	}

	// Stage 6: commit the new version. Only after a successful switch do the
	// in-memory config, disk config, receiver set and runtime state reflect
	// the candidate.
	oc.config = candidate
	oc.service = newService
	oc.cancel = cancel
	oc.stopped = false

	return nil
}

// rollbackState captures the last confirmed version that a failed switch can
// be reverted to: its in-memory config, the running collector instance, the
// bytes currently published on disk and whether a config file existed at all.
type rollbackState struct {
	config      *config.Config
	service     types.CollectorInterface
	cancel      context.CancelFunc
	confPath    string
	configData  []byte
	diskExisted bool
	wasRunning  bool
}

// newRollbackState snapshots the current confirmed state and renders the last
// healthy config to bytes so that it can be republished during a rollback.
func (oc *Collector) newRollbackState(ctx context.Context, confPath string) *rollbackState {
	_, diskStatErr := os.Stat(confPath)
	state := &rollbackState{
		config:      oc.config,
		service:     oc.service,
		cancel:      oc.cancel,
		confPath:    confPath,
		diskExisted: diskStatErr == nil,
		wasRunning:  !oc.stopped && oc.service != nil,
	}
	state.configData = state.renderConfig(ctx)

	return state
}

// renderConfig renders the last healthy config to bytes from an independent
// copy so that rollback rendering never mutates the confirmed in-memory
// config.
func (r *rollbackState) renderConfig(ctx context.Context) []byte {
	if !r.diskExisted || r.config == nil {
		return nil
	}

	rollbackCopy := deepCopyConfig(r.config)
	data, err := renderCollectorConfig(rollbackCopy.Collector)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to render last healthy OTel collector config for rollback",
			"error", err)

		return nil
	}

	return data
}

// retireAndPublish stops the currently running collector (if any) and
// atomically publishes the staged candidate over the confirmed config path.
// Any failure restores the last healthy pipeline and returns an error; the
// in-memory config is left untouched by this method.
func (oc *Collector) retireAndPublish(ctx context.Context, tempPath string, rollback *rollbackState) error {
	if rollback.wasRunning {
		if shutdownErr := oc.stopService(ctx, rollback.service, rollback.cancel); shutdownErr != nil {
			_ = os.Remove(tempPath)
			restoreErr := oc.restoreLastHealthy(ctx, rollback)

			return fmt.Errorf("shutdown retiring OTel collector: %w (restore error: %v)",
				shutdownErr, restoreErr)
		}

		oc.stopped = true
	}

	if publishErr := publishConfig(tempPath, rollback.confPath); publishErr != nil {
		_ = os.Remove(tempPath)
		var restoreErr error
		if rollback.wasRunning {
			restoreErr = oc.restoreLastHealthy(ctx, rollback)
		}

		return fmt.Errorf("publish OTel collector config: %w (restore error: %v)",
			publishErr, restoreErr)
	}

	return nil
}

// startCollector builds a collector for the candidate and brings it up to the
// running state without touching any Collector field: the caller commits the
// returned service only after a successful switch.
//
//nolint:ireturn // returns the concrete collector abstraction used by the plugin
func (oc *Collector) startCollector(
	ctx context.Context,
	candidate *config.Config,
) (types.CollectorInterface, context.CancelFunc, error) {
	settings := OTelCollectorSettings(candidate)
	oc.writeDebugConfigIfEnabled(ctx, candidate, settings)

	newService, err := otelcol.NewCollector(settings)
	if err != nil {
		return nil, nil, fmt.Errorf("create new OTel collector: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	if bootErr := oc.bootup(runCtx, newService, candidate); bootErr != nil {
		cancel()

		return nil, nil, fmt.Errorf("start new OTel collector: %w", bootErr)
	}

	return newService, cancel, nil
}

// validateCandidate parses the staged config with the same factories and URI
// merge order as the running collector and performs a dry-run validation of
// the full component graph, without starting the collector or binding ports.
func (oc *Collector) validateCandidate(ctx context.Context, candidate *config.Config, tempPath string) error {
	settings := OTelCollectorSettings(candidate)
	settings.ConfigProviderSettings.ResolverSettings.URIs = configURIs(
		tempPath, candidate.Collector.AdditionalConfigPaths,
	)

	validator, err := otelcol.NewCollector(settings)
	if err != nil {
		return fmt.Errorf("build OTel collector config validator: %w", err)
	}

	if dryRunErr := validator.DryRun(ctx); dryRunErr != nil {
		return fmt.Errorf("invalid OTel collector config candidate: %w", dryRunErr)
	}

	return nil
}

func (oc *Collector) writeDebugConfigIfEnabled(
	ctx context.Context,
	candidate *config.Config,
	settings otelcol.CollectorSettings,
) {
	if candidate.Log == nil || strings.ToLower(candidate.Log.Level) != "debug" {
		return
	}

	if err := oc.writeRunningConfig(ctx, settings); err != nil {
		slog.ErrorContext(ctx, "Failed to write debug OTel Collector config", "error", err)
	}
}

// rollbackAfterFailedStart restores the last healthy disk config and, if a
// pipeline was running before the switch, restarts the last healthy collector.
func (oc *Collector) rollbackAfterFailedStart(ctx context.Context, rollback *rollbackState) error {
	rollback.republish(ctx)

	if !rollback.wasRunning {
		oc.stopped = true

		return nil
	}

	return oc.restoreLastHealthy(ctx, rollback)
}

// restoreLastHealthy re-publishes the last healthy config and starts a fresh
// collector with it, re-providing the collection that was live before the
// failed switch. It must not be called with a nil lastGood config.
func (oc *Collector) restoreLastHealthy(ctx context.Context, rollback *rollbackState) error {
	if rollback.config == nil {
		return errors.New("no last healthy OTel collector config to restore")
	}

	rollback.republish(ctx)

	slog.InfoContext(ctx, "Restoring last healthy OTel collector configuration")

	service, err := otelcol.NewCollector(OTelCollectorSettings(rollback.config))
	if err != nil {
		return fmt.Errorf("recreate last healthy OTel collector: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	if bootErr := oc.bootup(runCtx, service, rollback.config); bootErr != nil {
		cancel()

		return fmt.Errorf("restart last healthy OTel collector: %w", bootErr)
	}

	oc.service = service
	oc.cancel = cancel
	oc.stopped = false

	return nil
}

// republish puts the last confirmed config bytes back on disk atomically, or
// removes the config file if no confirmed config existed before the switch.
func (r *rollbackState) republish(ctx context.Context) {
	if !r.diskExisted {
		if err := os.Remove(r.confPath); err != nil && !os.IsNotExist(err) {
			slog.ErrorContext(ctx,
				"Failed to remove unconfirmed OTel collector config during rollback",
				"config_path", r.confPath, "error", err)
		}

		return
	}

	tempPath := tempConfigPath(r.confPath)
	if err := writeConfigFile(tempPath, r.configData); err != nil {
		slog.ErrorContext(ctx,
			"Failed to stage last healthy OTel collector config during rollback",
			"config_path", r.confPath, "error", err)

		return
	}

	if err := publishConfig(tempPath, r.confPath); err != nil {
		_ = os.Remove(tempPath)
		slog.ErrorContext(ctx,
			"Failed to republish last healthy OTel collector config",
			"config_path", r.confPath, "error", err)
	}
}

func (oc *Collector) updateResourceProcessor(cfg *config.Config, resourceUpdateContext *v1.Resource) bool {
	resourceProcessorUpdated := false

	if cfg.Collector.Processors.Resource == nil {
		cfg.Collector.Processors.Resource = make(map[string]*config.Resource)
		cfg.Collector.Processors.Resource["default"] = &config.Resource{
			Attributes: make([]config.ResourceAttribute, 0),
		}
	}

	if cfg.Collector.Processors.Resource["default"] != nil &&
		resourceUpdateContext.GetResourceId() != "" {
		resourceProcessorUpdated = oc.updateResourceAttributes(
			cfg,
			[]config.ResourceAttribute{
				{
					Key:    "resource.id", //nolint:goconst // value is local to this function
					Action: insertAction,
					Value:  resourceUpdateContext.GetResourceId(),
				},
			},
		)
	}

	return resourceProcessorUpdated
}

func (oc *Collector) updateHeadersSetterExtension(
	ctx context.Context,
	cfg *config.Config,
	resourceUpdateContext *v1.Resource,
) bool {
	headersSetterExtensionUpdated := false

	if cfg.Collector.Extensions.HeadersSetter != nil &&
		cfg.Collector.Extensions.HeadersSetter.Headers != nil {
		isUUIDHeaderSet := false
		for _, header := range cfg.Collector.Extensions.HeadersSetter.Headers {
			if header.Key == "uuid" { //nolint:goconst // value is local to this function
				isUUIDHeaderSet = true
				break
			}
		}

		if !isUUIDHeaderSet {
			slog.DebugContext(
				ctx, "Adding uuid header to OTel collector",
				"uuid", resourceUpdateContext.GetResourceId(),
			)
			cfg.Collector.Extensions.HeadersSetter.Headers = append(
				cfg.Collector.Extensions.HeadersSetter.Headers,
				config.Header{
					Action: insertAction,
					Key:    "uuid",
					Value:  resourceUpdateContext.GetResourceId(),
				},
			)

			headersSetterExtensionUpdated = true
		}
	}

	return headersSetterExtensionUpdated
}

func (oc *Collector) writeRunningConfig(ctx context.Context, settings otelcol.CollectorSettings) error {
	slog.DebugContext(ctx, "Writing running OTel collector config", "path",
		oc.debugOTelConfigPath)
	resolver, err := confmap.NewResolver(settings.ConfigProviderSettings.ResolverSettings)
	if err != nil {
		return fmt.Errorf("unable to create resolver: %w", err)
	}

	con, err := resolver.Resolve(ctx)
	if err != nil {
		return fmt.Errorf("error while resolving config: %w", err)
	}
	b, err := yaml.Marshal(con.ToStringMap())
	if err != nil {
		return fmt.Errorf("error while marshaling to YAML: %w", err)
	}

	writeErr := os.WriteFile(oc.debugOTelConfigPath, b, filePermission)
	if writeErr != nil {
		return fmt.Errorf("error while writing debug config: %w", writeErr)
	}

	return nil
}

func (oc *Collector) setProxyIfNeeded(ctx context.Context, conf *config.Config) {
	if conf.Collector.Exporters.OtlpExporters != nil ||
		conf.Collector.Exporters.PrometheusExporter != nil {
		// Set proxy env vars for OTLP exporter if proxy is configured.
		oc.setExporterProxyEnvVars(ctx, conf)
	}
}

func (oc *Collector) checkForNewReceivers(
	ctx context.Context,
	cfg *config.Config,
	nginxConfigContext *model.NginxConfigContext,
) bool {
	nginxReceiverFound, reloadCollector := oc.updateExistingNginxPlusReceiver(cfg, nginxConfigContext)

	if !nginxReceiverFound && nginxConfigContext.PlusAPI.URL != "" {
		slog.DebugContext(ctx, "Adding new NGINX Plus receiver", "url", nginxConfigContext.PlusAPI.URL)
		cfg.Collector.Receivers.NginxPlusReceivers = append(
			cfg.Collector.Receivers.NginxPlusReceivers,
			config.NginxPlusReceiver{
				InstanceID: nginxConfigContext.InstanceID,
				PlusAPI: config.APIDetails{
					URL:      nginxConfigContext.PlusAPI.URL,
					Listen:   nginxConfigContext.PlusAPI.Listen,
					Location: nginxConfigContext.PlusAPI.Location,
					Ca:       nginxConfigContext.PlusAPI.Ca,
				},
				CollectionInterval: defaultCollectionInterval,
			},
		)
		slog.DebugContext(ctx, "NGINX Plus API found, NGINX Plus receiver enabled to scrape metrics")

		reloadCollector = true
	} else if nginxConfigContext.PlusAPI.URL == "" {
		slog.WarnContext(ctx, "NGINX Plus API is not configured, searching for stub status endpoint")
		reloadCollector = oc.addNginxOssReceiver(ctx, cfg, nginxConfigContext)
	}

	if cfg.IsFeatureEnabled(pkgConfig.FeatureLogsNap) {
		tcplogReceiversFound := oc.updateNginxAppProtectTcplogReceivers(ctx, cfg, nginxConfigContext)
		if tcplogReceiversFound {
			reloadCollector = true
		}
	} else {
		slog.DebugContext(ctx, "NAP logs feature disabled", "enabled_features", cfg.Features)
	}

	if cfg.Collector.Receivers.CertificateMetrics != nil {
		reloadCollector = reloadCollector || oc.updateCertificateReceivers(cfg, nginxConfigContext)
	} else if len(cfg.Collector.Receivers.CertificateReceivers) > 0 {
		cfg.Collector.Receivers.CertificateReceivers = nil
		reloadCollector = true
	}

	return reloadCollector
}

func (oc *Collector) addNginxOssReceiver(
	ctx context.Context,
	cfg *config.Config,
	nginxConfigContext *model.NginxConfigContext,
) bool {
	nginxReceiverFound, reloadCollector := oc.updateExistingNginxOSSReceiver(cfg, nginxConfigContext)

	if !nginxReceiverFound && nginxConfigContext.StubStatus.URL != "" {
		slog.DebugContext(ctx, "Adding new NGINX OSS receiver", "url", nginxConfigContext.StubStatus.URL)
		cfg.Collector.Receivers.NginxReceivers = append(
			cfg.Collector.Receivers.NginxReceivers,
			config.NginxReceiver{
				InstanceID: nginxConfigContext.InstanceID,
				StubStatus: config.APIDetails{
					URL:      nginxConfigContext.StubStatus.URL,
					Listen:   nginxConfigContext.StubStatus.Listen,
					Location: nginxConfigContext.StubStatus.Location,
				},
				AccessLogs:         toConfigAccessLog(nginxConfigContext.AccessLogs),
				CollectionInterval: defaultCollectionInterval,
			},
		)
		slog.DebugContext(ctx, "Stub status endpoint found, OSS receiver enabled to scrape metrics")

		reloadCollector = true
	} else if nginxConfigContext.StubStatus.URL == "" {
		slog.WarnContext(ctx, "Stub status endpoint not found, NGINX metrics not available")
	}

	return reloadCollector
}

func (oc *Collector) updateExistingNginxPlusReceiver(
	cfg *config.Config,
	nginxConfigContext *model.NginxConfigContext,
) (nginxReceiverFound, reloadCollector bool) {
	for index, nginxPlusReceiver := range cfg.Collector.Receivers.NginxPlusReceivers {
		if nginxPlusReceiver.InstanceID == nginxConfigContext.InstanceID {
			nginxReceiverFound = true

			if nginxPlusReceiver.PlusAPI.URL != nginxConfigContext.PlusAPI.URL {
				cfg.Collector.Receivers.NginxPlusReceivers = append(
					cfg.Collector.Receivers.NginxPlusReceivers[:index],
					cfg.Collector.Receivers.NginxPlusReceivers[index+1:]...,
				)
				if nginxConfigContext.PlusAPI.URL != "" {
					slog.Debug("Updating existing NGINX Plus receiver", "url",
						nginxConfigContext.PlusAPI.URL)
					nginxPlusReceiver.PlusAPI.URL = nginxConfigContext.PlusAPI.URL
					cfg.Collector.Receivers.NginxPlusReceivers = append(
						cfg.Collector.Receivers.NginxPlusReceivers,
						nginxPlusReceiver,
					)
				}

				reloadCollector = true
				nginxReceiverFound = true
			}

			return nginxReceiverFound, reloadCollector
		}
	}

	return nginxReceiverFound, reloadCollector
}

func (oc *Collector) updateExistingNginxOSSReceiver(
	cfg *config.Config,
	nginxConfigContext *model.NginxConfigContext,
) (nginxReceiverFound, reloadCollector bool) {
	for index, nginxReceiver := range cfg.Collector.Receivers.NginxReceivers {
		if nginxReceiver.InstanceID == nginxConfigContext.InstanceID {
			nginxReceiverFound = true

			if isOSSReceiverChanged(nginxReceiver, nginxConfigContext) {
				cfg.Collector.Receivers.NginxReceivers = append(
					cfg.Collector.Receivers.NginxReceivers[:index],
					cfg.Collector.Receivers.NginxReceivers[index+1:]...,
				)
				if nginxConfigContext.StubStatus.URL != "" {
					slog.Debug("Updating existing NGINX OSS receiver", "url",
						nginxConfigContext.StubStatus.URL)
					nginxReceiver.StubStatus = config.APIDetails{
						URL:      nginxConfigContext.StubStatus.URL,
						Listen:   nginxConfigContext.StubStatus.Listen,
						Location: nginxConfigContext.StubStatus.Location,
					}
					nginxReceiver.AccessLogs = toConfigAccessLog(nginxConfigContext.AccessLogs)
					cfg.Collector.Receivers.NginxReceivers = append(
						cfg.Collector.Receivers.NginxReceivers,
						nginxReceiver,
					)
				}

				reloadCollector = true
				nginxReceiverFound = true
			}

			return nginxReceiverFound, reloadCollector
		}
	}

	return nginxReceiverFound, reloadCollector
}

//nolint:goconst // keeping map raw values to avoid overengineering
func (oc *Collector) updateNginxAppProtectTcplogReceivers(
	ctx context.Context,
	cfg *config.Config,
	nginxConfigContext *model.NginxConfigContext,
) bool {
	newTcplogReceiverAdded := false

	if cfg.Collector.Receivers.TcplogReceivers == nil {
		cfg.Collector.Receivers.TcplogReceivers = make(map[string]*config.TcplogReceiver)
	}

	napSysLogServer := oc.findAvailableSyslogServer(ctx, nginxConfigContext.NAPSysLogServer)

	if napSysLogServer != "" {
		if !oc.doesTcplogReceiverAlreadyExist(cfg, napSysLogServer) {
			cfg.Collector.Receivers.TcplogReceivers["nginx_app_protect"] = &config.TcplogReceiver{
				ListenAddress: napSysLogServer,
				Operators: []config.Operator{
					// regex captures the priority number from the log line
					{
						Type: "regex_parser",
						Fields: map[string]string{
							"regex":      "^<(?P<priority>\\d+)>",
							"parse_from": "body",
							"parse_to":   "attributes",
						},
					},
					// filter drops all logs that have a severity above 4
					// https://docs.secureauth.com/0902/en/how-to-read-a-syslog-message.html#severity-code-table
					{
						Type: "filter",
						Fields: map[string]string{
							"expr":       "'int(attributes.priority) % 8 > 4'",
							"drop_ratio": "1.0",
						},
					},
					{
						Type: "add",
						Fields: map[string]string{
							"field": "body",
							"value": timestampConversionExpression,
						},
					},
					{
						Type: "syslog_parser",
						Fields: map[string]string{
							"protocol": "rfc3164",
						},
					},
					{
						Type: "remove",
						Fields: map[string]string{
							"field": "attributes.message",
						},
					},
					{
						Type: "add",
						Fields: map[string]string{
							"field": "resource[\"instance.id\"]",
							"value": nginxConfigContext.InstanceID,
						},
					},
					{
						Type: "add",
						Fields: map[string]string{
							"field": "resource[\"instance.type\"]",
							"value": "nginx-app-protect",
						},
					},
				},
			}

			newTcplogReceiverAdded = true
		}
	}

	tcplogReceiverDeleted := oc.areNapReceiversDeleted(cfg, napSysLogServer)

	return newTcplogReceiverAdded || tcplogReceiverDeleted
}

func (oc *Collector) areNapReceiversDeleted(cfg *config.Config, napSysLogServer string) bool {
	listenAddressesToBeDeleted := oc.configDeletedNapReceivers(cfg, napSysLogServer)
	if len(listenAddressesToBeDeleted) != 0 {
		delete(cfg.Collector.Receivers.TcplogReceivers, "nginx_app_protect")
		return true
	}

	return false
}

func (oc *Collector) configDeletedNapReceivers(cfg *config.Config, napSysLogServer string) map[string]bool {
	elements := make(map[string]bool)

	for _, tcplogReceiver := range cfg.Collector.Receivers.TcplogReceivers {
		elements[tcplogReceiver.ListenAddress] = true
	}

	if napSysLogServer != "" {
		addressesToDelete := make(map[string]bool)
		if !elements[napSysLogServer] {
			addressesToDelete[napSysLogServer] = true
		}

		return addressesToDelete
	}

	return elements
}

func (oc *Collector) doesTcplogReceiverAlreadyExist(cfg *config.Config, listenAddress string) bool {
	for _, tcplogReceiver := range cfg.Collector.Receivers.TcplogReceivers {
		if listenAddress == tcplogReceiver.ListenAddress {
			return true
		}
	}

	return false
}

// updateCertificateReceivers adds, removes, or restarts the certificate receiver for an instance.
// It compares cert metadata from CertificateMeta to detect in-place cert renewals.
func (oc *Collector) updateCertificateReceivers(
	cfg *config.Config,
	nginxConfigContext *model.NginxConfigContext,
) bool {
	certMeta := certMetaFromFiles(nginxConfigContext.Files)

	for i, certReceiver := range cfg.Collector.Receivers.CertificateReceivers {
		if certReceiver.InstanceID == nginxConfigContext.InstanceID {
			if len(certMeta) == 0 {
				cfg.Collector.Receivers.CertificateReceivers = append(
					cfg.Collector.Receivers.CertificateReceivers[:i],
					cfg.Collector.Receivers.CertificateReceivers[i+1:]...,
				)

				return true
			}

			if maps.Equal(certReceiver.CertMeta, certMeta) {
				return false
			}

			cfg.Collector.Receivers.CertificateReceivers[i].CertMeta = certMeta

			return true
		}
	}

	if len(certMeta) == 0 {
		return false
	}

	cfg.Collector.Receivers.CertificateReceivers = append(
		cfg.Collector.Receivers.CertificateReceivers,
		config.CertificateReceiver{
			InstanceID:         nginxConfigContext.InstanceID,
			CollectionInterval: cfg.Collector.Receivers.CertificateMetrics.CollectionInterval,
			CertMeta:           certMeta,
		},
	)

	return true
}

// certMetaFromFiles extracts cert metadata from Files with CertificateMeta.
func certMetaFromFiles(files []*v1.File) map[string]config.CertMeta {
	result := make(map[string]config.CertMeta)

	for _, f := range files {
		if cm, ok := f.GetFileMeta().GetFileType().(*v1.FileMeta_CertificateMeta); ok {
			path := f.GetFileMeta().GetName()
			meta := cm.CertificateMeta
			result[path] = config.CertMeta{
				SerialNumber:       meta.GetSerialNumber(),
				CommonName:         meta.GetSubject().GetCommonName(),
				PublicKeyAlgorithm: meta.GetPublicKeyAlgorithm(),
				NotAfter:           meta.GetDates().GetNotAfter(),
			}
		}
	}

	return result
}

func (oc *Collector) updateResourceAttributes(
	cfg *config.Config,
	attributesToAdd []config.ResourceAttribute,
) (actionUpdated bool) {
	actionUpdated = false

	if cfg.Collector.Processors.Resource["default"].Attributes != nil {
	OUTER:
		for _, toAdd := range attributesToAdd {
			for _, action := range cfg.Collector.Processors.Resource["default"].Attributes {
				if action.Key == toAdd.Key {
					continue OUTER
				}
			}
			cfg.Collector.Processors.Resource["default"].Attributes = append(
				cfg.Collector.Processors.Resource["default"].Attributes,
				toAdd,
			)
			actionUpdated = true
		}
	}

	return actionUpdated
}

func (oc *Collector) findAvailableSyslogServer(ctx context.Context, napSysLogServer string) string {
	if oc.previousNAPSysLogServer != "" &&
		normaliseAddress(oc.previousNAPSysLogServer) == normaliseAddress(napSysLogServer) {
		return napSysLogServer
	}

	listenConfig := &net.ListenConfig{}
	ln, err := listenConfig.Listen(ctx, "tcp", napSysLogServer)
	if err != nil {
		slog.DebugContext(ctx, "NAP syslog server is not reachable", "address", napSysLogServer,
			"error", err)

		return ""
	}

	closeError := ln.Close()
	if closeError != nil {
		slog.DebugContext(ctx, "Failed to close syslog server", "address", napSysLogServer, "error", closeError)
	}

	oc.previousNAPSysLogServer = napSysLogServer

	return napSysLogServer
}

func isOSSReceiverChanged(nginxReceiver config.NginxReceiver, nginxConfigContext *model.NginxConfigContext) bool {
	return nginxReceiver.StubStatus.URL != nginxConfigContext.StubStatus.URL ||
		len(nginxReceiver.AccessLogs) != len(nginxConfigContext.AccessLogs)
}

func toConfigAccessLog(al []*model.AccessLog) []config.AccessLog {
	if al == nil {
		return nil
	}

	results := make([]config.AccessLog, 0, len(al))
	for _, ctxAccessLog := range al {
		results = append(results, config.AccessLog{
			LogFormat: escapeString(ctxAccessLog.Format),
			FilePath:  ctxAccessLog.Name,
		})
	}

	return results
}

func escapeString(input string) string {
	output := strings.ReplaceAll(input, "$", "$$")
	output = strings.ReplaceAll(output, "\"", "\\\"")

	return output
}

func (oc *Collector) setExporterProxyEnvVars(ctx context.Context, conf *config.Config) {
	proxy := conf.Command.Server.Proxy
	proxyURL := proxy.URL
	parsedProxyURL, err := url.Parse(proxyURL)
	if err != nil {
		slog.ErrorContext(ctx, "Malformed proxy URL, unable to configure proxy for OTLP exporter",
			"url", proxyURL, "error", err)

		return
	}

	if parsedProxyURL.Scheme == "https" {
		slog.ErrorContext(ctx, "HTTPS protocol not supported by OTLP exporter, unable to configure proxy for "+
			"OTLP exporter", "url", proxyURL)

		return
	}

	auth := ""
	if proxy.AuthMethod != "" && strings.TrimSpace(proxy.AuthMethod) != "" {
		auth = strings.TrimSpace(proxy.AuthMethod)
	}

	// Use the standalone setProxyWithBasicAuth function
	if auth == "" {
		setProxyEnvs(ctx, proxyURL, "Setting Proxy from command.Proxy (no auth)")
		return
	}
	authLower := strings.ToLower(auth)
	if authLower == "basic" {
		setProxyWithBasicAuth(ctx, proxy, parsedProxyURL)
	} else {
		slog.ErrorContext(ctx, "Unknown auth type for proxy, unable to configure proxy for OTLP exporter",
			"auth", auth, "url", proxyURL)
	}
}

// setProxyEnvs sets the HTTP_PROXY and HTTPS_PROXY environment variables and logs the action.
func setProxyEnvs(ctx context.Context, proxyEnvURL, msg string) {
	logURL := proxyEnvURL
	if u, err := url.Parse(proxyEnvURL); err == nil && u.User != nil {
		u.User = nil
		logURL = u.String()
	}

	slog.DebugContext(ctx, msg, "url", logURL)

	if setenvErr := os.Setenv("HTTPS_PROXY", proxyEnvURL); setenvErr != nil {
		slog.ErrorContext(ctx, "Failed to set OTLP exporter proxy environment variables", "error", setenvErr)
	}
}

// setProxyWithBasicAuth sets the proxy environment variables with basic auth credentials.
func setProxyWithBasicAuth(ctx context.Context, proxy *config.Proxy, parsedProxyURL *url.URL) {
	username := proxy.Username
	password := proxy.Password
	if username == "" || password == "" {
		slog.ErrorContext(ctx, "Unable to configure OTLP exporter proxy, username or password missing for basic auth")
		return
	}
	parsedProxyURL.User = url.UserPassword(username, password)
	proxyURL := parsedProxyURL.String()
	setProxyEnvs(ctx, proxyURL, "Setting Proxy with basic auth")
}

func normaliseAddress(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}

	if host == "localhost" { //nolint:goconst // value is local to this function
		host = "127.0.0.1"
	}

	return net.JoinHostPort(host, port)
}

// copyOf returns a deep, independent copy of v (or its typed zero value when v
// is nil or cannot be represented as T). It is a typed wrapper around
// deepCopyValue.
//
//nolint:ireturn // generic helper must return the parameterized type T
func copyOf[T any](v T) T {
	copied := deepCopyValue(reflect.ValueOf(v))
	if !copied.IsValid() {
		var zero T
		return zero
	}

	result, ok := copied.Interface().(T)
	if !ok {
		var zero T
		return zero
	}

	return result
}
