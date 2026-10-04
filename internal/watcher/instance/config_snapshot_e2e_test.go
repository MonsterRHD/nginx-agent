// Copyright (c) F5, Inc.
//
// This source code is licensed under the Apache License, Version 2.0 license found in the
// LICENSE file in the root directory of this source tree.

package instance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	mpi "github.com/nginx/agent/v3/api/grpc/mpi/v1"
	parser "github.com/nginx/agent/v3/internal/datasource/config"
	"github.com/nginx/agent/v3/internal/model"
	"github.com/nginx/agent/v3/test/protos"
	"github.com/nginx/agent/v3/test/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testStableSettlePeriod = 10 * time.Millisecond

func writeConfigTree(t *testing.T, dir, serverConf string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "conf.d"), 0o700))
	rootConf := "events {\n}\nhttp {\n\tinclude " + filepath.Join(dir, "conf.d", "*.conf") + ";\n}\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "nginx.conf"), []byte(rootConf), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "conf.d", "01-server.conf"), []byte(serverConf), 0o600))
}

func newSnapshotE2EService(
	t *testing.T, dir string, settlePeriod time.Duration, maxAttempts int,
) (*InstanceWatcherService, string) {
	t.Helper()

	agentConfig := types.AgentConfig()

	resolvedDir, err := filepath.EvalSymlinks(dir)
	require.NoError(t, err)

	agentConfig.AllowedDirectories = []string{dir, resolvedDir}

	service := &InstanceWatcherService{
		nginxConfigParser:      parser.NewNginxConfigParser(agentConfig),
		configSettlePeriod:     settlePeriod,
		configParseMaxAttempts: maxAttempts,
		instanceCache:          make(map[string]*mpi.Instance),
		nginxConfigCache:       make(map[string]*model.NginxConfigContext),
	}

	rootPath := filepath.Join(dir, "nginx.conf")

	return service, rootPath
}

func configFileNames(nginxConfigContext *model.NginxConfigContext) []string {
	names := make([]string, 0, len(nginxConfigContext.Files))
	for _, file := range nginxConfigContext.Files {
		names = append(names, file.GetFileMeta().GetName())
	}

	return names
}

func TestParseStableConfig_FullConfigTree(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeConfigTree(t, dir, "server { listen 80; }")

	service, rootPath := newSnapshotE2EService(t, dir, 0, 2)
	instance := protos.NginxOssInstance([]string{})
	instance.InstanceRuntime.ConfigPath = rootPath

	nginxConfigContext, err := service.parseStableConfig(ctx, instance)
	require.NoError(t, err)

	assert.ElementsMatch(t,
		[]string{rootPath, filepath.Join(dir, "conf.d", "01-server.conf")},
		configFileNames(nginxConfigContext),
	)
}

func TestParseStableConfig_HalfWrittenIncludeConverges(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeConfigTree(t, dir, "server { listen 80; }")

	service, rootPath := newSnapshotE2EService(t, dir, testStableSettlePeriod, 50)
	instance := protos.NginxOssInstance([]string{})
	instance.InstanceRuntime.ConfigPath = rootPath

	// A batch replacement in progress: the newly added include file is only half written.
	halfWritten := "server {\n\tlisten 8080;\n"
	newInclude := filepath.Join(dir, "conf.d", "02-new.conf")
	require.NoError(t, os.WriteFile(newInclude, []byte(halfWritten), 0o600))

	type parseResult struct {
		context *model.NginxConfigContext
		err     error
	}

	resultChannel := make(chan parseResult, 1)

	go func() {
		nginxConfigContext, err := service.parseStableConfig(ctx, instance)
		resultChannel <- parseResult{context: nginxConfigContext, err: err}
	}()

	// While the stable parser keeps retrying the unreadable candidate, finish writing the
	// complete include file.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, os.WriteFile(newInclude,
		[]byte("server {\n\tlisten 8080;\n}\n"), 0o600))

	select {
	case result := <-resultChannel:
		require.NoError(t, result.err)
		assert.Contains(t, configFileNames(result.context), newInclude,
			"the confirmed snapshot must contain the completed include, not the half-written one")
	case <-time.After(5 * time.Second):
		t.Fatal("stable config parse did not converge after the batch write completed")
	}
}

func TestParseStableConfig_IncludeCycleRejected(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	rootConf := "events {\n}\nhttp {\n\tinclude " + filepath.Join(dir, "conf.d", "a.conf") + ";\n}\n"
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "conf.d"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "nginx.conf"), []byte(rootConf), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "conf.d", "a.conf"),
		[]byte("include "+filepath.Join(dir, "conf.d", "b.conf")+";\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "conf.d", "b.conf"),
		[]byte("include "+filepath.Join(dir, "conf.d", "a.conf")+";\n"), 0o600))

	service, rootPath := newSnapshotE2EService(t, dir, 0, 2)
	instance := protos.NginxOssInstance([]string{})
	instance.InstanceRuntime.ConfigPath = rootPath

	nginxConfigContext, err := service.parseStableConfig(ctx, instance)
	require.Error(t, err, "include cycles can never produce a confirmed snapshot")
	assert.Nil(t, nginxConfigContext)
}

func TestParseStableConfig_SymlinkedTreeSwitch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	targetA := filepath.Join(dir, "config-a")
	targetB := filepath.Join(dir, "config-b")
	require.NoError(t, os.MkdirAll(targetA, 0o700))
	require.NoError(t, os.MkdirAll(targetB, 0o700))

	configA := "events {\n}\nhttp {\n\tserver {\n\t\tlisten 80;\n\t}\n}\n"
	configB := "events {\n}\nhttp {\n\tserver {\n\t\tlisten 81;\n\t}\n}\n"
	require.NoError(t, os.WriteFile(filepath.Join(targetA, "nginx.conf"), []byte(configA), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(targetB, "nginx.conf"), []byte(configB), 0o600))

	link := filepath.Join(dir, "nginx.conf")
	require.NoError(t, os.Symlink(filepath.Join(targetA, "nginx.conf"), link))

	service, rootPath := newSnapshotE2EService(t, dir, 0, 2)
	instance := protos.NginxOssInstance([]string{})
	instance.InstanceRuntime.ConfigPath = rootPath

	firstContext, err := service.parseStableConfig(ctx, instance)
	require.NoError(t, err)
	require.Len(t, firstContext.Files, 1)
	firstHash := firstContext.Files[0].GetFileMeta().GetHash()

	// Switch the whole config tree atomically through the symlink.
	require.NoError(t, os.Remove(link))
	require.NoError(t, os.Symlink(filepath.Join(targetB, "nginx.conf"), link))

	secondContext, err := service.parseStableConfig(ctx, instance)
	require.NoError(t, err)
	require.Len(t, secondContext.Files, 1)
	assert.NotEqual(t, firstHash, secondContext.Files[0].GetFileMeta().GetHash(),
		"the snapshot must follow the switched symlink target")
}

func TestReparseConfigs_KeepsLastConfirmedSnapshotOnFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeConfigTree(t, dir, "server { listen 80; }")

	instance := protos.NginxOssInstance([]string{})
	instance.InstanceRuntime.ConfigPath = filepath.Join(dir, "nginx.conf")

	instancesChannel := make(chan ResourceUpdatesMessage, 2)
	nginxConfigContextChannel := make(chan NginxConfigContextMessage, 2)

	service, _ := newSnapshotE2EService(t, dir, 0, 2)
	service.instancesChannel = instancesChannel
	service.nginxConfigContextChannel = nginxConfigContextChannel
	service.instanceCache[instance.GetInstanceMeta().GetInstanceId()] = instance

	// Initial confirmed snapshot.
	service.ReparseConfigs(ctx)

	var initialContext *model.NginxConfigContext
	select {
	case message := <-nginxConfigContextChannel:
		initialContext = message.NginxConfigContext
	default:
		t.Fatal("expected the initial confirmed config context to be sent")
	}

	// The config becomes temporarily unreadable (half-written root).
	require.NoError(t, os.WriteFile(filepath.Join(dir, "nginx.conf"),
		[]byte("events {\nhttp {\n"), 0o600))

	service.ReparseConfigs(ctx)

	select {
	case <-nginxConfigContextChannel:
		t.Fatal("no config context update must be sent while no stable valid snapshot exists")
	default:
	}

	assert.Equal(t, initialContext,
		service.nginxConfigCache[instance.GetInstanceMeta().GetInstanceId()],
		"the last confirmed snapshot must be retained while parsing fails")

	// Recovery: the config is valid again, reparsing sends exactly one update.
	writeConfigTree(t, dir, "server { listen 81; }")
	service.ReparseConfigs(ctx)

	select {
	case message := <-nginxConfigContextChannel:
		assert.NotEqual(t, initialContext, message.NginxConfigContext)
	case <-time.After(2 * time.Second):
		t.Fatal("expected a config context update after the config recovered")
	}
}
