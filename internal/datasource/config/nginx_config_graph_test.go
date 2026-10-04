// Copyright (c) F5, Inc.
//
// This source code is licensed under the Apache License, Version 2.0 license found in the
// LICENSE file in the root directory of this source tree.

package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/nginx/agent/v3/test/protos"
	"github.com/nginx/agent/v3/test/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNginxConfigParser_ParseWithGraph(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "conf.d"), 0o700))

	rootPath := filepath.Join(dir, "nginx.conf")
	relativeGlobInclude := filepath.Join(dir, "conf.d", "server.conf")
	absoluteInclude := filepath.Join(dir, "extra.conf")

	// Relative glob include resolved against the root config directory, plus an absolute include.
	rootConf := "events {\n}\nhttp {\n" +
		"\tinclude conf.d/*.conf;\n" +
		"\tinclude " + absoluteInclude + ";\n" +
		"}\n"
	require.NoError(t, os.WriteFile(rootPath, []byte(rootConf), 0o600))
	require.NoError(t, os.WriteFile(relativeGlobInclude, []byte("server { listen 80; }"), 0o600))
	require.NoError(t, os.WriteFile(absoluteInclude, []byte("server { listen 81; }"), 0o600))

	instance := protos.NginxOssInstance([]string{})
	instance.InstanceRuntime.ConfigPath = rootPath

	agentConfig := types.AgentConfig()
	agentConfig.AllowedDirectories = []string{dir}

	ncp := NewNginxConfigParser(agentConfig)

	result, err := ncp.ParseWithGraph(ctx, instance)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.ConfigContext)
	require.NotNil(t, result.Graph)

	assert.Equal(t, rootPath, result.Graph.RootPath)
	assert.ElementsMatch(t,
		[]string{rootPath, relativeGlobInclude, absoluteInclude},
		result.Graph.Files,
		"the graph must contain every file read by the parser: root, glob match and absolute include",
	)
	assert.ElementsMatch(t,
		[]string{filepath.Join(dir, "conf.d", "*.conf"), absoluteInclude},
		result.Graph.IncludePatterns,
		"relative include patterns are resolved against the root config directory",
	)

	// Parse keeps returning only the config context, preserving the original interface behavior.
	configContext, err := ncp.Parse(ctx, instance)
	require.NoError(t, err)
	assert.Equal(t, result.ConfigContext.ConfigPath, configContext.ConfigPath)
	assert.Len(t, configContext.Files, len(result.ConfigContext.Files))
}

func TestNginxConfigParser_ParseWithGraph_IncludeCycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	rootPath := filepath.Join(dir, "nginx.conf")
	rootConf := "events {\n}\nhttp {\n\tinclude " + filepath.Join(dir, "a.conf") + ";\n}\n"
	require.NoError(t, os.WriteFile(rootPath, []byte(rootConf), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.conf"),
		[]byte("include "+filepath.Join(dir, "b.conf")+";\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.conf"),
		[]byte("include "+filepath.Join(dir, "a.conf")+";\n"), 0o600))

	instance := protos.NginxOssInstance([]string{})
	instance.InstanceRuntime.ConfigPath = rootPath

	agentConfig := types.AgentConfig()
	agentConfig.AllowedDirectories = []string{dir}

	ncp := NewNginxConfigParser(agentConfig)

	result, err := ncp.ParseWithGraph(ctx, instance)
	require.Error(t, err)
	assert.Nil(t, result)
}
