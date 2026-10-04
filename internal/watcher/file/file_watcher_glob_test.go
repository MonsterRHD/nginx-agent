// Copyright (c) F5, Inc.
//
// This source code is licensed under the Apache License, Version 2.0 license found in the
// LICENSE file in the root directory of this source tree.

package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/fsnotify/fsnotify"
	"github.com/nginx/agent/v3/internal/model"
	"github.com/nginx/agent/v3/test/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncludeDirectories(t *testing.T) {
	dir := t.TempDir()

	t.Run("plain include", func(t *testing.T) {
		assert.Equal(t, []string{filepath.Join(dir, "nginx")},
			includeDirectories(filepath.Join(dir, "nginx", "extra.conf")))
	})

	t.Run("glob without matches", func(t *testing.T) {
		pattern := filepath.Join(dir, "not-created", "*.conf")
		assert.Equal(t, []string{filepath.Join(dir, "not-created")}, includeDirectories(pattern))
	})

	t.Run("nested glob matches", func(t *testing.T) {
		siteDir := filepath.Join(dir, "sites", "site-a")
		require.NoError(t, os.MkdirAll(siteDir, directoryPermissions))
		require.NoError(t, os.WriteFile(filepath.Join(siteDir, "app.conf"), []byte("server {}"), 0o600))

		pattern := filepath.Join(dir, "sites", "*", "*.conf")
		assert.Equal(t,
			[]string{filepath.Join(dir, "sites"), siteDir},
			includeDirectories(pattern),
			"both the static prefix and the matched directory are watched")
	})
}

func TestFileWatcherService_Update_NestedGlobDirectories(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	siteDir := filepath.Join(dir, "sites", "site-a")
	require.NoError(t, os.MkdirAll(siteDir, directoryPermissions))
	require.NoError(t, os.WriteFile(filepath.Join(siteDir, "app.conf"), []byte("server {}"), 0o600))

	agentConfig := types.AgentConfig()
	agentConfig.AllowedDirectories = []string{dir}

	fileWatcherService := NewFileWatcherService(agentConfig)

	watcher, err := fsnotify.NewWatcher()
	require.NoError(t, err)
	fileWatcherService.watcher = watcher

	pattern := filepath.Join(dir, "sites", "*", "*.conf")

	// A newly created glob matched subdirectory is added to the watch list.
	fileWatcherService.Update(ctx, &model.NginxConfigContext{
		Includes: []string{pattern},
	})

	assert.Contains(t, fileWatcherService.directoriesToWatch, filepath.Join(dir, "sites"))
	assert.Contains(t, fileWatcherService.directoriesToWatch, siteDir)
	assert.ElementsMatch(t,
		[]string{filepath.Join(dir, "sites"), siteDir},
		fileWatcherService.watcher.WatchList(),
	)

	// Directories removed from the glob match set are dropped from the watch list.
	fileWatcherService.Update(ctx, &model.NginxConfigContext{
		Includes: []string{},
	})

	assert.Empty(t, fileWatcherService.directoriesToWatch)
	assert.Empty(t, fileWatcherService.watcher.WatchList())
}
