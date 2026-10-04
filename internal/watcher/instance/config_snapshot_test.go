// Copyright (c) F5, Inc.
//
// This source code is licensed under the Apache License, Version 2.0 license found in the
// LICENSE file in the root directory of this source tree.

package instance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	mpi "github.com/nginx/agent/v3/api/grpc/mpi/v1"
	"github.com/nginx/agent/v3/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasGlobMagic(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"/etc/nginx/nginx.conf", false},
		{"/etc/nginx/conf.d/*.conf", true},
		{"/etc/nginx/sites/*.conf", true},
		{"/etc/nginx/conf.d/?.conf", true},
		{"/etc/nginx/conf.d/[a-z].conf", true},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			assert.Equal(t, test.expected, hasGlobMagic(test.path))
		})
	}
}

func TestGlobStaticPrefix(t *testing.T) {
	tests := []struct {
		pattern  string
		expected string
	}{
		{"/etc/nginx/conf.d/*.conf", "/etc/nginx/conf.d"},
		{"/etc/nginx/sites/*/*.conf", "/etc/nginx/sites"},
		{"/etc/nginx/conf.d", "/etc/nginx"},
		{"/srv/nginx/vhosts/*/conf/*.conf", "/srv/nginx/vhosts"},
	}

	for _, test := range tests {
		t.Run(test.pattern, func(t *testing.T) {
			assert.Equal(t, test.expected, globStaticPrefix(test.pattern))
		})
	}
}

func TestResolveConfigGraph(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "conf.d"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sites", "site-a"), 0o700))

	root := filepath.Join(dir, "nginx.conf")
	plainInclude := filepath.Join(dir, "extra.conf")
	flatGlob := filepath.Join(dir, "conf.d", "*.conf")
	nestedGlob := filepath.Join(dir, "sites", "*", "*.conf")

	flatMatch := filepath.Join(dir, "conf.d", "server.conf")
	nestedMatch := filepath.Join(dir, "sites", "site-a", "app.conf")
	require.NoError(t, os.WriteFile(plainInclude, []byte("server { listen 81; }"), 0o600))
	require.NoError(t, os.WriteFile(flatMatch, []byte("server { listen 80; }"), 0o600))
	require.NoError(t, os.WriteFile(nestedMatch, []byte("server { listen 82; }"), 0o600))

	graph := resolveConfigGraph(root,
		[]string{root, plainInclude, flatMatch},
		[]string{plainInclude, flatGlob, nestedGlob},
	)

	assert.Equal(t, root, graph.rootPath)
	assert.ElementsMatch(t,
		[]string{root, plainInclude, flatMatch, nestedMatch},
		graph.files,
		"glob matches must be added to the graph files")

	assert.ElementsMatch(t,
		[]string{plainInclude, flatGlob, nestedGlob},
		graph.includePatterns,
	)

	assert.ElementsMatch(t,
		[]string{
			dir,
			filepath.Join(dir, "conf.d"),
			filepath.Join(dir, "sites"),
			filepath.Join(dir, "sites", "site-a"),
		},
		graph.globDirectories,
		"static glob prefixes and matched file directories must be tracked, including nested ones",
	)
}

func TestResolveConfigGraph_GlobWithoutMatches(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "not-created", "*.conf")

	graph := resolveConfigGraph(filepath.Join(dir, "nginx.conf"), nil, []string{pattern})

	assert.Contains(t, graph.globDirectories, filepath.Join(dir, "not-created"),
		"the static prefix of a glob without matches must still be tracked so later matches are noticed")
	assert.Equal(t, []string{filepath.Join(dir, "nginx.conf")}, graph.files,
		"glob patterns without matches do not add files")
}

func TestResolveGraphFromContext(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "nginx.conf")
	include := filepath.Join(dir, "conf.d", "*.conf")

	nginxConfigContext := &model.NginxConfigContext{
		ConfigPath: root,
		Files: []*mpi.File{
			{FileMeta: &mpi.FileMeta{Name: root}},
		},
		Includes: []string{include},
	}

	graph := resolveGraphFromContext(nginxConfigContext)
	assert.Equal(t, root, graph.rootPath)
	assert.Contains(t, graph.files, root)
	assert.Contains(t, graph.includePatterns, include)
	assert.Contains(t, graph.globDirectories, filepath.Join(dir, "conf.d"))
}

func TestFingerprintFile_UnchangedIsEqual(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nginx.conf")
	require.NoError(t, os.WriteFile(path, []byte("events {}"), 0o600))

	first := fingerprintFile(path)
	second := fingerprintFile(path)

	assert.True(t, first.exists)
	assert.True(t, first.accessible)
	assert.NotEmpty(t, first.hash)
	assert.Equal(t, first, second)
}

func TestFingerprintFile_ContentChangeDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nginx.conf")
	require.NoError(t, os.WriteFile(path, []byte("abc"), 0o600))

	before := fingerprintFile(path)

	// Same length replacement must still be detected via the content hash.
	require.NoError(t, os.WriteFile(path, []byte("abd"), 0o600))

	after := fingerprintFile(path)

	assert.NotEqual(t, before.hash, after.hash)
	assert.False(t, before == after)
}

func TestFingerprintFile_MissingAndCreatedDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nginx.conf")

	missing := fingerprintFile(path)
	assert.False(t, missing.exists)
	assert.NotEmpty(t, missing.problem)

	require.NoError(t, os.WriteFile(path, []byte("events {}"), 0o600))

	created := fingerprintFile(path)
	assert.True(t, created.exists)
	assert.True(t, created.accessible)
	assert.False(t, missing == created)
}

func TestFingerprintFile_SymlinkTargetSwapDetected(t *testing.T) {
	dir := t.TempDir()

	targetDirA := filepath.Join(dir, "config-a")
	targetDirB := filepath.Join(dir, "config-b")
	require.NoError(t, os.MkdirAll(targetDirA, 0o700))
	require.NoError(t, os.MkdirAll(targetDirB, 0o700))

	configA := filepath.Join(targetDirA, "nginx.conf")
	configB := filepath.Join(targetDirB, "nginx.conf")
	require.NoError(t, os.WriteFile(configA, []byte("server { listen 80; }"), 0o600))
	require.NoError(t, os.WriteFile(configB, []byte("server { listen 81; }"), 0o600))

	link := filepath.Join(dir, "nginx.conf")
	require.NoError(t, os.Symlink(configA, link))

	before := fingerprintFile(link)
	require.NoError(t, os.Remove(link))
	require.NoError(t, os.Symlink(configB, link))
	after := fingerprintFile(link)

	assert.NotEmpty(t, before.linkTarget)
	assert.NotEqual(t, before.linkTarget, after.linkTarget)
	assert.NotEqual(t, before.resolvedPath, after.resolvedPath)
	assert.NotEqual(t, before.hash, after.hash)
	assert.False(t, before == after)
}

func TestFingerprintDirectory_EntryChangesDetected(t *testing.T) {
	dir := t.TempDir()

	empty := fingerprintDirectory(dir)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.conf"), []byte("a"), 0o600))
	withA := fingerprintDirectory(dir)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.conf"), []byte("b"), 0o600))
	withAAndB := fingerprintDirectory(dir)
	require.NoError(t, os.Remove(filepath.Join(dir, "b.conf")))
	backToA := fingerprintDirectory(dir)
	require.NoError(t, os.Remove(filepath.Join(dir, "a.conf")))
	emptyAgain := fingerprintDirectory(dir)

	assert.True(t, empty.equal(emptyAgain), "removing every entry restores the directory fingerprint")
	assert.True(t, withA.equal(backToA), "adding then removing one file restores the previous fingerprint")
	assert.False(t, empty.equal(withA), "adding an entry is detected")
	assert.False(t, withA.equal(withAAndB), "adding another entry is detected")
}

func TestGraphFingerprint_Validity(t *testing.T) {
	dir := t.TempDir()

	availableFile := filepath.Join(dir, "nginx.conf")
	require.NoError(t, os.WriteFile(availableFile, []byte("events {}"), 0o600))
	missingFile := filepath.Join(dir, "missing.conf")
	availableDir := filepath.Join(dir, "conf.d")
	require.NoError(t, os.MkdirAll(availableDir, 0o700))
	missingDir := filepath.Join(dir, "not-created")

	graph := resolveConfigGraph(availableFile,
		[]string{availableFile, missingFile},
		[]string{
			filepath.Join(availableDir, "*.conf"),
			filepath.Join(missingDir, "*.conf"),
		},
	)

	fp := fingerprintConfigGraph(graph)

	assert.False(t, fp.valid(), "a missing referenced file makes the candidate invalid")

	// A missing glob directory is valid because an empty glob include is legal NGINX.
	graphOnly := resolveConfigGraph(availableFile, []string{availableFile},
		[]string{filepath.Join(missingDir, "*.conf")})
	assert.True(t, fingerprintConfigGraph(graphOnly).valid())
}

func TestGraphFingerprint_UnreadableDirectoryIsInvalid(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}

	dir := t.TempDir()
	restrictedDir := filepath.Join(dir, "restricted")
	require.NoError(t, os.MkdirAll(restrictedDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(restrictedDir, "a.conf"), []byte("a"), 0o600))
	require.NoError(t, os.Chmod(restrictedDir, 0o000))
	t.Cleanup(func() { _ = os.Chmod(restrictedDir, 0o700) })

	graph := resolveConfigGraph(filepath.Join(dir, "nginx.conf"), nil,
		[]string{filepath.Join(restrictedDir, "*.conf")})

	fp := fingerprintConfigGraph(graph)
	assert.False(t, fp.valid(), "a permission denied glob directory makes the candidate invalid")
}

func newTestSnapshotService(t *testing.T, maxAttempts int) *InstanceWatcherService {
	t.Helper()

	return &InstanceWatcherService{
		configSettlePeriod:     0,
		configParseMaxAttempts: maxAttempts,
	}
}

func TestBuildStableConfig_AcceptsStableCandidate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	root := filepath.Join(dir, "nginx.conf")
	require.NoError(t, os.WriteFile(root, []byte("events {}"), 0o600))

	graph := resolveConfigGraph(root, []string{root}, nil)
	expectedContext := &model.NginxConfigContext{ConfigPath: root, InstanceID: "instance-1"}

	service := newTestSnapshotService(t, 3)
	calls := 0
	candidate := func(context.Context) (*model.NginxConfigContext, *resolvedConfigGraph, error) {
		calls++

		return expectedContext, graph, nil
	}

	contextResult, err := service.buildStableConfig(ctx, candidate)
	require.NoError(t, err)
	assert.Equal(t, expectedContext, contextResult)
	assert.Equal(t, 2, calls, "the candidate is parsed once and then re-read on the stable graph")
}

func TestBuildStableConfig_GraphChangesDuringSettle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	root := filepath.Join(dir, "nginx.conf")
	require.NoError(t, os.WriteFile(root, []byte("events {}"), 0o600))

	graph := resolveConfigGraph(root, []string{root}, nil)
	expectedContext := &model.NginxConfigContext{ConfigPath: root}

	service := newTestSnapshotService(t, 3)

	// Simulate the file changing between the post-parse and post-settle fingerprint of the
	// first attempt; the following samples are stable again.
	fingerprintCalls := 0
	service.fingerprintGraphFunc = func(graph *resolvedConfigGraph) graphFingerprint {
		fingerprintCalls++
		fp := fingerprintConfigGraph(graph)

		if fingerprintCalls == 2 {
			for path, state := range fp.files {
				state.hash = "changed-during-settle"
				fp.files[path] = state
			}
		}

		return fp
	}

	calls := 0
	candidate := func(context.Context) (*model.NginxConfigContext, *resolvedConfigGraph, error) {
		calls++

		return expectedContext, graph, nil
	}

	contextResult, err := service.buildStableConfig(ctx, candidate)
	require.NoError(t, err)
	assert.Equal(t, expectedContext, contextResult)
	assert.Equal(t, 3, calls, "the first candidate is discarded and the config is re-read after convergence")
}

func TestBuildStableConfig_ConfirmedGraphChanges(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	root := filepath.Join(dir, "nginx.conf")
	extra := filepath.Join(dir, "extra.conf")
	require.NoError(t, os.WriteFile(root, []byte("events {}"), 0o600))
	require.NoError(t, os.WriteFile(extra, []byte("server { listen 81; }"), 0o600))

	graphWithoutExtra := resolveConfigGraph(root, []string{root}, nil)
	graphWithExtra := resolveConfigGraph(root, []string{root, extra}, []string{extra})

	service := newTestSnapshotService(t, 3)
	calls := 0
	candidate := func(context.Context) (*model.NginxConfigContext, *resolvedConfigGraph, error) {
		calls++
		// The first read sees the old graph; the new include is present for every later read.
		if calls == 1 {
			return &model.NginxConfigContext{}, graphWithoutExtra, nil
		}

		return &model.NginxConfigContext{ConfigPath: root}, graphWithExtra, nil
	}

	contextResult, err := service.buildStableConfig(ctx, candidate)
	require.NoError(t, err)
	assert.Equal(t, root, contextResult.ConfigPath)
	assert.Equal(t, 4, calls,
		"the candidate whose graph grew during the confirmed read is discarded and re-parsed")
}

func TestBuildStableConfig_MissingReferenceAppears(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	root := filepath.Join(dir, "nginx.conf")
	require.NoError(t, os.WriteFile(root, []byte("events {}"), 0o600))

	service := newTestSnapshotService(t, 3)
	calls := 0
	includeCreated := false
	candidate := func(context.Context) (*model.NginxConfigContext, *resolvedConfigGraph, error) {
		calls++

		// The first candidate references a file that has not been written yet.
		if calls == 1 {
			return nil, resolveConfigGraph(root, []string{root, filepath.Join(dir, "include.conf")}, nil), nil
		}

		if !includeCreated {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "include.conf"),
				[]byte("server { listen 80; }"), 0o600))
			includeCreated = true
		}

		graph := resolveConfigGraph(root, []string{root, filepath.Join(dir, "include.conf")}, nil)

		return &model.NginxConfigContext{ConfigPath: root}, graph, nil
	}

	contextResult, err := service.buildStableConfig(ctx, candidate)
	require.NoError(t, err)
	assert.Equal(t, root, contextResult.ConfigPath)
}

func TestBuildStableConfig_ParseErrorRetriedThenAccepted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	root := filepath.Join(dir, "nginx.conf")
	require.NoError(t, os.WriteFile(root, []byte("events {}"), 0o600))
	graph := resolveConfigGraph(root, []string{root}, nil)

	service := newTestSnapshotService(t, 4)
	calls := 0
	candidate := func(context.Context) (*model.NginxConfigContext, *resolvedConfigGraph, error) {
		calls++
		if calls == 1 {
			return nil, nil, errors.New("premature end of file")
		}

		return &model.NginxConfigContext{ConfigPath: root}, graph, nil
	}

	contextResult, err := service.buildStableConfig(ctx, candidate)
	require.NoError(t, err)
	assert.Equal(t, root, contextResult.ConfigPath)
}

func TestBuildStableConfig_PersistentParseErrorFails(t *testing.T) {
	ctx := context.Background()

	service := newTestSnapshotService(t, 3)
	calls := 0
	candidate := func(context.Context) (*model.NginxConfigContext, *resolvedConfigGraph, error) {
		calls++

		return nil, nil, errors.New("configs contain include cycle")
	}

	contextResult, err := service.buildStableConfig(ctx, candidate)
	require.Error(t, err)
	assert.Nil(t, contextResult)
	assert.Equal(t, 3, calls, "the last confirmed snapshot is kept after every candidate read fails")
}

func TestBuildStableConfig_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	service := newTestSnapshotService(t, 3)
	candidate := func(context.Context) (*model.NginxConfigContext, *resolvedConfigGraph, error) {
		return &model.NginxConfigContext{}, resolveConfigGraph("", nil, nil), nil
	}

	_, err := service.buildStableConfig(ctx, candidate)
	require.ErrorIs(t, err, context.Canceled)
}
