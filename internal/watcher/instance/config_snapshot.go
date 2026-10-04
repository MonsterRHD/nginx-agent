// Copyright (c) F5, Inc.
//
// This source code is licensed under the Apache License, Version 2.0 license found in the
// LICENSE file in the root directory of this source tree.

package instance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	mpi "github.com/nginx/agent/v3/api/grpc/mpi/v1"
	parser "github.com/nginx/agent/v3/internal/datasource/config"
	"github.com/nginx/agent/v3/internal/model"
	"github.com/nginx/agent/v3/pkg/files"
)

const (
	// defaultConfigSettlePeriod is how long a candidate parse waits for batched file writes
	// or symlink switches to settle before re-checking the include graph.
	defaultConfigSettlePeriod = 500 * time.Millisecond
	// defaultConfigParseMaxAttempts bounds how many candidate reads are performed while the
	// include graph keeps changing or the config cannot yet be parsed.
	defaultConfigParseMaxAttempts = 10
)

var (
	// globMagicPattern matches glob meta characters used by NGINX include directives.
	globMagicPattern = regexp.MustCompile(`[*?[]`)

	// errIncludeGraphChanged marks a candidate snapshot that has to be discarded because a file,
	// a matched directory or a symlink target changed while the config was being read.
	errIncludeGraphChanged = errors.New("include graph changed while parsing the NGINX config")
)

// resolvedConfigGraph is the concrete set of filesystem locations a candidate parse depended on.
type resolvedConfigGraph struct {
	rootPath        string
	files           []string
	includePatterns []string
	// globDirectories contains the static prefix directories of glob include patterns together
	// with the parent directories of every current match. Watching these directories detects
	// newly added or removed glob matches, including files in newly created subdirectories.
	globDirectories []string
}

// fileFingerprint identifies one file at two points in time. In addition to the file metadata and
// the resolved symlink chain, regular files are identified by the sha256 of their contents, so a
// half-written or replaced file can never look stable.
type fileFingerprint struct {
	modTime      time.Time
	hash         string
	linkTarget   string
	problem      string
	resolvedPath string
	size         int64
	linkMode     os.FileMode
	targetMode   os.FileMode
	accessible   bool
	exists       bool
}

type dirEntryFingerprint struct {
	modTime    time.Time
	linkTarget string
	name       string
	size       int64
	entryMode  os.FileMode
	targetMode os.FileMode
}

// directoryFingerprint identifies a glob matched directory, including every directory entry so
// that creating, removing or renaming a matching file invalidates an in-flight candidate.
type directoryFingerprint struct {
	linkTarget   string
	resolvedPath string
	problem      string
	entries      []dirEntryFingerprint
	linkMode     os.FileMode
	accessible   bool
	exists       bool
}

type graphFingerprint struct {
	files map[string]fileFingerprint
	dirs  map[string]directoryFingerprint
}

// resolveConfigGraph expands the files and include patterns of a parsed config into the concrete
// files and glob matched directories the parse depended on. All paths are cleaned and de-duplicated.
func resolveConfigGraph(rootPath string, parsedFiles, includePatterns []string) *resolvedConfigGraph {
	graph := &resolvedConfigGraph{rootPath: filepath.Clean(rootPath)}

	seenFiles := make(map[string]struct{})
	seenPatterns := make(map[string]struct{})
	directories := make(map[string]struct{})

	addFile := func(path string) {
		graph.addFile(path, seenFiles)
	}
	addDirectory := func(path string) {
		if path != "" {
			directories[filepath.Clean(path)] = struct{}{}
		}
	}

	addFile(rootPath)
	for _, file := range parsedFiles {
		addFile(file)
	}

	for _, pattern := range includePatterns {
		graph.addIncludePattern(pattern, seenPatterns, addFile, addDirectory)
	}

	for directory := range directories {
		graph.globDirectories = append(graph.globDirectories, directory)
	}

	sort.Strings(graph.files)
	sort.Strings(graph.includePatterns)
	sort.Strings(graph.globDirectories)

	return graph
}

func (graph *resolvedConfigGraph) addFile(path string, seenFiles map[string]struct{}) {
	if path == "" {
		return
	}

	path = filepath.Clean(path)
	if _, ok := seenFiles[path]; ok {
		return
	}

	seenFiles[path] = struct{}{}
	graph.files = append(graph.files, path)
}

// addIncludePattern records an include pattern and the files and directories the pattern matches.
func (graph *resolvedConfigGraph) addIncludePattern(
	pattern string,
	seenPatterns map[string]struct{},
	addFile func(string),
	addDirectory func(string),
) {
	pattern = filepath.Clean(pattern)
	if pattern == "" || pattern == "." {
		return
	}

	if _, ok := seenPatterns[pattern]; ok {
		return
	}

	seenPatterns[pattern] = struct{}{}
	graph.includePatterns = append(graph.includePatterns, pattern)

	if !hasGlobMagic(pattern) {
		addFile(pattern)
		addDirectory(filepath.Dir(pattern))

		return
	}

	// The static prefix must be watched to detect newly created matching subdirectories.
	addDirectory(globStaticPrefix(pattern))

	matches, err := filepath.Glob(pattern)
	if err != nil {
		slog.Debug("Unable to expand include glob while resolving config graph",
			"pattern", pattern, "error", err)

		return
	}

	for _, match := range matches {
		addFile(match)
		addDirectory(filepath.Dir(match))
	}
}

// resolveGraphFromContext derives the include graph from a parsed config context. It is used when
// the parser does not expose the raw include graph, for example in tests with a fake parser.
func resolveGraphFromContext(nginxConfigContext *model.NginxConfigContext) *resolvedConfigGraph {
	if nginxConfigContext == nil {
		return resolveConfigGraph("", nil, nil)
	}

	return resolveConfigGraph(nginxConfigContext.ConfigPath,
		configContextFileNames(nginxConfigContext), nginxConfigContext.Includes)
}

// hasGlobMagic reports whether path contains NGINX include glob meta characters.
func hasGlobMagic(path string) bool {
	return globMagicPattern.MatchString(path)
}

// globStaticPrefix returns the leading part of a glob pattern that does not contain any glob meta
// characters. It is the highest existing directory that has to be watched to notice matches
// appearing or disappearing.
func globStaticPrefix(pattern string) string {
	cleaned := filepath.Clean(pattern)
	if !hasGlobMagic(cleaned) {
		return filepath.Dir(cleaned)
	}

	leadingSlash := strings.HasPrefix(cleaned, string(os.PathSeparator))
	parts := strings.Split(cleaned, string(os.PathSeparator))

	prefixParts := make([]string, 0, len(parts))

	for _, part := range parts {
		if part == "" {
			continue
		}

		if hasGlobMagic(part) {
			break
		}

		prefixParts = append(prefixParts, part)
	}

	prefix := strings.Join(prefixParts, string(os.PathSeparator))
	if leadingSlash {
		prefix = string(os.PathSeparator) + prefix
	}

	if prefix == "" {
		return filepath.Dir(cleaned)
	}

	return filepath.Clean(prefix)
}

// fingerprintConfigGraph takes a point-in-time fingerprint of every file, matched directory and
// symlink target reached by the include graph.
func fingerprintConfigGraph(graph *resolvedConfigGraph) graphFingerprint {
	fingerprint := graphFingerprint{
		files: make(map[string]fileFingerprint, len(graph.files)),
		dirs:  make(map[string]directoryFingerprint, len(graph.globDirectories)),
	}

	for _, file := range graph.files {
		fingerprint.files[file] = fingerprintFile(file)
	}

	for _, directory := range graph.globDirectories {
		fingerprint.dirs[directory] = fingerprintDirectory(directory)
	}

	return fingerprint
}

func fingerprintFile(path string) fileFingerprint {
	state := fileFingerprint{}

	linkInfo, err := os.Lstat(path)
	if err != nil {
		state.problem = err.Error()

		return state
	}

	state.exists = true
	state.linkMode = linkInfo.Mode()

	if linkInfo.Mode()&os.ModeSymlink != 0 {
		if target, readlinkErr := os.Readlink(path); readlinkErr == nil {
			state.linkTarget = target
		}
	}

	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		state.problem = err.Error()

		return state
	}

	state.resolvedPath = resolvedPath

	info, err := os.Stat(path)
	if err != nil {
		state.problem = err.Error()

		return state
	}

	state.targetMode = info.Mode()
	state.modTime = info.ModTime()
	state.size = info.Size()

	if info.Mode().IsRegular() {
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			state.problem = readErr.Error()

			return state
		}

		state.hash = files.GenerateHash(content)
	}

	state.accessible = true

	return state
}

func fingerprintDirectory(path string) directoryFingerprint {
	state := directoryFingerprint{}

	linkInfo, err := os.Lstat(path)
	if err != nil {
		// A directory that does not exist yet is tracked as a stable "missing" state rather than
		// an error: an empty glob include is valid NGINX configuration.
		state.problem = err.Error()

		return state
	}

	state.exists = true
	state.linkMode = linkInfo.Mode()

	if linkInfo.Mode()&os.ModeSymlink != 0 {
		if target, readlinkErr := os.Readlink(path); readlinkErr == nil {
			state.linkTarget = target
		}
	}

	if resolvedPath, evalErr := filepath.EvalSymlinks(path); evalErr == nil {
		state.resolvedPath = resolvedPath
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		state.problem = err.Error()

		return state
	}

	state.entries = directoryEntryFingerprints(path, entries)
	state.accessible = true

	return state
}

func directoryEntryFingerprints(directory string, entries []os.DirEntry) []dirEntryFingerprint {
	entryFingerprints := make([]dirEntryFingerprint, 0, len(entries))

	for _, entry := range entries {
		entryState := dirEntryFingerprint{
			name:      entry.Name(),
			entryMode: entry.Type(),
		}

		if info, infoErr := entry.Info(); infoErr == nil {
			entryState.targetMode = info.Mode()
			entryState.modTime = info.ModTime()
			entryState.size = info.Size()
		}

		if entry.Type()&os.ModeSymlink != 0 {
			targetPath := filepath.Join(directory, entry.Name())
			if target, readlinkErr := os.Readlink(targetPath); readlinkErr == nil {
				entryState.linkTarget = target
			}
		}

		entryFingerprints = append(entryFingerprints, entryState)
	}

	sort.Slice(entryFingerprints, func(i, j int) bool {
		return entryFingerprints[i].name < entryFingerprints[j].name
	})

	return entryFingerprints
}

// valid reports whether every reference in the graph could be read. A glob directory that has not
// been created yet is allowed because an empty glob is a valid include.
func (fp graphFingerprint) valid() bool {
	for _, state := range fp.files {
		if state.problem != "" {
			return false
		}
	}

	for _, state := range fp.dirs {
		if !state.exists {
			continue
		}

		if state.problem != "" {
			return false
		}
	}

	return true
}

// equal reports whether two fingerprints describe exactly the same files, directory entries and
// symlink targets. Different file sets or directory sets are considered different graphs.
func (fp graphFingerprint) equal(other graphFingerprint) bool {
	if len(fp.files) != len(other.files) || len(fp.dirs) != len(other.dirs) {
		return false
	}

	for path, state := range fp.files {
		otherState, ok := other.files[path]
		if !ok || state != otherState {
			return false
		}
	}

	for path, state := range fp.dirs {
		otherState, ok := other.dirs[path]
		if !ok || !state.equal(otherState) {
			return false
		}
	}

	return true
}

func (fp directoryFingerprint) equal(other directoryFingerprint) bool {
	return fp.exists == other.exists &&
		fp.accessible == other.accessible &&
		fp.linkMode == other.linkMode &&
		fp.linkTarget == other.linkTarget &&
		fp.resolvedPath == other.resolvedPath &&
		fp.problem == other.problem &&
		slices.Equal(fp.entries, other.entries)
}

// fingerprintGraph fingerprints the graph, allowing tests to simulate in-flight filesystem
// changes through the InstanceWatcherService.fingerprintGraphFunc override.
func (iw *InstanceWatcherService) fingerprintGraph(graph *resolvedConfigGraph) graphFingerprint {
	if iw.fingerprintGraphFunc != nil {
		return iw.fingerprintGraphFunc(graph)
	}

	return fingerprintConfigGraph(graph)
}

// parseConfigCandidate reads one candidate snapshot from disk. Syntax validation, allowed
// directory checks and include reference resolution happen inside the parser; the returned graph
// describes everything that was read.
func (iw *InstanceWatcherService) parseConfigCandidate(
	ctx context.Context,
	instance *mpi.Instance,
) (*model.NginxConfigContext, *resolvedConfigGraph, error) {
	if graphParser, ok := iw.nginxConfigParser.(parser.ConfigParserWithGraph); ok {
		result, err := graphParser.ParseWithGraph(ctx, instance)
		if err != nil {
			return nil, nil, err
		}

		// The payload graph covers every parsed config file. The context additionally lists
		// referenced files such as SSL certificates; fingerprint them too so swapping one while
		// a candidate is being built invalidates the candidate.
		referencedFiles := append(configContextFileNames(result.ConfigContext), result.Graph.Files...)
		graph := resolveConfigGraph(result.Graph.RootPath, referencedFiles, result.Graph.IncludePatterns)

		return result.ConfigContext, graph, nil
	}

	nginxConfigContext, err := iw.nginxConfigParser.Parse(ctx, instance)
	if err != nil {
		return nil, nil, err
	}

	return nginxConfigContext, resolveGraphFromContext(nginxConfigContext), nil
}

func configContextFileNames(nginxConfigContext *model.NginxConfigContext) []string {
	if nginxConfigContext == nil {
		return nil
	}

	fileNames := make([]string, 0, len(nginxConfigContext.Files))
	for _, file := range nginxConfigContext.Files {
		fileNames = append(fileNames, file.GetFileMeta().GetName())
	}

	return fileNames
}

// parseStableConfig reads the instance config from disk only when the include graph is stable:
//
//  1. A candidate is parsed and its include graph fingerprinted immediately after the read.
//  2. After a settle period the same graph is fingerprinted again. If any read file, matched
//     directory or symlink target changed, the candidate is discarded and parsing restarts once
//     the changes have converged.
//  3. The config is parsed a second time and bracketed by identical fingerprints, proving every
//     file used by the accepted snapshot was unchanged from parse start to parse completion.
//
// Missing files, half-written content, include cycles, unreadable directories and other parse
// failures never produce a snapshot: the caller keeps the last confirmed snapshot and retries on
// the next filesystem change once the configuration converges.
func (iw *InstanceWatcherService) parseStableConfig(
	ctx context.Context,
	instance *mpi.Instance,
) (*model.NginxConfigContext, error) {
	return iw.buildStableConfig(ctx, func(parseCtx context.Context) (*model.NginxConfigContext,
		*resolvedConfigGraph, error,
	) {
		return iw.parseConfigCandidate(parseCtx, instance)
	})
}

// parseCandidateFunc reads one candidate and resolves its include graph.
type parseCandidateFunc func(context.Context) (*model.NginxConfigContext, *resolvedConfigGraph, error)

func (iw *InstanceWatcherService) buildStableConfig(
	ctx context.Context,
	parseCandidate parseCandidateFunc,
) (*model.NginxConfigContext, error) {
	maxAttempts := iw.configParseMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultConfigParseMaxAttempts
	}

	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		candidate, reason := iw.stableParseAttempt(ctx, parseCandidate, attempt, maxAttempts)
		if candidate != nil {
			return candidate, nil
		}

		// A canceled or expired context aborts the scan immediately.
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		lastErr = reason
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("unable to read a stable NGINX config after %d attempts: %w",
		maxAttempts, lastErr)
}

// stableParseAttempt performs one candidate read cycle. It returns the confirmed candidate when
// the include graph was identical from parse start to parse completion. Otherwise it returns the
// reason the candidate was discarded so the caller can retry after the filesystem settles.
func (iw *InstanceWatcherService) stableParseAttempt(
	ctx context.Context,
	parseCandidate parseCandidateFunc,
	attempt, maxAttempts int,
) (candidate *model.NginxConfigContext, reason error) {
	// Read the candidate and verify syntax, allowed directories and include references. The
	// context from this first read is discarded: only the second read on the proven stable graph
	// becomes the confirmed snapshot.
	_, graph, err := parseCandidate(ctx)
	if err != nil {
		reason = fmt.Errorf("parse NGINX config candidate: %w", err)
		slog.WarnContext(
			ctx,
			"Unable to parse NGINX config candidate, keeping the last confirmed config",
			"attempt", attempt, "max_attempts", maxAttempts, "error", reason,
		)
		_ = iw.waitForConfigSettle(ctx)

		return nil, reason
	}

	fingerprintAfterParse := iw.fingerprintGraph(graph)
	if !fingerprintAfterParse.valid() {
		reason = fmt.Errorf("NGINX config candidate references unreadable or missing files: %w",
			errIncludeGraphChanged)
		slog.WarnContext(
			ctx,
			"Discarding NGINX config candidate with unreadable references, keeping the last confirmed config",
			"attempt", attempt, "max_attempts", maxAttempts,
		)
		_ = iw.waitForConfigSettle(ctx)

		return nil, reason
	}

	// Wait for batched writes and symlink switches to settle, then verify the graph is identical
	// to what was just parsed.
	_ = iw.waitForConfigSettle(ctx)

	fingerprintAfterSettle := iw.fingerprintGraph(graph)
	if !fingerprintAfterSettle.valid() || !fingerprintAfterParse.equal(fingerprintAfterSettle) {
		slog.DebugContext(
			ctx,
			"Discarding NGINX config candidate: the include graph changed while reading the config",
			"attempt", attempt, "max_attempts", maxAttempts,
		)

		return nil, errIncludeGraphChanged
	}

	// Read the config a second time on the now stable graph. The identical fingerprints before
	// and after this parse bracket the read window, proving no referenced file, matched directory
	// or link target changed between parse start and completion.
	confirmedCandidate, confirmedGraph, err := parseCandidate(ctx)
	if err != nil {
		reason = fmt.Errorf("re-parse stable NGINX config candidate: %w", err)
		slog.WarnContext(
			ctx,
			"Unable to re-parse the stable NGINX config candidate, keeping the last confirmed config",
			"attempt", attempt, "max_attempts", maxAttempts, "error", reason,
		)
		_ = iw.waitForConfigSettle(ctx)

		return nil, reason
	}

	confirmedFingerprint := iw.fingerprintGraph(confirmedGraph)
	if !confirmedFingerprint.valid() || !fingerprintAfterSettle.equal(confirmedFingerprint) {
		slog.DebugContext(
			ctx,
			"Discarding re-parsed NGINX config candidate: the include graph changed during the read",
			"attempt", attempt, "max_attempts", maxAttempts,
		)

		return nil, errIncludeGraphChanged
	}

	slog.DebugContext(
		ctx,
		"Accepted stable NGINX config candidate",
		"attempt", attempt, "files", len(confirmedFingerprint.files),
		"matched_directories", len(confirmedFingerprint.dirs),
	)

	return confirmedCandidate, nil
}

// waitForConfigSettle pauses candidate parsing for the configured settle period so that batched
// config writes and symlink switches can converge. A zero period disables waiting.
func (iw *InstanceWatcherService) waitForConfigSettle(ctx context.Context) error {
	period := iw.configSettlePeriod
	if period <= 0 {
		return ctx.Err()
	}

	timer := time.NewTimer(period)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
