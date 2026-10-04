// Copyright (c) F5, Inc.
//
// This source code is licensed under the Apache License, Version 2.0 license found in the
// LICENSE file in the root directory of this source tree.

package instance

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nginx/agent/v3/pkg/host"
	"github.com/nginx/agent/v3/pkg/host/exec"
	proto2 "google.golang.org/protobuf/proto"

	"github.com/nginx/agent/v3/internal/datasource/proto"

	parser "github.com/nginx/agent/v3/internal/datasource/config"

	"github.com/nginx/agent/v3/pkg/nginxprocess"

	mpi "github.com/nginx/agent/v3/api/grpc/mpi/v1"
	"github.com/nginx/agent/v3/internal/watcher/process"

	"github.com/nginx/agent/v3/internal/config"
	"github.com/nginx/agent/v3/internal/logger"
	"github.com/nginx/agent/v3/internal/model"
)

const defaultAgentPath = "/run/nginx-agent"

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.11.2 -generate
//counterfeiter:generate . processParser

type (
	processParser interface {
		Parse(ctx context.Context, processes []*nginxprocess.Process) map[string]*mpi.Instance
	}

	InstanceWatcherService struct {
		processOperator                process.ProcessOperatorInterface
		nginxAppProtectInstanceWatcher *NginxAppProtectInstanceWatcher
		nginxConfigParser              parser.ConfigParser
		nginxParser                    processParser
		executer                       exec.ExecInterface
		enabled                        *atomic.Bool
		agentConfig                    *config.Config
		instanceCache                  map[string]*mpi.Instance
		nginxConfigCache               map[string]*model.NginxConfigContext
		pendingInstances               map[string]*pendingGeneration
		instancesChannel               chan<- ResourceUpdatesMessage
		nginxConfigContextChannel      chan<- NginxConfigContextMessage
		info                           host.InfoInterface
		resource                       *mpi.Resource
		processCache                   []*nginxprocess.Process
		cacheMutex                     sync.Mutex
		resourceMutex                  sync.Mutex
	}

	// discovery is the process based candidate snapshot for a single monitoring cycle. Nothing in here is confirmed.
	discovery struct {
		nginx     map[string]*mpi.Instance
		napUpdate bool
	}

	// pendingGeneration tracks a discovered process based candidate whose configuration has not been confirmed yet.
	// The confirmed generation for the instance keeps being used until the pending candidate confirms or disappears.
	pendingGeneration struct {
		instanceID string
		processID  int32
		candidate  *mpi.Instance
		attempts   int
		err        error
	}

	ResourceUpdatesMessage struct {
		CorrelationID slog.Attr
		Resource      *mpi.Resource
	}

	NginxConfigContextMessage struct {
		CorrelationID      slog.Attr
		NginxConfigContext *model.NginxConfigContext
	}
)

func NewInstanceWatcherService(agentConfig *config.Config) *InstanceWatcherService {
	enabled := &atomic.Bool{}
	enabled.Store(true)

	napWatcher := NewNginxAppProtectInstanceWatcher(agentConfig)

	instanceWatcherService := &InstanceWatcherService{
		agentConfig:                    agentConfig,
		nginxAppProtectInstanceWatcher: napWatcher,
		processOperator:                process.NewProcessOperator(),
		nginxParser:                    NewNginxProcessParser(),
		nginxConfigParser:              parser.NewNginxConfigParser(agentConfig),
		instanceCache:                  make(map[string]*mpi.Instance),
		cacheMutex:                     sync.Mutex{},
		resourceMutex:                  sync.Mutex{},
		nginxConfigCache:               make(map[string]*model.NginxConfigContext),
		pendingInstances:               make(map[string]*pendingGeneration),
		executer:                       &exec.Exec{},
		info:                           host.NewInfo(),
		resource:                       &mpi.Resource{},
		enabled:                        enabled,
		processCache:                   []*nginxprocess.Process{},
	}

	return instanceWatcherService
}

func (iw *InstanceWatcherService) SetEnabled(enabled bool) {
	iw.enabled.Store(enabled)
}

func (iw *InstanceWatcherService) Watch(
	ctx context.Context,
	instancesChannel chan<- ResourceUpdatesMessage,
	nginxConfigContextChannel chan<- NginxConfigContextMessage,
) {
	iw.updateResourceInfo(ctx)
	go iw.nginxAppProtectInstanceWatcher.Watch(ctx)

	monitoringFrequency := iw.agentConfig.Watchers.InstanceWatcher.MonitoringFrequency
	slog.DebugContext(ctx, "Starting instance watcher monitoring", "monitoring_frequency", monitoringFrequency)

	iw.instancesChannel = instancesChannel
	iw.nginxConfigContextChannel = nginxConfigContextChannel

	instanceWatcherTicker := time.NewTicker(monitoringFrequency)
	defer instanceWatcherTicker.Stop()

	for {
		select {
		case <-ctx.Done():

			return
		case <-instanceWatcherTicker.C:
			if iw.enabled.Load() {
				iw.checkForUpdates(ctx)
			} else {
				slog.DebugContext(ctx, "Skipping check for instance updates, instance watcher is disabled")
			}
		}
	}
}

func (iw *InstanceWatcherService) ReparseConfigs(ctx context.Context) {
	slog.DebugContext(ctx, "Reparsing all instance configurations")

	iw.cacheMutex.Lock()
	instances := make([]*mpi.Instance, 0, len(iw.instanceCache))
	for _, instance := range iw.instanceCache {
		instances = append(instances, instance)
	}
	iw.cacheMutex.Unlock()

	for _, instance := range instances {
		nginxConfigContext := &model.NginxConfigContext{}
		var parseErr error
		slog.DebugContext(
			ctx,
			"Reparsing NGINX instance config",
			"instance_id", instance.GetInstanceMeta().GetInstanceId(),
		)

		if instance.GetInstanceMeta().GetInstanceType() == mpi.InstanceMeta_INSTANCE_TYPE_NGINX ||
			instance.GetInstanceMeta().GetInstanceType() == mpi.InstanceMeta_INSTANCE_TYPE_NGINX_PLUS {
			nginxConfigContext, parseErr = iw.nginxConfigParser.Parse(ctx, instance)
			if parseErr != nil {
				slog.WarnContext(
					ctx,
					"Failed to parse NGINX instance config",
					"config_path", instance.GetInstanceRuntime().GetConfigPath(),
					"instance_id", instance.GetInstanceMeta().GetInstanceId(),
					"error", parseErr,
				)

				continue
			}
		}

		iw.HandleNginxConfigContextUpdate(ctx, instance.GetInstanceMeta().GetInstanceId(), nginxConfigContext)
	}
	slog.DebugContext(ctx, "Finished reparsing all instance configurations")
}

func (iw *InstanceWatcherService) HandleNginxConfigContextUpdate(ctx context.Context, instanceID string,
	nginxConfigContext *model.NginxConfigContext,
) {
	iw.cacheMutex.Lock()
	defer iw.cacheMutex.Unlock()

	updatesRequired := false
	instance := iw.instanceCache[instanceID]
	instanceType := instance.GetInstanceMeta().GetInstanceType()
	correlationID := logger.CorrelationIDAttr(ctx)

	if instanceType == mpi.InstanceMeta_INSTANCE_TYPE_NGINX ||
		instanceType == mpi.InstanceMeta_INSTANCE_TYPE_NGINX_PLUS {
		iw.sendNginxConfigContextUpdate(ctx, nginxConfigContext)
		iw.nginxConfigCache[nginxConfigContext.InstanceID] = nginxConfigContext
		updatesRequired = proto.UpdateNginxInstanceRuntime(instance, nginxConfigContext)
	}

	if updatesRequired {
		iw.updateInstanceInResource(ctx, instance)
		select {
		case iw.instancesChannel <- ResourceUpdatesMessage{
			CorrelationID: correlationID,
			Resource:      iw.resource,
		}:
		case <-ctx.Done():
			return
		}
	}
}

// checkForUpdates performs one discovery/confirmation cycle. Process discovery and configuration confirmation are
// kept separate: a process based candidate only becomes the confirmed generation of an instance once its
// configuration parses successfully. Candidates whose configuration cannot be parsed yet, for example while the
// configuration files are being atomically replaced, remain pending; the last confirmed configuration and collection
// keep being used, the pending error is logged and the still alive candidate is retried on every following cycle
// until it confirms, is superseded by a newer candidate or disappears.
//
// On confirmation the instance resource and the configuration context are published from the same generation, so
// downstream configuration context, file watcher and collector receivers switch to the same generation once and the
// added, updated and removed events are neither duplicated nor lost.
func (iw *InstanceWatcherService) checkForUpdates(
	ctx context.Context,
) {
	correlationID := logger.GenerateCorrelationID()
	newCtx := context.WithValue(ctx, logger.CorrelationIDContextKey, correlationID)

	found, err := iw.discover(newCtx)
	if err != nil {
		slog.ErrorContext(newCtx, "Instance watcher updates", "error", err)
	}
	if found == nil {
		return
	}

	iw.cacheMutex.Lock()

	agentInstance := iw.agentInstance(newCtx)

	confirmed := make(map[string]*mpi.Instance, len(iw.instanceCache)+1)
	for instanceID, instance := range iw.instanceCache {
		confirmed[instanceID] = instance
	}

	pending := make(map[string]*pendingGeneration, len(iw.pendingInstances))
	for instanceID, candidate := range iw.pendingInstances {
		pending[instanceID] = candidate
	}

	changed := false

	if oldAgent, ok := iw.instanceCache[agentInstance.GetInstanceMeta().GetInstanceId()]; !ok ||
		!areInstancesEqual(oldAgent.GetInstanceRuntime(), agentInstance.GetInstanceRuntime()) {
		changed = true
	}
	confirmed[agentInstance.GetInstanceMeta().GetInstanceId()] = agentInstance

	// Drop pending generations that have been superseded by a different, still alive candidate or disappeared.
	for instanceID, candidateGeneration := range pending {
		latest, ok := found.nginx[instanceID]
		if !ok || latest.GetInstanceRuntime().GetProcessId() != candidateGeneration.processID {
			slog.DebugContext(newCtx,
				"Removing unconfirmed NGINX generation, candidate is no longer the latest alive process",
				"instance_id", instanceID, "candidate_process_id", candidateGeneration.processID)
			delete(pending, instanceID)
		}
	}

	// Remove confirmed generations whose instances are no longer present.
	for instanceID := range confirmed {
		if instanceID == agentInstance.GetInstanceMeta().GetInstanceId() {
			continue
		}
		if _, ok := found.nginx[instanceID]; !ok {
			slog.InfoContext(newCtx, "NGINX instance no longer exists", "instance_id", instanceID)
			delete(confirmed, instanceID)
			delete(iw.nginxConfigCache, instanceID)
			changed = true
		}
	}

	var contextUpdates []*model.NginxConfigContext

	// Confirm candidates matching the current configuration, retry the ones that are still unconfirmed.
	for instanceID, candidate := range found.nginx {
		if current, ok := confirmed[instanceID]; ok &&
			areInstancesEqual(current.GetInstanceRuntime(), candidate.GetInstanceRuntime()) {
			continue
		}

		candidateGeneration := pending[instanceID]
		if candidateGeneration == nil ||
			candidateGeneration.processID != candidate.GetInstanceRuntime().GetProcessId() {
			candidateGeneration = newPendingGeneration(candidate)
			pending[instanceID] = candidateGeneration
		}

		nginxConfigContext, parseErr := iw.nginxConfigParser.Parse(newCtx, candidate)
		candidateGeneration.attempt(parseErr)

		if parseErr != nil || nginxConfigContext == nil {
			slog.WarnContext(newCtx,
				"NGINX configuration for discovered process could not be confirmed, "+
					"continuing to use the last confirmed configuration and retrying",
				"instance_id", instanceID,
				"candidate_process_id", candidateGeneration.processID,
				"attempts", candidateGeneration.attempts,
				"error", parseErr,
			)

			continue
		}

		proto.UpdateNginxInstanceRuntime(candidate, nginxConfigContext)

		if cached := iw.nginxConfigCache[instanceID]; cached == nil ||
			!cached.Equal(nginxConfigContext) {
			contextUpdates = append(contextUpdates, nginxConfigContext)
		}
		iw.nginxConfigCache[instanceID] = nginxConfigContext
		confirmed[instanceID] = candidate
		delete(pending, instanceID)
		changed = true

		slog.InfoContext(newCtx, "Confirmed NGINX instance generation",
			"instance_id", instanceID,
			"process_id", candidate.GetInstanceRuntime().GetProcessId(),
		)
	}

	if found.napUpdate {
		changed = true
	}

	instancesToPublish := make([]*mpi.Instance, 0, len(confirmed)+1)
	for _, instance := range confirmed {
		instancesToPublish = append(instancesToPublish, instance)
	}
	if napInstance := iw.nginxAppProtectInstanceWatcher.NginxAppProtectInstance(); napInstance != nil {
		instancesToPublish = append(instancesToPublish, napInstance)
	}

	iw.instanceCache = confirmed
	iw.pendingInstances = pending

	iw.cacheMutex.Unlock()

	for _, nginxConfigContext := range contextUpdates {
		select {
		case iw.nginxConfigContextChannel <- NginxConfigContextMessage{
			CorrelationID:      correlationID,
			NginxConfigContext: nginxConfigContext,
		}:
		case <-ctx.Done():
			return
		}
	}

	if changed {
		iw.updateResourceInstanceList(ctx, instancesToPublish)

		select {
		case iw.instancesChannel <- ResourceUpdatesMessage{
			CorrelationID: correlationID,
			Resource:      iw.resource,
		}:
		case <-ctx.Done():
			return
		}
	}
}

func (iw *InstanceWatcherService) updateResourceInstanceList(ctx context.Context, instances []*mpi.Instance) {
	iw.resourceMutex.Lock()
	defer iw.resourceMutex.Unlock()

	slog.DebugContext(ctx, "Updating resource instance list", "instances", len(instances))

	resourceCopy, ok := proto2.Clone(iw.resource).(*mpi.Resource)
	if !ok {
		slog.WarnContext(ctx, "Unable to clone resource while updating instances", "resource",
			iw.resource, "instances", instances)

		return
	}
	resourceCopy.Instances = instances

	iw.resource = resourceCopy
}

func (iw *InstanceWatcherService) updateInstanceInResource(ctx context.Context, updatedInstance *mpi.Instance) {
	iw.resourceMutex.Lock()
	defer iw.resourceMutex.Unlock()

	resourceCopy, ok := proto2.Clone(iw.resource).(*mpi.Resource)

	if !ok {
		slog.WarnContext(ctx, "Unable to clone resource while updating instances", "resource",
			iw.resource, "instances", updatedInstance)

		return
	}

	for _, instance := range resourceCopy.GetInstances() {
		if instance.GetInstanceMeta().GetInstanceId() == updatedInstance.GetInstanceMeta().GetInstanceId() {
			instance.InstanceMeta = updatedInstance.GetInstanceMeta()
			instance.InstanceRuntime = updatedInstance.GetInstanceRuntime()
			instance.InstanceConfig = updatedInstance.GetInstanceConfig()
		}
	}

	iw.resource = resourceCopy
}

func (iw *InstanceWatcherService) sendNginxConfigContextUpdate(
	ctx context.Context,
	nginxConfigContext *model.NginxConfigContext,
) {
	if iw.nginxConfigCache[nginxConfigContext.InstanceID] == nil ||
		!iw.nginxConfigCache[nginxConfigContext.InstanceID].Equal(nginxConfigContext) {
		slog.DebugContext(
			ctx,
			"New NGINX config context",
			"instance_id", nginxConfigContext.InstanceID,
			"nginx_config_context", nginxConfigContext,
		)

		select {
		case iw.nginxConfigContextChannel <- NginxConfigContextMessage{
			CorrelationID:      logger.CorrelationIDAttr(ctx),
			NginxConfigContext: nginxConfigContext,
		}:
		case <-ctx.Done():
			return
		}
	}
}

// discover gathers the current NGINX processes and turns them into unconfirmed instance candidates. It only performs
// process discovery, it never updates the confirmed instance state.
func (iw *InstanceWatcherService) discover(ctx context.Context) (*discovery, error) {
	nginxProcesses, err := iw.processOperator.Processes(ctx)
	if err != nil {
		return nil, err
	}

	if !slices.EqualFunc(iw.processCache, nginxProcesses, func(a, b *nginxprocess.Process) bool {
		return a.Equal(b)
	}) {
		processesJSON, marshalErr := json.Marshal(nginxProcesses)
		if marshalErr != nil {
			slog.DebugContext(ctx, "Unable to marshal NGINX processes", "error", marshalErr)
		} else {
			slog.DebugContext(ctx, "NGINX processes changed", "processes", processesJSON)
			iw.processCache = nginxProcesses
		}
	}

	nginxInstances := iw.nginxParser.Parse(ctx, nginxProcesses)
	napUpdate := iw.nginxAppProtectInstanceWatcher.checkForAppProtectUpdates(ctx)

	return &discovery{
		nginx:     nginxInstances,
		napUpdate: napUpdate,
	}, nil
}

func newPendingGeneration(candidate *mpi.Instance) *pendingGeneration {
	return &pendingGeneration{
		instanceID: candidate.GetInstanceMeta().GetInstanceId(),
		processID:  candidate.GetInstanceRuntime().GetProcessId(),
		candidate:  candidate,
	}
}

// attempt records one configuration confirmation attempt and the error that prevented confirmation, if any.
func (p *pendingGeneration) attempt(err error) {
	p.attempts++
	p.err = err
}

func (iw *InstanceWatcherService) agentInstance(ctx context.Context) *mpi.Instance {
	processPath, err := iw.executer.Executable()
	if err != nil {
		processPath = defaultAgentPath
		slog.WarnContext(ctx, "Unable to read process location, defaulting to /var/run/nginx-agent", "error", err)
	}

	labels, convertErr := mpi.ConvertToStructs(iw.agentConfig.Labels)
	if convertErr != nil {
		slog.WarnContext(ctx, "Unable to convert config to labels structure", "error", convertErr)
	}

	instance := &mpi.Instance{
		InstanceMeta: &mpi.InstanceMeta{
			InstanceId:   iw.agentConfig.UUID,
			InstanceType: mpi.InstanceMeta_INSTANCE_TYPE_AGENT,
			Version:      iw.agentConfig.Version,
		},
		InstanceConfig: &mpi.InstanceConfig{
			Actions: []*mpi.InstanceAction{},
			Config: &mpi.InstanceConfig_AgentConfig{
				AgentConfig: &mpi.AgentConfig{
					Command:           config.ToCommandProto(iw.agentConfig.Command),
					Metrics:           &mpi.MetricsServer{},
					File:              &mpi.FileServer{},
					Labels:            labels,
					Features:          iw.agentConfig.Features,
					MessageBufferSize: "",
					Log:               config.ToAgentConfigLogProto(iw.agentConfig.Log),
				},
			},
		},
		InstanceRuntime: &mpi.InstanceRuntime{
			ProcessId:  iw.executer.ProcessID(),
			BinaryPath: processPath,
			ConfigPath: iw.agentConfig.Path,
			Details:    nil,
		},
	}

	if iw.agentConfig.IsAuxiliaryCommandGrpcClientConfigured() {
		instance.GetInstanceConfig().GetAgentConfig().AuxiliaryCommand = config.
			ToAuxiliaryCommandServerProto(iw.agentConfig.AuxiliaryCommand)
	}

	return instance
}

func areInstancesEqual(oldRuntime, currentRuntime *mpi.InstanceRuntime) (equal bool) {
	if oldRuntime.GetProcessId() != currentRuntime.GetProcessId() {
		return false
	}

	oldRuntimeChildren := oldRuntime.GetInstanceChildren()
	currentRuntimeChildren := currentRuntime.GetInstanceChildren()

	if len(oldRuntimeChildren) != len(currentRuntimeChildren) {
		return false
	}

	for _, oldChild := range oldRuntimeChildren {
		childFound := false
		for _, currentChild := range currentRuntimeChildren {
			if oldChild.GetProcessId() == currentChild.GetProcessId() {
				childFound = true
				break
			}
		}

		if !childFound {
			return false
		}
	}

	return true
}

func (iw *InstanceWatcherService) updateResourceInfo(ctx context.Context) {
	iw.resourceMutex.Lock()
	defer iw.resourceMutex.Unlock()

	isContainer, err := iw.info.IsContainer()
	if err != nil {
		slog.WarnContext(ctx, "Failed to check if resource is container", "error", err)
	}

	if isContainer {
		iw.resource.Info, err = iw.info.ContainerInfo(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to get container info", "error", err)
			return
		}
		iw.resource.ResourceId = iw.resource.GetContainerInfo().GetContainerId()
		iw.resource.Instances = []*mpi.Instance{}
	} else {
		iw.resource.Info, err = iw.info.HostInfo(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to get host info", "error", err)
			return
		}
		iw.resource.ResourceId = iw.resource.GetHostInfo().GetHostId()
		iw.resource.Instances = []*mpi.Instance{}
	}
}
