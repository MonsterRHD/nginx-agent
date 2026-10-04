// Copyright (c) F5, Inc.
//
// This source code is licensed under the Apache License, Version 2.0 license found in the
// LICENSE file in the root directory of this source tree.

package instance

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/nginx/agent/v3/internal/datasource/config/configfakes"
	"github.com/nginx/agent/v3/pkg/host/exec/execfakes"

	"github.com/nginx/agent/v3/internal/watcher/instance/instancefakes"
	"github.com/nginx/agent/v3/internal/watcher/process/processfakes"

	mpi "github.com/nginx/agent/v3/api/grpc/mpi/v1"
	"github.com/nginx/agent/v3/internal/model"
	testModel "github.com/nginx/agent/v3/test/model"
	"github.com/nginx/agent/v3/test/protos"
	"github.com/nginx/agent/v3/test/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testProcessID matches the NGINX process id fixture used by test/protos.
const testProcessID int32 = 1234

func TestInstanceWatcherService_checkForUpdates(t *testing.T) {
	ctx := context.Background()

	nginxConfigContext := testModel.ConfigContext()

	fakeProcessWatcher := &processfakes.FakeProcessOperatorInterface{}
	fakeProcessWatcher.ProcessesReturns(nil, nil)

	fakeProcessParser := &instancefakes.FakeProcessParser{}
	fakeProcessParser.ParseReturns(map[string]*mpi.Instance{
		protos.NginxOssInstance([]string{}).GetInstanceMeta().GetInstanceId(): protos.
			NginxOssInstance([]string{}),
	})

	fakeNginxConfigParser := &configfakes.FakeConfigParser{}
	fakeNginxConfigParser.ParseReturns(nginxConfigContext, nil)
	instanceUpdatesChannel := make(chan ResourceUpdatesMessage, 1)
	nginxConfigContextChannel := make(chan NginxConfigContextMessage, 1)

	instanceWatcherService := NewInstanceWatcherService(types.AgentConfig())
	instanceWatcherService.processOperator = fakeProcessWatcher
	instanceWatcherService.nginxParser = fakeProcessParser
	instanceWatcherService.nginxConfigParser = fakeNginxConfigParser
	instanceWatcherService.instancesChannel = instanceUpdatesChannel
	instanceWatcherService.nginxConfigContextChannel = nginxConfigContextChannel

	instanceWatcherService.checkForUpdates(ctx)

	instanceUpdatesMessage := <-instanceUpdatesChannel
	assert.Len(t, instanceUpdatesMessage.Resource.GetInstances(), 2)

	nginxConfigContextMessage := <-nginxConfigContextChannel
	assert.Equal(t, nginxConfigContext, nginxConfigContextMessage.NginxConfigContext)
}

func TestInstanceWatcherService_checkForUpdates_TopologyChanges(t *testing.T) {
	ctx := context.Background()
	processID := int32(123)

	agentInstance := protos.AgentInstance(processID, types.AgentConfig())
	nginxInstance := protos.NginxOssInstance([]string{})
	nginxInstanceWithDifferentPID := protos.NginxOssInstance([]string{})
	nginxInstanceWithDifferentPID.GetInstanceRuntime().ProcessId = 3526

	tests := []struct {
		name          string
		initialCache  map[string]*mpi.Instance
		candidates    map[string]*mpi.Instance
		expectedPids  []int32
		expectPublish bool
	}{
		{
			name: "Test 1: No updates, same confirmed generation is not republished",
			initialCache: map[string]*mpi.Instance{
				agentInstance.GetInstanceMeta().GetInstanceId(): agentInstance,
				nginxInstance.GetInstanceMeta().GetInstanceId(): nginxInstance,
			},
			candidates: map[string]*mpi.Instance{
				nginxInstance.GetInstanceMeta().GetInstanceId(): nginxInstance,
			},
			expectPublish: false,
		},
		{
			name: "Test 2: New instance is confirmed and published",
			initialCache: map[string]*mpi.Instance{
				agentInstance.GetInstanceMeta().GetInstanceId(): agentInstance,
			},
			candidates: map[string]*mpi.Instance{
				nginxInstance.GetInstanceMeta().GetInstanceId(): nginxInstance,
			},
			expectedPids:  []int32{processID, processIDFor(nginxInstance)},
			expectPublish: true,
		},
		{
			name: "Test 3: Updated generation takes over once confirmed",
			initialCache: map[string]*mpi.Instance{
				agentInstance.GetInstanceMeta().GetInstanceId():                 agentInstance,
				nginxInstanceWithDifferentPID.GetInstanceMeta().GetInstanceId(): nginxInstanceWithDifferentPID,
			},
			candidates: map[string]*mpi.Instance{
				nginxInstance.GetInstanceMeta().GetInstanceId(): nginxInstance,
			},
			expectedPids:  []int32{processID, processIDFor(nginxInstance)},
			expectPublish: true,
		},
		{
			name: "Test 4: Deleted instance is removed from the resource",
			initialCache: map[string]*mpi.Instance{
				agentInstance.GetInstanceMeta().GetInstanceId(): agentInstance,
				nginxInstance.GetInstanceMeta().GetInstanceId(): nginxInstance,
			},
			candidates:    make(map[string]*mpi.Instance),
			expectedPids:  []int32{processID},
			expectPublish: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			fakeNginxConfigParser := &configfakes.FakeConfigParser{}
			fakeNginxConfigParser.ParseReturns(testModel.ConfigContext(), nil)

			watcher, resourceChannel, configContextChannel :=
				setupTestInstanceWatcher(tt, test.candidates, fakeNginxConfigParser)
			watcher.instanceCache = test.initialCache

			// First cycle publishes the initial/changed topology
			watcher.checkForUpdates(ctx)

			if test.expectPublish {
				msg := <-resourceChannel
				require.Len(tt, msg.Resource.GetInstances(), len(test.expectedPids))
				actualPids := make([]int32, 0, len(test.expectedPids))
				for _, published := range msg.Resource.GetInstances() {
					actualPids = append(actualPids, published.GetInstanceRuntime().GetProcessId())
				}
				slices.Sort(actualPids)
				assert.Equal(tt, test.expectedPids, actualPids)
			} else {
				// initial cache matches candidates, nothing to publish
				assertEmptyResourceChannel(tt, resourceChannel)
			}
			// A second cycle on the same generation never republishes
			watcher.checkForUpdates(ctx)
			assertEmptyResourceChannel(tt, resourceChannel)
			drainConfigContextChannel(configContextChannel)
		})
	}
}

// TestInstanceWatcherService_checkForUpdates_ConfigParseFailure verifies the core fix: a discovered process whose
// configuration cannot be parsed yet stays pending. The last confirmed configuration keeps being used, the pending
// error is recorded and the still alive candidate is retried every cycle until it confirms. The successful
// generation is published exactly once.
func TestInstanceWatcherService_checkForUpdates_ConfigParseFailure(t *testing.T) {
	ctx := context.Background()

	nginxInstance := protos.NginxOssInstance([]string{})
	instanceID := nginxInstance.GetInstanceMeta().GetInstanceId()

	newGeneration := protos.NginxOssInstance([]string{})
	newGeneration.GetInstanceRuntime().ProcessId = 3526

	newConfigContext := testModel.ConfigContext()
	newConfigContext.AccessLogs = []*model.AccessLog{{Name: "access2.log"}}

	fakeNginxConfigParser := &configfakes.FakeConfigParser{}
	fakeNginxConfigParser.ParseReturns(testModel.ConfigContext(), nil)

	watcher, resourceChannel, configContextChannel :=
		setupTestInstanceWatcher(t, map[string]*mpi.Instance{
			instanceID: nginxInstance,
		}, fakeNginxConfigParser)

	// Cycle 1: confirm the initial generation
	watcher.checkForUpdates(ctx)
	resourceMsg := <-resourceChannel
	initialConfigMsg := <-configContextChannel
	require.Len(t, resourceMsg.Resource.GetInstances(), 2)
	assert.Equal(t, testProcessID, findInstance(t, resourceMsg.Resource, instanceID).
		GetInstanceRuntime().GetProcessId())

	// Cycle 2: new generation discovered, parsing fails. Nothing is published and the confirmed state is retained.
	fakeNginxConfigParser.ParseReturns(nil, errors.New("config parse error"))
	watcher.processParserFake(map[string]*mpi.Instance{instanceID: newGeneration})

	watcher.checkForUpdates(ctx)

	assertEmptyResourceChannel(t, resourceChannel)
	assertEmptyConfigContextChannel(t, configContextChannel)
	assert.Equal(t, testProcessID, watcher.instanceCache[instanceID].GetInstanceRuntime().GetProcessId(),
		"last confirmed generation must be retained")
	require.Contains(t, watcher.pendingInstances, instanceID)
	assert.Equal(t, int32(3526), watcher.pendingInstances[instanceID].processID)
	assert.Equal(t, 1, watcher.pendingInstances[instanceID].attempts)
	require.Error(t, watcher.pendingInstances[instanceID].err)

	// Cycle 3: candidate still alive, parsing keeps failing, the agent keeps retrying.
	watcher.checkForUpdates(ctx)
	assertEmptyResourceChannel(t, resourceChannel)
	assert.Equal(t, 2, watcher.pendingInstances[instanceID].attempts,
		"pending candidate must be retried on every cycle")
	assert.Equal(t, testProcessID, watcher.instanceCache[instanceID].GetInstanceRuntime().GetProcessId())

	// Cycle 4: parsing succeeds, the new generation confirms exactly once.
	fakeNginxConfigParser.ParseReturns(newConfigContext, nil)
	watcher.checkForUpdates(ctx)

	resourceMsg = <-resourceChannel
	newConfigMsg := <-configContextChannel
	assertEmptyResourceChannel(t, resourceChannel)
	assertEmptyConfigContextChannel(t, configContextChannel)

	assert.Equal(t, newConfigContext, newConfigMsg.NginxConfigContext)
	assert.False(t, initialConfigMsg.NginxConfigContext.Equal(newConfigMsg.NginxConfigContext),
		"the confirmed config context must be the new generation, not the last confirmed one")
	assert.Equal(t, int32(3526),
		findInstance(t, resourceMsg.Resource, instanceID).GetInstanceRuntime().GetProcessId())
	assert.Equal(t, []string{"access2.log"},
		findInstance(t, resourceMsg.Resource, instanceID).GetInstanceRuntime().GetNginxRuntimeInfo().GetAccessLogs())
	assert.NotContains(t, watcher.pendingInstances, instanceID)
	assert.Equal(t, int32(3526), watcher.instanceCache[instanceID].GetInstanceRuntime().GetProcessId())
}

// TestInstanceWatcherService_checkForUpdates_PendingCandidateDisappears verifies that an unconfirmed candidate
// disappearing clears its pending state. If the instance is gone a single removal is published; if the confirmed
// generation is still alive (for example a failed upgrade falls back to the old master) nothing changes.
func TestInstanceWatcherService_checkForUpdates_PendingCandidateDisappears(t *testing.T) {
	ctx := context.Background()

	nginxInstance := protos.NginxOssInstance([]string{})
	instanceID := nginxInstance.GetInstanceMeta().GetInstanceId()

	failedGeneration := protos.NginxOssInstance([]string{})
	failedGeneration.GetInstanceRuntime().ProcessId = 3526

	t.Run("instance gone: pending cleared, removal published once", func(tt *testing.T) {
		fakeNginxConfigParser := &configfakes.FakeConfigParser{}
		fakeNginxConfigParser.ParseReturns(nil, errors.New("config parse error"))

		watcher, resourceChannel, _ :=
			setupTestInstanceWatcher(tt, map[string]*mpi.Instance{
				instanceID: failedGeneration,
			}, fakeNginxConfigParser)
		watcher.instanceCache = map[string]*mpi.Instance{
			protos.AgentInstance(123, types.AgentConfig()).GetInstanceMeta().GetInstanceId(): protos.AgentInstance(123, types.AgentConfig()),
			instanceID: nginxInstance,
		}

		// Pending generation fails to confirm
		watcher.checkForUpdates(ctx)
		drainResourceChannel(resourceChannel)
		require.Contains(tt, watcher.pendingInstances, instanceID)

		// Candidate disappears on the next cycle
		watcher.processParserFake(make(map[string]*mpi.Instance))
		watcher.checkForUpdates(ctx)

		resourceMsg := <-resourceChannel
		assertEmptyResourceChannel(tt, resourceChannel)
		assert.Len(tt, resourceMsg.Resource.GetInstances(), 1)
		assert.NotContains(tt, watcher.pendingInstances, instanceID)
		assert.NotContains(tt, watcher.instanceCache, instanceID)
		assert.NotContains(tt, watcher.nginxConfigCache, instanceID)
	})

	t.Run("confirmed generation still alive: pending cleared, no update", func(tt *testing.T) {
		fakeNginxConfigParser := &configfakes.FakeConfigParser{}
		fakeNginxConfigParser.ParseReturns(nil, errors.New("config parse error"))

		watcher, resourceChannel, _ :=
			setupTestInstanceWatcher(tt, map[string]*mpi.Instance{
				instanceID: failedGeneration,
			}, fakeNginxConfigParser)

		// Confirm the original generation first
		fakeNginxConfigParser.ParseReturns(testModel.ConfigContext(), nil)
		watcher.processParserFake(map[string]*mpi.Instance{instanceID: nginxInstance})
		watcher.checkForUpdates(ctx)
		drainResourceChannel(resourceChannel)

		// Upgrade candidate appears and fails
		fakeNginxConfigParser.ParseReturns(nil, errors.New("config parse error"))
		watcher.processParserFake(map[string]*mpi.Instance{instanceID: failedGeneration})
		watcher.checkForUpdates(ctx)
		assertEmptyResourceChannel(tt, resourceChannel)
		require.Contains(tt, watcher.pendingInstances, instanceID)

		// Failed candidate gone, old master is the candidate again and matches the confirmed generation
		watcher.processParserFake(map[string]*mpi.Instance{instanceID: nginxInstance})
		watcher.checkForUpdates(ctx)

		assertEmptyResourceChannel(tt, resourceChannel)
		assert.NotContains(tt, watcher.pendingInstances, instanceID)
		assert.Equal(tt, testProcessID, watcher.instanceCache[instanceID].GetInstanceRuntime().GetProcessId())
	})
}

// TestInstanceWatcherService_checkForUpdates_OnlyLatestCandidateConfirms verifies that with rapid successive
// generations only the latest alive candidate can take over. Superseded candidates produce no events and their
// pending state is discarded.
func TestInstanceWatcherService_checkForUpdates_OnlyLatestCandidateConfirms(t *testing.T) {
	ctx := context.Background()

	nginxInstance := protos.NginxOssInstance([]string{})
	instanceID := nginxInstance.GetInstanceMeta().GetInstanceId()

	candidate1 := protos.NginxOssInstance([]string{})
	candidate1.GetInstanceRuntime().ProcessId = 3526

	candidate2 := protos.NginxOssInstance([]string{})
	candidate2.GetInstanceRuntime().ProcessId = 5678

	candidate2ConfigContext := testModel.ConfigContext()
	candidate2ConfigContext.AccessLogs = []*model.AccessLog{{Name: "access2.log"}}

	fakeNginxConfigParser := &configfakes.FakeConfigParser{}

	watcher, resourceChannel, configContextChannel :=
		setupTestInstanceWatcher(t, map[string]*mpi.Instance{
			instanceID: candidate1,
		}, fakeNginxConfigParser)

	// Candidate 1 fails to confirm: only the agent is published on the first cycle, the NGINX candidate is not
	fakeNginxConfigParser.ParseReturns(nil, errors.New("config parse error"))
	watcher.checkForUpdates(ctx)
	agentOnlyMsg := <-resourceChannel
	assert.Len(t, agentOnlyMsg.Resource.GetInstances(), 1)
	assert.Equal(t, mpi.InstanceMeta_INSTANCE_TYPE_AGENT,
		agentOnlyMsg.Resource.GetInstances()[0].GetInstanceMeta().GetInstanceType())
	assertEmptyConfigContextChannel(t, configContextChannel)
	require.Contains(t, watcher.pendingInstances, instanceID)
	assert.Equal(t, int32(3526), watcher.pendingInstances[instanceID].processID)

	// Candidate 2 supersedes candidate 1 and confirms
	fakeNginxConfigParser.ParseReturns(candidate2ConfigContext, nil)
	watcher.processParserFake(map[string]*mpi.Instance{instanceID: candidate2})
	watcher.checkForUpdates(ctx)

	// Exactly one resource update and one config context update for the confirmed generation
	resourceMsg := <-resourceChannel
	configMsg := <-configContextChannel
	assertEmptyResourceChannel(t, resourceChannel)
	assertEmptyConfigContextChannel(t, configContextChannel)

	assert.Equal(t, candidate2ConfigContext, configMsg.NginxConfigContext)
	assert.Equal(t, int32(5678),
		findInstance(t, resourceMsg.Resource, instanceID).GetInstanceRuntime().GetProcessId())
	assert.NotContains(t, watcher.pendingInstances, instanceID)
	assert.Equal(t, int32(5678), watcher.instanceCache[instanceID].GetInstanceRuntime().GetProcessId())
}

func TestInstanceWatcherService_checkForUpdates_NapMetadataRepublish(t *testing.T) {
	ctx := context.Background()
	nginxInstance := protos.NginxOssInstance([]string{})

	napInstance := protos.NginxAppProtectInstance()

	tests := []struct {
		name               string
		napState           *mpi.Instance
		napVersion         string
		napRelease         string
		expectNapPresent   bool
		expectedNapRelease string
	}{
		{
			name:               "Test 1: New NAP instance is added to the resource",
			napState:           nil,
			napVersion:         "6",
			napRelease:         napInstance.GetInstanceRuntime().GetNginxAppProtectRuntimeInfo().GetRelease(),
			expectNapPresent:   true,
			expectedNapRelease: napInstance.GetInstanceRuntime().GetNginxAppProtectRuntimeInfo().GetRelease(),
		},
		{
			name:               "Test 2: NAP metadata updated",
			napState:           napInstance,
			napVersion:         napInstance.GetInstanceMeta().GetVersion(),
			napRelease:         "1.2.3.4",
			expectNapPresent:   true,
			expectedNapRelease: "1.2.3.4",
		},
		{
			name:             "Test 3: NAP uninstalled, NAP instance removed from the resource",
			napState:         napInstance,
			napVersion:       "",
			napRelease:       "",
			expectNapPresent: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			fakeNginxConfigParser := &configfakes.FakeConfigParser{}
			fakeNginxConfigParser.ParseReturns(testModel.ConfigContext(), nil)

			watcher, resourceChannel, _ :=
				setupTestInstanceWatcher(tt, map[string]*mpi.Instance{
					nginxInstance.GetInstanceMeta().GetInstanceId(): nginxInstance,
				}, fakeNginxConfigParser)
			watcher.instanceCache = map[string]*mpi.Instance{
				protos.AgentInstance(123, types.AgentConfig()).GetInstanceMeta().GetInstanceId(): protos.AgentInstance(123, types.AgentConfig()),
				nginxInstance.GetInstanceMeta().GetInstanceId():                                  nginxInstance,
			}
			watcher.nginxAppProtectInstanceWatcher.nginxAppProtectInstance = test.napState
			watcher.nginxAppProtectInstanceWatcher.version = test.napVersion
			watcher.nginxAppProtectInstanceWatcher.release = test.napRelease
			if test.napVersion != "" {
				napInstance.GetInstanceMeta().Version = test.napVersion
			}

			watcher.checkForUpdates(ctx)

			resourceMsg := <-resourceChannel
			var napFound *mpi.Instance
			for _, instance := range resourceMsg.Resource.GetInstances() {
				if instance.GetInstanceMeta().GetInstanceType() == mpi.InstanceMeta_INSTANCE_TYPE_NGINX_APP_PROTECT {
					napFound = instance
				}
			}
			if test.expectNapPresent {
				require.NotNil(tt, napFound)
				assert.Equal(tt, test.expectedNapRelease,
					napFound.GetInstanceRuntime().GetNginxAppProtectRuntimeInfo().GetRelease())
			} else {
				assert.Nil(tt, napFound)
			}
		})
	}
}

func setupTestInstanceWatcher(
	t *testing.T,
	nginxInstances map[string]*mpi.Instance,
	configParser *configfakes.FakeConfigParser,
) (*InstanceWatcherService, chan ResourceUpdatesMessage, chan NginxConfigContextMessage) {
	t.Helper()

	fakeProcessWatcher := &processfakes.FakeProcessOperatorInterface{}
	fakeProcessWatcher.ProcessesReturns(nil, nil)

	fakeProcessParser := &instancefakes.FakeProcessParser{}
	fakeProcessParser.ParseReturns(nginxInstances)

	fakeExec := &execfakes.FakeExecInterface{}
	fakeExec.ExecutableReturns(defaultAgentPath, nil)
	fakeExec.ProcessIDReturns(123)

	resourceChannel := make(chan ResourceUpdatesMessage, 5)
	configContextChannel := make(chan NginxConfigContextMessage, 5)

	instanceWatcherService := NewInstanceWatcherService(types.AgentConfig())
	instanceWatcherService.processOperator = fakeProcessWatcher
	instanceWatcherService.nginxParser = fakeProcessParser
	instanceWatcherService.nginxConfigParser = configParser
	instanceWatcherService.executer = fakeExec
	instanceWatcherService.instancesChannel = resourceChannel
	instanceWatcherService.nginxConfigContextChannel = configContextChannel

	return instanceWatcherService, resourceChannel, configContextChannel
}

// processParserFake updates the instances returned by the process parser on the following cycles.
func (iw *InstanceWatcherService) processParserFake(instances map[string]*mpi.Instance) {
	if fake, ok := iw.nginxParser.(*instancefakes.FakeProcessParser); ok {
		fake.ParseReturns(instances)
	}
}

func processIDFor(instance *mpi.Instance) int32 {
	return instance.GetInstanceRuntime().GetProcessId()
}

// findInstance returns the instance with the given id from a resource, failing the test if it is not present.
func findInstance(t require.TestingT, resource *mpi.Resource, instanceID string) *mpi.Instance {
	for _, instance := range resource.GetInstances() {
		if instance.GetInstanceMeta().GetInstanceId() == instanceID {
			return instance
		}
	}

	require.Failf(t, "instance not found in resource", "instance_id %s", instanceID)

	return nil
}

func assertEmptyResourceChannel(t *testing.T, ch <-chan ResourceUpdatesMessage) {
	t.Helper()
	select {
	case msg := <-ch:
		assert.Failf(t, "expected no resource update", "got %v", msg.Resource)
	default:
	}
}

func assertEmptyConfigContextChannel(t *testing.T, ch <-chan NginxConfigContextMessage) {
	t.Helper()
	select {
	case msg := <-ch:
		assert.Failf(t, "expected no config context update", "got %v", msg.NginxConfigContext)
	default:
	}
}

func drainResourceChannel(ch <-chan ResourceUpdatesMessage) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func drainConfigContextChannel(ch <-chan NginxConfigContextMessage) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func TestInstanceWatcherService_areInstancesEqual(t *testing.T) {
	tests := []struct {
		oldRuntime     *mpi.InstanceRuntime
		currentRuntime *mpi.InstanceRuntime
		name           string
		expected       bool
	}{
		{
			name: "Test 1: Instances are equal",
			oldRuntime: &mpi.InstanceRuntime{
				ProcessId: 123,
				InstanceChildren: []*mpi.InstanceChild{
					{
						ProcessId: 111,
					},
					{
						ProcessId: 222,
					},
				},
			},
			currentRuntime: &mpi.InstanceRuntime{
				ProcessId: 123,
				InstanceChildren: []*mpi.InstanceChild{
					{
						ProcessId: 222,
					},
					{
						ProcessId: 111,
					},
				},
			},
			expected: true,
		},
		{
			name: "Test 2: Different PIDs",
			oldRuntime: &mpi.InstanceRuntime{
				ProcessId: 123,
				InstanceChildren: []*mpi.InstanceChild{
					{
						ProcessId: 111,
					},
				},
			},
			currentRuntime: &mpi.InstanceRuntime{
				ProcessId: 456,
				InstanceChildren: []*mpi.InstanceChild{
					{
						ProcessId: 111,
					},
				},
			},
			expected: false,
		},
		{
			name: "Test 3: Different children PIDs",
			oldRuntime: &mpi.InstanceRuntime{
				ProcessId: 123,
				InstanceChildren: []*mpi.InstanceChild{
					{
						ProcessId: 111,
					},
					{
						ProcessId: 333,
					},
				},
			},
			currentRuntime: &mpi.InstanceRuntime{
				ProcessId: 123,
				InstanceChildren: []*mpi.InstanceChild{
					{
						ProcessId: 111,
					},
					{
						ProcessId: 222,
					},
				},
			},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			assert.Equal(t, test.expected, areInstancesEqual(test.oldRuntime, test.currentRuntime))
		})
	}
}

func TestInstanceWatcherService_ReparseConfig(t *testing.T) {
	ctx := context.Background()

	nginxConfigContext := testModel.ConfigContext()
	updateNginxConfigContext := testModel.ConfigContext()
	updateNginxConfigContext.AccessLogs = []*model.AccessLog{
		{
			Name: "access2.log",
		},
	}

	instance := protos.NginxOssInstance([]string{})
	instance.GetInstanceRuntime().GetNginxRuntimeInfo().AccessLogs = []string{"access.logs"}
	instance.GetInstanceRuntime().GetNginxRuntimeInfo().ErrorLogs = []string{"error.log"}

	updatedInstance := protos.NginxOssInstance([]string{})
	updatedInstance.GetInstanceRuntime().GetNginxRuntimeInfo().AccessLogs = []string{"access2.log"}
	updatedInstance.GetInstanceRuntime().GetNginxRuntimeInfo().ErrorLogs = []string{"error.log"}

	updateNginxConfigContext.InstanceID = updatedInstance.GetInstanceMeta().GetInstanceId()

	tests := []struct {
		parseReturns *model.NginxConfigContext
		name         string
	}{
		{
			name:         "Test 1: Config Context Different",
			parseReturns: updateNginxConfigContext,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			instanceUpdatesChannel := make(chan ResourceUpdatesMessage, 1)
			nginxConfigContextChannel := make(chan NginxConfigContextMessage, 1)

			instanceWatcherService := NewInstanceWatcherService(types.AgentConfig())
			instanceWatcherService.instancesChannel = instanceUpdatesChannel
			instanceWatcherService.nginxConfigContextChannel = nginxConfigContextChannel
			instanceWatcherService.resource = &mpi.Resource{
				ResourceId: protos.HostResource().GetResourceId(),
				Instances: []*mpi.Instance{
					instance,
				},
			}

			instanceWatcherService.nginxConfigCache = map[string]*model.NginxConfigContext{
				instance.GetInstanceMeta().GetInstanceId(): nginxConfigContext,
			}

			instanceWatcherService.instanceCache = map[string]*mpi.Instance{
				instance.GetInstanceMeta().GetInstanceId(): instance,
			}

			instanceWatcherService.HandleNginxConfigContextUpdate(ctx, updatedInstance.
				GetInstanceMeta().GetInstanceId(),
				updateNginxConfigContext)

			nginxConfigContextMessage := <-nginxConfigContextChannel
			assert.Equal(t, updateNginxConfigContext, nginxConfigContextMessage.NginxConfigContext)

			assert.Equal(tt, updateNginxConfigContext, instanceWatcherService.
				nginxConfigCache[updatedInstance.GetInstanceMeta().GetInstanceId()])

			instanceUpdatesMessage := <-instanceUpdatesChannel
			assert.Len(t, instanceUpdatesMessage.Resource.GetInstances(), 1)
			assert.Equal(tt, updatedInstance.GetInstanceRuntime().GetNginxRuntimeInfo().GetAccessLogs(),
				instanceUpdatesMessage.Resource.GetInstances()[0].GetInstanceRuntime().GetNginxRuntimeInfo().
					GetAccessLogs())
		})
	}
}

func TestInstanceWatcherService_ReparseConfigs_ContinuesOnParseError(t *testing.T) {
	ctx := context.Background()

	instance1 := protos.NginxOssInstance([]string{})
	instance2 := protos.NginxPlusInstance([]string{})

	fakeParser := &configfakes.FakeConfigParser{}
	fakeParser.ParseReturns(nil, errors.New("parse error"))

	instanceWatcherService := NewInstanceWatcherService(types.AgentConfig())
	instanceWatcherService.nginxConfigParser = fakeParser
	instanceWatcherService.instancesChannel = make(chan ResourceUpdatesMessage, 2)
	instanceWatcherService.nginxConfigContextChannel = make(chan NginxConfigContextMessage, 2)
	instanceWatcherService.instanceCache = map[string]*mpi.Instance{
		instance1.GetInstanceMeta().GetInstanceId(): instance1,
		instance2.GetInstanceMeta().GetInstanceId(): instance2,
	}

	instanceWatcherService.ReparseConfigs(ctx)

	assert.Equal(t, 2, fakeParser.ParseCallCount(),
		"ReparseConfigs must attempt all instances — continue not return on parse failure")
}
