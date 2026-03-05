// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opampextension

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestProcessRemoteConfig_WritesFiles(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{
				AcceptsRemoteConfig: true,
				ReportsRemoteConfig: true,
			},
			Storage: StorageConfig{Directory: storageDir},
		},
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
		opampClient:  &mockOpAMPClient{},
	}

	msg := &protobufs.AgentRemoteConfig{
		Config: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"test-config.yaml": {
					Body:        []byte("receivers:\n  otlp:\n    protocols:\n      grpc:\n"),
					ContentType: "text/yaml",
				},
			},
		},
		ConfigHash: []byte("testhash"),
	}

	agent.processRemoteConfig(msg)

	content, err := os.ReadFile(filepath.Join(storageDir, "test-config.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "receivers:\n  otlp:\n    protocols:\n      grpc:\n", string(content))

	_, err = os.Stat(filepath.Join(storageDir, lastRecvRemoteConfigFile))
	assert.NoError(t, err)
}

func TestProcessRemoteConfig_CapabilityDisabled(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{AcceptsRemoteConfig: false},
			Storage:      StorageConfig{Directory: storageDir},
		},
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{AcceptsRemoteConfig: false},
		opampClient:  &mockOpAMPClient{},
	}

	msg := &protobufs.AgentRemoteConfig{
		Config: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"test.yaml": {Body: []byte("data"), ContentType: "text/yaml"},
			},
		},
	}

	agent.processRemoteConfig(msg)

	_, err := os.Stat(filepath.Join(storageDir, "test.yaml"))
	assert.True(t, os.IsNotExist(err))
}

func TestProcessRemoteConfig_EmptyNameDefaultsToRemoteConfig(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
			Storage:      StorageConfig{Directory: storageDir},
		},
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
		opampClient:  &mockOpAMPClient{},
	}

	msg := &protobufs.AgentRemoteConfig{
		Config: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"": {Body: []byte("config-data"), ContentType: "text/yaml"},
			},
		},
		ConfigHash: []byte("hash"),
	}

	agent.processRemoteConfig(msg)

	content, err := os.ReadFile(filepath.Join(storageDir, "remote_config"))
	require.NoError(t, err)
	assert.Equal(t, "config-data", string(content))
}

func TestSaveAndLoadRemoteConfig_RoundTrip(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{AcceptsRemoteConfig: true},
			Storage:      StorageConfig{Directory: storageDir},
		},
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{AcceptsRemoteConfig: true},
		opampClient:  &mockOpAMPClient{},
	}

	original := &protobufs.AgentRemoteConfig{
		Config: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"collector.yaml": {
					Body:        []byte("receivers:\n  otlp:\n"),
					ContentType: "text/yaml",
				},
			},
		},
		ConfigHash: []byte("myhash123"),
	}

	err := agent.saveLastReceivedConfig(original)
	require.NoError(t, err)

	loaded := agent.loadRemoteConfig()
	require.NotNil(t, loaded)
	assert.Equal(t, original.ConfigHash, loaded.ConfigHash)
	require.NotNil(t, loaded.Config)
	require.Contains(t, loaded.Config.ConfigMap, "collector.yaml")
	assert.Equal(t, original.Config.ConfigMap["collector.yaml"].Body, loaded.Config.ConfigMap["collector.yaml"].Body)
}

func TestLoadRemoteConfig_NoFile(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{AcceptsRemoteConfig: true},
			Storage:      StorageConfig{Directory: storageDir},
		},
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{AcceptsRemoteConfig: true},
	}

	loaded := agent.loadRemoteConfig()
	assert.Nil(t, loaded)
}

func TestLoadRemoteConfig_CapabilityDisabled(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{AcceptsRemoteConfig: false},
			Storage:      StorageConfig{Directory: storageDir},
		},
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{AcceptsRemoteConfig: false},
	}

	loaded := agent.loadRemoteConfig()
	assert.Nil(t, loaded)
}

func TestWriteConfigFile_CreatesBackup(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		logger: zaptest.NewLogger(t),
	}

	originalContent := []byte("original content")
	err := os.WriteFile(filepath.Join(storageDir, "config.yaml"), originalContent, 0o600)
	require.NoError(t, err)

	newContent := []byte("updated content")
	changed, err := agent.writeConfigFile(storageDir, "config.yaml", newContent)
	require.NoError(t, err)
	assert.True(t, changed)

	written, err := os.ReadFile(filepath.Join(storageDir, "config.yaml"))
	require.NoError(t, err)
	assert.Equal(t, newContent, written)

	backup, err := os.ReadFile(filepath.Join(storageDir, "config.yaml.backup"))
	require.NoError(t, err)
	assert.Equal(t, originalContent, backup)
}

func TestWriteConfigFile_NoChangeOnSameContent(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		logger: zaptest.NewLogger(t),
	}

	content := []byte("same content")
	err := os.WriteFile(filepath.Join(storageDir, "config.yaml"), content, 0o600)
	require.NoError(t, err)

	changed, err := agent.writeConfigFile(storageDir, "config.yaml", content)
	require.NoError(t, err)
	assert.False(t, changed)

	_, err = os.Stat(filepath.Join(storageDir, "config.yaml.backup"))
	assert.True(t, os.IsNotExist(err))
}

func TestWriteConfigFile_PathTraversalPrevented(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		logger: zaptest.NewLogger(t),
	}

	_, err := agent.writeConfigFile(storageDir, "../escape.yaml", []byte("malicious"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "escapes storage directory")
}

func TestReportRemoteConfigStatus(t *testing.T) {
	var receivedStatus *protobufs.RemoteConfigStatus

	mockClient := &mockOpAMPClient{}
	mockClient.setRemoteConfigStatusFunc = func(rcs *protobufs.RemoteConfigStatus) error {
		receivedStatus = rcs
		return nil
	}

	agent := &opampAgent{
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{ReportsRemoteConfig: true},
		opampClient:  mockClient,
	}

	agent.reportRemoteConfigStatus(
		protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED,
		[]byte("confighash"),
		"",
	)

	require.NotNil(t, receivedStatus)
	assert.Equal(t, protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED, receivedStatus.Status)
	assert.Equal(t, []byte("confighash"), receivedStatus.LastRemoteConfigHash)
	assert.Empty(t, receivedStatus.ErrorMessage)
}

func TestReportRemoteConfigStatus_Disabled(t *testing.T) {
	mockClient := &mockOpAMPClient{}
	called := false
	mockClient.setRemoteConfigStatusFunc = func(_ *protobufs.RemoteConfigStatus) error {
		called = true
		return nil
	}

	agent := &opampAgent{
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{ReportsRemoteConfig: false},
		opampClient:  mockClient,
	}

	agent.reportRemoteConfigStatus(
		protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED,
		[]byte("hash"),
		"",
	)

	assert.False(t, called)
}

func TestOnMessage_WithRemoteConfig(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
			Storage:      StorageConfig{Directory: storageDir},
		},
		logger:                   zaptest.NewLogger(t),
		capabilities:             Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
		opampClient:              &mockOpAMPClient{},
		customCapabilityRegistry: newCustomCapabilityRegistry(zaptest.NewLogger(t), &mockOpAMPClient{}),
	}

	agent.onMessage(t.Context(), &types.MessageData{
		RemoteConfig: &protobufs.AgentRemoteConfig{
			Config: &protobufs.AgentConfigMap{
				ConfigMap: map[string]*protobufs.AgentConfigFile{
					"via-onmessage.yaml": {Body: []byte("test data"), ContentType: "text/yaml"},
				},
			},
			ConfigHash: []byte("hash"),
		},
	})

	content, err := os.ReadFile(filepath.Join(storageDir, "via-onmessage.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "test data", string(content))
}

func TestLoadAndApplyPersistedRemoteConfig(t *testing.T) {
	storageDir := t.TempDir()

	var receivedStatus *protobufs.RemoteConfigStatus
	mockClient := &mockOpAMPClient{}
	mockClient.setRemoteConfigStatusFunc = func(rcs *protobufs.RemoteConfigStatus) error {
		receivedStatus = rcs
		return nil
	}

	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
			Storage:      StorageConfig{Directory: storageDir},
		},
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
		opampClient:  mockClient,
	}

	original := &protobufs.AgentRemoteConfig{
		Config: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"test.yaml": {Body: []byte("data"), ContentType: "text/yaml"},
			},
		},
		ConfigHash: []byte("persistedhash"),
	}
	err := agent.saveLastReceivedConfig(original)
	require.NoError(t, err)

	agent.loadAndApplyPersistedRemoteConfig()

	agent.remoteConfigMu.RLock()
	defer agent.remoteConfigMu.RUnlock()
	require.NotNil(t, agent.remoteConfig)
	assert.Equal(t, []byte("persistedhash"), agent.remoteConfig.ConfigHash)

	require.NotNil(t, receivedStatus)
	assert.Equal(t, protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED, receivedStatus.Status)
}

func TestProcessRemoteConfig_MultipleFiles(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
			Storage:      StorageConfig{Directory: storageDir},
		},
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
		opampClient:  &mockOpAMPClient{},
	}

	msg := &protobufs.AgentRemoteConfig{
		Config: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"config1.yaml": {Body: []byte("config one"), ContentType: "text/yaml"},
				"config2.yaml": {Body: []byte("config two"), ContentType: "text/yaml"},
			},
		},
		ConfigHash: []byte("multihash"),
	}

	agent.processRemoteConfig(msg)

	content1, err := os.ReadFile(filepath.Join(storageDir, "config1.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "config one", string(content1))

	content2, err := os.ReadFile(filepath.Join(storageDir, "config2.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "config two", string(content2))
}

func TestProcessRemoteConfig_ReportsFailedOnError(t *testing.T) {
	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
			Storage:      StorageConfig{Directory: "/nonexistent/path/that/should/fail"},
		},
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
		opampClient: &mockOpAMPClient{
			setRemoteConfigStatusFunc: func(rcs *protobufs.RemoteConfigStatus) error {
				assert.Equal(t, protobufs.RemoteConfigStatuses_RemoteConfigStatuses_FAILED, rcs.Status)
				assert.NotEmpty(t, rcs.ErrorMessage)
				return nil
			},
		},
	}

	msg := &protobufs.AgentRemoteConfig{
		Config: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"test.yaml": {Body: []byte("data"), ContentType: "text/yaml"},
			},
		},
		ConfigHash: []byte("failhash"),
	}

	agent.processRemoteConfig(msg)
}

func TestProcessRemoteConfig_ConcurrentAccess(t *testing.T) {
	storageDir := t.TempDir()

	agent := &opampAgent{
		cfg: &Config{
			Capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
			Storage:      StorageConfig{Directory: storageDir},
		},
		logger:       zaptest.NewLogger(t),
		capabilities: Capabilities{AcceptsRemoteConfig: true, ReportsRemoteConfig: true},
		opampClient:  &mockOpAMPClient{},
	}

	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msg := &protobufs.AgentRemoteConfig{
				Config: &protobufs.AgentConfigMap{
					ConfigMap: map[string]*protobufs.AgentConfigFile{
						"concurrent.yaml": {
							Body:        []byte("data from goroutine"),
							ContentType: "text/yaml",
						},
					},
				},
				ConfigHash: []byte("hash"),
			}
			agent.processRemoteConfig(msg)
		}(i)
	}
	wg.Wait()

	content, err := os.ReadFile(filepath.Join(storageDir, "concurrent.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "data from goroutine", string(content))
}
