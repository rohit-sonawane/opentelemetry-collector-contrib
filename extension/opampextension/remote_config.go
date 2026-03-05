// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opampextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/opampextension"

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/open-telemetry/opamp-go/protobufs"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const lastRecvRemoteConfigFile = "last_recv_remote_config.dat"

func (o *opampAgent) processRemoteConfig(msg *protobufs.AgentRemoteConfig) {
	if !o.capabilities.AcceptsRemoteConfig {
		o.logger.Warn("Received remote config but AcceptsRemoteConfig capability is disabled, ignoring")
		return
	}

	if msg == nil || msg.Config == nil || msg.Config.ConfigMap == nil {
		o.logger.Warn("Received empty remote config message")
		return
	}

	if err := o.saveLastReceivedConfig(msg); err != nil {
		o.logger.Error("Failed to persist remote config", zap.Error(err))
	}

	o.remoteConfigMu.Lock()
	o.remoteConfig = msg
	o.remoteConfigMu.Unlock()

	changed, err := o.applyRemoteConfigFiles(msg)
	if err != nil {
		o.logger.Error("Failed to apply remote config files", zap.Error(err))
		o.reportRemoteConfigStatus(
			protobufs.RemoteConfigStatuses_RemoteConfigStatuses_FAILED,
			msg.GetConfigHash(),
			err.Error(),
		)
		return
	}

	if changed {
		o.logger.Info("Remote config applied successfully")
	} else {
		o.logger.Debug("Remote config unchanged")
	}
	o.reportRemoteConfigStatus(
		protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED,
		msg.GetConfigHash(),
		"",
	)
}

func (o *opampAgent) applyRemoteConfigFiles(msg *protobufs.AgentRemoteConfig) (bool, error) {
	storageDir := o.cfg.Storage.Directory
	changed := false
	var errs error

	for name, configFile := range msg.Config.ConfigMap {
		if name == "" {
			name = "remote_config"
		}

		fileChanged, err := o.writeConfigFile(storageDir, name, configFile.GetBody())
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to write config file %q: %w", name, err))
			continue
		}
		if fileChanged {
			changed = true
		}
	}

	return changed, errs
}

func (o *opampAgent) writeConfigFile(storageDir, name string, body []byte) (bool, error) {
	targetPath := filepath.Join(storageDir, filepath.Clean(name))

	resolved, err := filepath.Abs(targetPath)
	if err != nil {
		return false, fmt.Errorf("failed to resolve path: %w", err)
	}
	absStorageDir, err := filepath.Abs(storageDir)
	if err != nil {
		return false, fmt.Errorf("failed to resolve storage directory: %w", err)
	}
	if !strings.HasPrefix(resolved, absStorageDir+string(filepath.Separator)) && resolved != absStorageDir {
		return false, fmt.Errorf("path %q escapes storage directory", name)
	}

	dir := filepath.Dir(resolved)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, fmt.Errorf("failed to create directory %q: %w", dir, err)
	}

	existing, readErr := os.ReadFile(resolved)
	if readErr == nil {
		existingHash := sha256.Sum256(existing)
		newHash := sha256.Sum256(body)
		if bytes.Equal(existingHash[:], newHash[:]) {
			return false, nil
		}

		backupPath := resolved + ".backup"
		if err := os.WriteFile(backupPath, existing, 0o600); err != nil {
			o.logger.Warn("Failed to create backup file", zap.String("path", backupPath), zap.Error(err))
		}
	}

	if err := os.WriteFile(resolved, body, 0o600); err != nil {
		return false, fmt.Errorf("failed to write file %q: %w", resolved, err)
	}

	o.logger.Info("Wrote remote config file", zap.String("path", resolved), zap.Int("size", len(body)))
	return true, nil
}

func (o *opampAgent) saveLastReceivedConfig(config *protobufs.AgentRemoteConfig) error {
	cfg, err := proto.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal remote config: %w", err)
	}

	filePath := filepath.Join(o.cfg.Storage.Directory, lastRecvRemoteConfigFile)
	if err := os.MkdirAll(o.cfg.Storage.Directory, 0o700); err != nil {
		return fmt.Errorf("failed to create storage directory: %w", err)
	}

	return os.WriteFile(filePath, cfg, 0o600)
}

func (o *opampAgent) loadRemoteConfig() *protobufs.AgentRemoteConfig {
	if !o.capabilities.AcceptsRemoteConfig {
		return nil
	}

	if o.cfg.Storage.Directory == "" {
		return nil
	}

	filePath := filepath.Join(o.cfg.Storage.Directory, lastRecvRemoteConfigFile)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			o.logger.Error("Failed to read persisted remote config", zap.String("path", filePath), zap.Error(err))
		}
		return nil
	}

	config := &protobufs.AgentRemoteConfig{}
	if err := proto.Unmarshal(data, config); err != nil {
		o.logger.Error("Failed to unmarshal persisted remote config", zap.Error(err))
		return nil
	}

	o.logger.Info("Loaded persisted remote config", zap.String("path", filePath))
	return config
}

func (o *opampAgent) loadAndApplyPersistedRemoteConfig() {
	config := o.loadRemoteConfig()
	if config == nil {
		return
	}

	o.remoteConfigMu.Lock()
	o.remoteConfig = config
	o.remoteConfigMu.Unlock()

	o.reportRemoteConfigStatus(
		protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED,
		config.GetConfigHash(),
		"",
	)
}

func (o *opampAgent) reportRemoteConfigStatus(status protobufs.RemoteConfigStatuses, configHash []byte, errMsg string) {
	if !o.capabilities.ReportsRemoteConfig {
		return
	}

	rcs := &protobufs.RemoteConfigStatus{
		LastRemoteConfigHash: configHash,
		Status:               status,
		ErrorMessage:         errMsg,
	}

	if err := o.opampClient.SetRemoteConfigStatus(rcs); err != nil {
		o.logger.Error("Failed to report remote config status", zap.Error(err))
	}
}
