// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hotreloadprocessor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFileWatcher(t *testing.T) {
	dir := t.TempDir()

	done := make(chan bool)
	watchedFiles := []string{}
	watcher := newFileWatcher(zap.NewNop(), dir, func(filePath string) error {
		watchedFiles = append(watchedFiles, filePath)
		done <- true
		return nil
	})

	err := watcher.Start(t.Context())
	require.NoError(t, err)

	filePath := filepath.Join(dir, "config.yaml")
	err = os.WriteFile(filePath, []byte("test"), 0o600)
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout: file not watched")
	}

	err = watcher.Stop(t.Context())
	require.NoError(t, err)

	require.Equal(t, []string{filePath}, watchedFiles)
}
