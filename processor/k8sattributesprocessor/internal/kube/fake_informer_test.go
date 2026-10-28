// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kube_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/k8sattributesprocessor/internal/kube"
)

func TestFakeInformersAreAlreadySynced(t *testing.T) {
	for name, informer := range map[string]cache.SharedInformer{
		"pod":         kube.NewFakeInformer(nil, "", labels.Everything(), fields.Everything()),
		"namespace":   kube.NewFakeNamespaceInformer(nil),
		"replica_set": kube.NewFakeReplicaSetInformer(nil, ""),
		"noop":        kube.NewNoOpInformer(nil),
	} {
		t.Run(name, func(t *testing.T) {
			require.True(t, informer.HasSynced())
			assertAlreadySynced(t, informer.HasSyncedChecker())
			require.True(t, informer.GetController().HasSynced())
			assertAlreadySynced(t, informer.GetController().HasSyncedChecker())

			handler, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{})
			require.NoError(t, err)
			require.True(t, handler.HasSynced())
			assertAlreadySynced(t, handler.HasSyncedChecker())
		})
	}
}

func assertAlreadySynced(t *testing.T, checker cache.DoneChecker) {
	t.Helper()
	require.NotEmpty(t, checker.Name())
	for range 2 {
		select {
		case <-checker.Done():
		default:
			t.Fatal("an always-synced fake must return an already-closed Done channel")
		}
	}
}
