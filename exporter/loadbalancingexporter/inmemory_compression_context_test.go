// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configauth"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcmetadata "google.golang.org/grpc/metadata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func setQueueContextGate(t *testing.T, enabled bool) {
	t.Helper()
	found := false
	previous := false
	featuregate.GlobalRegistry().VisitAll(func(g *featuregate.Gate) {
		if g.ID() == "exporter.PersistRequestContext" {
			found = true
			previous = g.IsEnabled()
		}
	})
	require.True(t, found, "the pinned helper must register the context-persistence gate")
	require.NoError(t, featuregate.GlobalRegistry().Set("exporter.PersistRequestContext", enabled))
	t.Cleanup(func() {
		require.NoError(t, featuregate.GlobalRegistry().Set("exporter.PersistRequestContext", previous))
	})
}

func TestInMemoryCompressionRequiresContextPersistence(t *testing.T) {
	setQueueContextGate(t, false)
	for _, signal := range []string{"logs", "metrics", "traces"} {
		t.Run(signal, func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig().(*Config)
			cfg.Resolver = ResolverSettings{Static: configoptional.Some(StaticResolver{Hostnames: []string{"127.0.0.1:4317"}})}
			cfg.QueueSettings.QueueConfig = configoptional.Some(exporterhelper.NewDefaultQueueConfig())
			cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionSnappy
			cfg.QueueSettings.CompressInMemory = true
			require.NoError(t, cfg.Validate())
			settings := exportertest.NewNopSettings(metadata.Type)
			var exp component.Component
			var err error
			switch signal {
			case "logs":
				exp, err = factory.CreateLogs(t.Context(), settings, cfg)
			case "metrics":
				exp, err = factory.CreateMetrics(t.Context(), settings, cfg)
			case "traces":
				exp, err = factory.CreateTraces(t.Context(), settings, cfg)
			}
			if exp != nil {
				t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })
			}
			require.ErrorContains(t, err, "sending_queue.compress_in_memory requires the exporter.PersistRequestContext feature gate to be enabled")
			require.Nil(t, exp)
		})
	}
}

func TestInMemoryCompressionPreservesClientMetadata(t *testing.T) {
	for _, test := range []struct {
		name     string
		gate     bool
		compress bool
	}{
		{name: "raw", gate: true},
		{name: "compressed", gate: true, compress: true},
		{name: "raw_gate_disabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			setQueueContextGate(t, test.gate)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			peer := &queueContextPeer{headers: make(chan []string, 1)}
			server := grpc.NewServer()
			plogotlp.RegisterGRPCServer(server, peer)
			go func() { _ = server.Serve(listener) }()
			defer server.Stop()
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig().(*Config)
			cfg.Resolver = ResolverSettings{Static: configoptional.Some(StaticResolver{Hostnames: []string{listener.Addr().String()}})}
			cfg.Protocol.OTLP.ClientConfig.TLS.Insecure = true
			authID := component.MustNewID("context_header")
			cfg.Protocol.OTLP.ClientConfig.Auth = configoptional.Some(configauth.Config{AuthenticatorID: authID})
			cfg.Protocol.OTLP.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
			cfg.Protocol.OTLP.RetryConfig.Enabled = false
			cfg.CentralQueue.Enabled = false
			queue := exporterhelper.NewDefaultQueueConfig()
			queue.NumConsumers = 1
			queue.QueueSize = 10
			cfg.QueueSettings.QueueConfig = configoptional.Some(queue)
			cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionSnappy
			cfg.QueueSettings.CompressInMemory = test.compress
			require.NoError(t, cfg.Validate())
			exp, err := factory.CreateLogs(ctx, exportertest.NewNopSettings(metadata.Type), cfg)
			require.NoError(t, err)
			require.NoError(t, exp.Start(ctx, queueContextHost{authID: &queueContextAuth{}}))
			defer func() {
				shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				require.NoError(t, exp.Shutdown(shutdown))
			}()
			requestCtx := client.NewContext(ctx, client.Info{Metadata: client.NewMetadata(map[string][]string{"tenant": {"synthetic-tenant"}})})
			require.NoError(t, exp.ConsumeLogs(requestCtx, queueCompressionLogs(42)))
			select {
			case headers := <-peer.headers:
				require.Equal(t, []string{"synthetic-tenant"}, headers, "queue encoding must preserve the client metadata used by downstream authentication")
			case <-ctx.Done():
				t.Fatal("the admitted request did not reach the backend")
			}
		})
	}
}

type queueContextPeer struct {
	plogotlp.UnimplementedGRPCServer
	headers chan []string
}

func (p *queueContextPeer) Export(ctx context.Context, _ plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	md, _ := grpcmetadata.FromIncomingContext(ctx)
	select {
	case p.headers <- md.Get("x-request-tenant"):
	case <-ctx.Done():
		return plogotlp.NewExportResponse(), ctx.Err()
	}
	return plogotlp.NewExportResponse(), nil
}

type queueContextHost map[component.ID]component.Component

func (h queueContextHost) GetExtensions() map[component.ID]component.Component { return h }

type queueContextAuth struct {
	component.StartFunc
	component.ShutdownFunc
}

func (*queueContextAuth) PerRPCCredentials() (credentials.PerRPCCredentials, error) {
	return queueContextCredentials{}, nil
}

type queueContextCredentials struct{}

func (queueContextCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	values := client.FromContext(ctx).Metadata.Get("tenant")
	if len(values) == 0 {
		return nil, nil
	}
	return map[string]string{"x-request-tenant": values[0]}, nil
}

func (queueContextCredentials) RequireTransportSecurity() bool { return false }
