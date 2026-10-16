// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"maps"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
)

var centralQueueAckMiddlewareID = component.MustNewIDWithName("loadbalancing_ack", "internal")

type (
	centralQueueAckContextKey struct{}
	centralQueueOTLPAck       struct {
		observed bool
		rejected int64
	}
)

// The standard OTLP exporter logs partial rejection and returns nil. Observe
// the actual wire response through its public middleware seam so that durable
// deletion includes a persisted terminal disposition for those records.
type centralQueueAckMiddleware struct{ signal signalKind }

func (centralQueueAckMiddleware) Start(context.Context, component.Host) error { return nil }
func (centralQueueAckMiddleware) Shutdown(context.Context) error              { return nil }
func (m centralQueueAckMiddleware) GetGRPCClientOptions(context.Context) ([]grpc.DialOption, error) {
	return []grpc.DialOption{grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if err := invoke(ctx, method, req, reply, conn, opts...); err != nil {
			return err
		}
		acknowledgement, ok := ctx.Value(centralQueueAckContextKey{}).(*centralQueueOTLPAck)
		if !ok {
			return errors.New("durable central queue response has no acknowledgement owner")
		}
		// pdata deliberately hides the generated wire type. Its public proto
		// methods let us decode through the supported plogotlp response API.
		wire, ok := reply.(interface {
			SizeProto() int
			MarshalProto([]byte) int
		})
		if !ok {
			return errors.New("unsupported durable central queue OTLP response")
		}
		data := make([]byte, wire.SizeProto())
		wire.MarshalProto(data)
		switch m.signal {
		case signalKindLogs:
			response := plogotlp.NewExportResponse()
			if err := response.UnmarshalProto(data); err != nil {
				return err
			}
			acknowledgement.rejected = response.PartialSuccess().RejectedLogRecords()
		case signalKindMetrics:
			response := pmetricotlp.NewExportResponse()
			if err := response.UnmarshalProto(data); err != nil {
				return err
			}
			acknowledgement.rejected = response.PartialSuccess().RejectedDataPoints()
		default:
			return errors.New("unsupported durable central queue acknowledgement signal")
		}
		acknowledgement.observed = true
		return nil
	})}, nil
}

type centralQueueAckHost struct {
	component.Host
	extensions map[component.ID]component.Component
}

func (h centralQueueAckHost) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

type centralQueueAckLogsExporter struct{ exporter.Logs }

func (e centralQueueAckLogsExporter) Start(ctx context.Context, host component.Host) error {
	wrapped, err := centralQueueAckHostForSignal(host, signalKindLogs)
	if err != nil {
		return err
	}
	return e.Logs.Start(ctx, wrapped)
}

type centralQueueAckMetricsExporter struct{ exporter.Metrics }

func (e centralQueueAckMetricsExporter) Start(ctx context.Context, host component.Host) error {
	wrapped, err := centralQueueAckHostForSignal(host, signalKindMetrics)
	if err != nil {
		return err
	}
	return e.Metrics.Start(ctx, wrapped)
}

func centralQueueAckHostForSignal(host component.Host, signal signalKind) (component.Host, error) {
	extensions := maps.Clone(host.GetExtensions())
	if extensions == nil {
		extensions = make(map[component.ID]component.Component)
	}
	if _, exists := extensions[centralQueueAckMiddlewareID]; exists {
		return nil, errors.New("reserved durable central queue middleware identity is already used")
	}
	extensions[centralQueueAckMiddlewareID] = centralQueueAckMiddleware{signal: signal}
	return centralQueueAckHost{Host: host, extensions: extensions}, nil
}
