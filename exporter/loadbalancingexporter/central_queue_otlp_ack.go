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
	"google.golang.org/grpc"
)

var centralQueueAckMiddlewareID = component.MustNewIDWithName("loadbalancing_ack", "internal")

type (
	centralQueueAckContextKey struct{}
	centralQueueLogAck        struct {
		observed bool
		rejected int64
	}
)

// The standard OTLP exporter logs partial rejection and returns nil. Observe
// the actual wire response through its public middleware seam so that durable
// deletion includes a persisted terminal disposition for those records.
type centralQueueAckMiddleware struct{}

func (centralQueueAckMiddleware) Start(context.Context, component.Host) error { return nil }
func (centralQueueAckMiddleware) Shutdown(context.Context) error              { return nil }
func (centralQueueAckMiddleware) GetGRPCClientOptions(context.Context) ([]grpc.DialOption, error) {
	return []grpc.DialOption{grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if err := invoke(ctx, method, req, reply, conn, opts...); err != nil {
			return err
		}
		acknowledgement, ok := ctx.Value(centralQueueAckContextKey{}).(*centralQueueLogAck)
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
		response := plogotlp.NewExportResponse()
		if err := response.UnmarshalProto(data); err != nil {
			return err
		}
		acknowledgement.observed = true
		acknowledgement.rejected = response.PartialSuccess().RejectedLogRecords()
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

type centralQueueAckExporter struct{ exporter.Logs }

func (e centralQueueAckExporter) Start(ctx context.Context, host component.Host) error {
	extensions := maps.Clone(host.GetExtensions())
	if extensions == nil {
		extensions = make(map[component.ID]component.Component)
	}
	if _, exists := extensions[centralQueueAckMiddlewareID]; exists {
		return errors.New("reserved durable central queue middleware identity is already used")
	}
	extensions[centralQueueAckMiddlewareID] = centralQueueAckMiddleware{}
	return e.Logs.Start(ctx, centralQueueAckHost{Host: host, extensions: extensions})
}
