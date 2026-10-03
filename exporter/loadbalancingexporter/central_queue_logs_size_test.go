// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestCentralQueueLaneSizeMatchesSerializedLogs(t *testing.T) {
	for _, metadata := range []bool{false, true} {
		t.Run(fmt.Sprintf("metadata=%t", metadata), func(t *testing.T) {
			logs := plog.NewLogs()
			for resource := range 3 {
				rl := logs.ResourceLogs().AppendEmpty()
				if metadata {
					rl.Resource().Attributes().PutInt("resource", int64(resource%2))
					rl.Resource().SetDroppedAttributesCount(3)
					rl.SetSchemaUrl("resource-schema")
				}
				for scope := range 3 {
					sl := rl.ScopeLogs().AppendEmpty()
					if metadata {
						sl.Scope().SetName(fmt.Sprint(scope % 2))
						sl.Scope().Attributes().PutStr("scope", strings.Repeat("s", 120))
						sl.Scope().SetDroppedAttributesCount(2)
						sl.SetSchemaUrl("scope-schema")
					}
					for _, size := range []int{0, 1, 120, 127, 128, 16370, 16383, 16384} {
						record := sl.LogRecords().AppendEmpty()
						record.Body().SetStr(strings.Repeat("x", size))
					}
				}
			}
			marshaler := &plog.ProtoMarshaler{}
			before, err := marshaler.MarshalLogs(logs)
			require.NoError(t, err)
			lane := newCentralQueueLogLaneBuilder([]byte("lane"))
			for range 2 {
				for i := range logs.ResourceLogs().Len() {
					rl := logs.ResourceLogs().At(i)
					for j := range rl.ScopeLogs().Len() {
						sl := rl.ScopeLogs().At(j)
						for k := range sl.LogRecords().Len() {
							record := sl.LogRecords().At(k)
							expected := plog.NewLogs()
							lane.logs.CopyTo(expected)
							insertLogRecord(expected, rl, sl, record)
							wire, marshalErr := marshaler.MarshalLogs(expected)
							require.NoError(t, marshalErr)
							require.True(t, lane.canFit(rl, sl, record, marshaler, len(wire)))
							require.False(t, lane.canFit(rl, sl, record, marshaler, len(wire)-1))
							lane.appendRecord(rl, sl, record, marshaler)
							require.Equal(t, len(wire), lane.size.bytes)
						}
					}
				}
				lane.reset()
				require.Zero(t, lane.size.bytes)
			}
			after, err := marshaler.MarshalLogs(logs)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func BenchmarkCentralQueueLaneSizeUpdate(b *testing.B) {
	for _, records := range []int{32, 256} {
		b.Run(fmt.Sprint(records), func(b *testing.B) {
			logs := sharedScopeLogsWithoutTraceIDs(repeatedStrings(records, strings.Repeat("x", 512))...)
			rl := logs.ResourceLogs().At(0)
			sl := rl.ScopeLogs().At(0)
			marshaler := &plog.ProtoMarshaler{}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				state := centralQueueLogSizeState{}
				for i := range sl.LogRecords().Len() {
					state.addRecord(rl, sl, sl.LogRecords().At(i), marshaler)
				}
				if state.bytes == 0 {
					b.Fatal("missing size")
				}
			}
		})
	}
}
