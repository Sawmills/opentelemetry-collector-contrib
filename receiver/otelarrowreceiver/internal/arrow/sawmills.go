package arrow // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/otelarrowreceiver/internal/arrow"

import (
	"context"
	"fmt"

	arrowpb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"go.opentelemetry.io/collector/client"
	"google.golang.org/protobuf/encoding/protojson"
)

// extractOrgID extracts org_id from context, checking both Auth attributes and Metadata.
func extractOrgID(ctx context.Context) string {
	ci := client.FromContext(ctx)
	if ci.Auth != nil {
		if orgIDAttr := ci.Auth.GetAttribute("org_id"); orgIDAttr != nil {
			if orgID, ok := orgIDAttr.(string); ok {
				return orgID
			}
		}
	}
	if orgIDs := ci.Metadata.Get("org_id"); len(orgIDs) > 0 {
		return orgIDs[0]
	}

	return ""
}

// serializeBatchRecords serializes BatchArrowRecords to JSON for logging.
func serializeBatchRecords(records *arrowpb.BatchArrowRecords) string {
	if records == nil {
		return "null"
	}
	marshaler := protojson.MarshalOptions{
		Multiline:       false,
		Indent:          "",
		EmitUnpopulated: true,
	}

	jsonBytes, err := marshaler.Marshal(records)
	if err != nil {
		return fmt.Sprintf("<error serializing batch: %v>", err)
	}

	return string(jsonBytes)
}
