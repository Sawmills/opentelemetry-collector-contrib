// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package upload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPreparedUploadCleansTimedOutPartsBeforeRetry(t *testing.T) {
	var creates, aborts, parts atomic.Int32
	var cleanupFails atomic.Bool
	cleanupFails.Store(true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		switch {
		case r.Method == http.MethodDelete:
			aborts.Add(1)
			if cleanupFails.Load() {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Query().Has("uploads"):
			id := creates.Add(1)
			_, _ = fmt.Fprintf(w, "<InitiateMultipartUploadResult><UploadId>upload-%d</UploadId></InitiateMultipartUploadResult>", id)
		case r.Method == http.MethodPut:
			parts.Add(1)
			if creates.Load() == 1 && r.URL.Query().Get("partNumber") == "2" {
				cancel()
				<-r.Context().Done()
				return
			}
			w.Header().Set("ETag", "\"part\"")
		default:
			_, _ = io.WriteString(w, "<CompleteMultipartUploadResult><ETag>\"object\"</ETag></CompleteMultipartUploadResult>")
		}
	}))
	defer server.Close()
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), Retryer: aws.NopRetryer{}})
	manager := NewS3Manager(zap.NewNop(), "archives", &PartitionKeyBuilder{}, client, "STANDARD").(*s3manager)
	manager.uploader = transfermanager.New(client, func(o *transfermanager.Options) {
		o.Concurrency = 1
		o.PartSizeBytes = 5 * 1024 * 1024
		o.MultipartUploadThreshold = 5 * 1024 * 1024
	})
	prepared := manager.Prepare(t.Context(), bytes.Repeat([]byte("x"), 6*1024*1024), nil)
	_, err := prepared.Upload(ctx)
	require.Error(t, err)
	require.GreaterOrEqual(t, parts.Load(), int32(2))
	require.GreaterOrEqual(t, aborts.Load(), int32(1), "cleanup must use a live context after the upload is canceled")
	retryCtx, retryCancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer retryCancel()
	_, err = prepared.Upload(retryCtx)
	require.Error(t, err, "failed cleanup must prevent a new multipart upload")
	require.Equal(t, int32(1), creates.Load())
	cleanupFails.Store(false)
	_, err = prepared.Upload(retryCtx)
	require.NoError(t, err)
	require.Equal(t, int32(2), creates.Load())
}
