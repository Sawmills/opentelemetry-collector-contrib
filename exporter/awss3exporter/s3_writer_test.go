// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.uber.org/zap"
)

type testUploaderClient struct {
	putObjectInput    testPutObjectInput
	staticCredentials testStaticCredentials
}

type testPutObjectInput struct {
	ServerSideEncryption string
}

type testStaticCredentials struct {
	AccessKeyID string
}

func newTestClientFromConfig(t *testing.T, conf Config) testUploaderClient {
	t.Helper()

	t.Setenv("AWS_ACCESS_KEY_ID", "env-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret-key")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	var captured testUploaderClient
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()

		captured.putObjectInput.ServerSideEncryption = r.Header.Get("x-amz-server-side-encryption")
		captured.staticCredentials.AccessKeyID = credentialAccessKey(r.Header.Get("Authorization"))

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	conf.S3Uploader.Endpoint = srv.URL
	conf.S3Uploader.Region = "us-east-1"
	conf.S3Uploader.S3Bucket = "test-bucket"
	conf.S3Uploader.S3ForcePathStyle = true

	manager, err := newUploadManager(
		t.Context(),
		&conf,
		zap.NewNop(),
		"logs",
		"otlp",
		false,
	)
	if !assert.NoError(t, err) {
		return captured
	}

	_, err = manager.Upload(t.Context(), []byte("payload"), nil)
	if !assert.NoError(t, err) {
		return captured
	}

	return captured
}

func credentialAccessKey(authHeader string) string {
	const credentialPrefix = "Credential" + "="

	start := strings.Index(authHeader, credentialPrefix)
	if start < 0 {
		return ""
	}

	credential := authHeader[start+len(credentialPrefix):]
	if end := strings.Index(credential, "/"); end >= 0 {
		return credential[:end]
	}

	return credential
}

func TestUploaderOptions_LegacyAuthAndSSE(t *testing.T) {
	client := newTestClientFromConfig(t, Config{
		S3Uploader: S3UploaderConfig{
			AccessKeyID:          "key",
			SecretAccessKey:      "secret",
			ServerSideEncryption: "AES256",
		},
	})

	assert.Equal(t, "AES256", client.putObjectInput.ServerSideEncryption)
	assert.Equal(t, "key", client.staticCredentials.AccessKeyID)
}

func TestUploaderOptions_StaticCredsAndRoleArnUsesAssumeRole(t *testing.T) {
	assumedAccessKey := "assumed-access-key"
	var stsCalls atomic.Int32
	s3AuthorizationCh := make(chan string, 1)

	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	stsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()

		stsCalls.Add(1)
		assert.Equal(t, "POST", r.Method)

		w.Header().Set("Content-Type", "text/xml")
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>%s</AccessKeyId>
      <SecretAccessKey>assumed-secret</SecretAccessKey>
      <SessionToken>assumed-token</SessionToken>
      <Expiration>%s</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/test/session</Arn>
      <AssumedRoleId>ID:session</AssumedRoleId>
    </AssumedRoleUser>
  </AssumeRoleResult>
  <ResponseMetadata>
    <RequestId>request-id</RequestId>
  </ResponseMetadata>
</AssumeRoleResponse>`,
			assumedAccessKey,
			time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		)
	}))
	t.Cleanup(stsSrv.Close)

	s3Srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()

		select {
		case s3AuthorizationCh <- r.Header.Get("Authorization"):
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s3Srv.Close)

	t.Setenv("AWS_ENDPOINT_URL", stsSrv.URL)

	conf := Config{
		S3Uploader: S3UploaderConfig{
			Region:               "us-east-1",
			S3Bucket:             "test-bucket",
			S3ForcePathStyle:     true,
			Endpoint:             s3Srv.URL,
			AccessKeyID:          "static-key",
			SecretAccessKey:      "static-secret",
			RoleArn:              "arn:aws:iam::123456789012:role/test",
			ServerSideEncryption: "AES256",
		},
	}

	manager, err := newUploadManager(
		t.Context(),
		&conf,
		zap.NewNop(),
		"logs",
		"otlp",
		false,
	)
	if !assert.NoError(t, err) {
		return
	}

	_, err = manager.Upload(t.Context(), []byte("payload"), nil)
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, int32(1), stsCalls.Load())
	var s3Authorization string
	select {
	case s3Authorization = <-s3AuthorizationCh:
	default:
		t.Fatal("expected S3 authorization header")
	}
	assert.Contains(t, s3Authorization, "Credential="+assumedAccessKey+"/")
}

func TestNewUploadManager(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		conf   *Config
		errVal string
	}{
		{
			name: "valid configuration",
			conf: &Config{
				S3Uploader: S3UploaderConfig{
					Region:              "local",
					S3Bucket:            "my-awesome-bucket",
					S3Prefix:            "opentelemetry",
					S3PartitionFormat:   "year=%Y/month=%m/day=%d/hour=%H",
					S3PartitionTimezone: "Europe/London",
					FilePrefix:          "ingested-data-",
					Endpoint:            "localhost",
					RoleArn:             "arn:aws:iam::123456789012:my-awesome-user",
					S3ForcePathStyle:    true,
					DisableSSL:          true,
					Compression:         configcompression.TypeGzip,
				},
			},
			errVal: "",
		},
		{
			name: "invalid timezone configuration",
			conf: &Config{
				S3Uploader: S3UploaderConfig{
					Region:              "local",
					S3Bucket:            "my-awesome-bucket",
					S3Prefix:            "opentelemetry",
					S3PartitionFormat:   "year=%Y/month=%m/day=%d/hour=%H",
					S3PartitionTimezone: "non-existing timezone",
					FilePrefix:          "ingested-data-",
					Endpoint:            "localhost",
					RoleArn:             "arn:aws:iam::123456789012:my-awesome-user",
					S3ForcePathStyle:    true,
					DisableSSL:          true,
					Compression:         configcompression.TypeGzip,
				},
			},
			errVal: "invalid S3 partition timezone: unknown time zone non-existing timezone",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sm, err := newUploadManager(
				t.Context(),
				tc.conf,
				zap.NewNop(),
				"metrics",
				"otlp",
				false,
			)

			if tc.errVal != "" {
				assert.Nil(t, sm, "Must not have a valid s3 upload manager")
				assert.EqualError(t, err, tc.errVal, "Must match the expected error")
			} else {
				assert.NotNil(t, sm, "Must have a valid manager")
				assert.NoError(t, err, "Must not error when creating client")
			}
		})
	}
}
