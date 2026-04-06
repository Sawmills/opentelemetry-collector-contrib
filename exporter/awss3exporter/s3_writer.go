// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter"

import (
	"context"
	"errors"
	"fmt"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter/internal/upload"
)

func newUploadManager(
	ctx context.Context,
	conf *Config,
	logger *zap.Logger,
	metadata string,
	format string,
	isCompressed bool,
) (upload.Manager, error) {
	configOpts := []func(*config.LoadOptions) error{}

	if region := conf.S3Uploader.Region; region != "" {
		configOpts = append(configOpts, config.WithRegion(region))
	}

	if (conf.S3Uploader.AccessKeyID == "") != (conf.S3Uploader.SecretAccessKey == "") {
		return nil, errors.New("access_key_id and secret_access_key must be set together")
	}

	if conf.S3Uploader.AccessKeyID != "" && conf.S3Uploader.SecretAccessKey != "" {
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				conf.S3Uploader.AccessKeyID,
				conf.S3Uploader.SecretAccessKey,
				"",
			),
		))
	}

	switch conf.S3Uploader.RetryMode {
	case "nop":
		configOpts = append(configOpts, config.WithRetryer(func() aws.Retryer {
			return aws.NopRetryer{}
		}))
	default:
		configOpts = append(configOpts, config.WithRetryMode(aws.RetryMode(conf.S3Uploader.RetryMode)))
	}

	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, err
	}

	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.EndpointOptions = s3.EndpointResolverOptions{
				DisableHTTPS: conf.S3Uploader.DisableSSL,
			}
			o.UsePathStyle = conf.S3Uploader.S3ForcePathStyle
			o.Retryer = retry.AddWithMaxAttempts(o.Retryer, conf.S3Uploader.RetryMaxAttempts)
			o.Retryer = retry.AddWithMaxBackoffDelay(o.Retryer, conf.S3Uploader.RetryMaxBackoff)
		},
	}

	if conf.S3Uploader.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(conf.S3Uploader.Endpoint)
		})
	}

	if conf.S3Uploader.ServerSideEncryption != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, serverSideEncryptionAPIOption(conf.S3Uploader.ServerSideEncryption))
		})
	}

	if arn := conf.S3Uploader.RoleArn; arn != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.Credentials = stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), arn)
		})
	}

	if endpoint := conf.S3Uploader.Endpoint; endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	var managerOpts []upload.ManagerOpt
	if conf.S3Uploader.ACL != "" {
		managerOpts = append(managerOpts,
			upload.WithACL(s3types.ObjectCannedACL(conf.S3Uploader.ACL)))
	}

	var uniqueKeyFunc func() string
	switch conf.S3Uploader.UniqueKeyFuncName {
	case "uuidv7":
		uniqueKeyFunc = upload.GenerateUUIDv7
	default:
		uniqueKeyFunc = nil
	}

	var s3PartitionTimeLocation *time.Location
	if conf.S3Uploader.S3PartitionTimezone != "" {
		s3PartitionTimeLocation, err = time.LoadLocation(conf.S3Uploader.S3PartitionTimezone)
		if err != nil {
			return nil, fmt.Errorf("invalid S3 partition timezone: %w", err)
		}
	} else {
		s3PartitionTimeLocation = time.Local
	}

	var parsedLegacyTemplate *template.Template
	if conf.S3Uploader.S3KeyTemplate != "" {
		parsedLegacyTemplate, err = upload.ParseLegacyTemplateForValidation(conf.S3Uploader.S3KeyTemplate)
		if err != nil {
			return nil, fmt.Errorf("parse legacy s3 key template: %w", err)
		}
	}

	return upload.NewS3Manager(
		logger,
		conf.S3Uploader.S3Bucket,
		&upload.PartitionKeyBuilder{
			PartitionBasePrefix:       conf.S3Uploader.S3BasePrefix,
			PartitionPrefix:           conf.S3Uploader.S3Prefix,
			LegacyS3KeyTemplate:       conf.S3Uploader.S3KeyTemplate,
			LegacyS3KeyTemplateParsed: parsedLegacyTemplate,
			PartitionFormat:           conf.S3Uploader.S3PartitionFormat,
			PartitionTimeLocation:     s3PartitionTimeLocation,
			FilePrefix:                conf.S3Uploader.FilePrefix,
			FileFormat:                format,
			Metadata:                  metadata,
			Compression:               conf.S3Uploader.Compression,
			UniqueKeyFunc:             uniqueKeyFunc,
			IsCompressed:              isCompressed,
		},
		s3.NewFromConfig(cfg, s3Opts...),
		s3types.StorageClass(conf.S3Uploader.StorageClass),
		managerOpts...,
	), nil
}

func serverSideEncryptionAPIOption(value string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc(
			"awss3exporterServerSideEncryption",
			func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				applyServerSideEncryption(in.Parameters, value)
				return next.HandleInitialize(ctx, in)
			},
		), middleware.After)
	}
}

func applyServerSideEncryption(parameters any, value string) {
	if value == "" || parameters == nil {
		return
	}

	switch input := parameters.(type) {
	case *s3.PutObjectInput:
		input.ServerSideEncryption = s3types.ServerSideEncryption(value)
	case *s3.CreateMultipartUploadInput:
		input.ServerSideEncryption = s3types.ServerSideEncryption(value)
	}
}
