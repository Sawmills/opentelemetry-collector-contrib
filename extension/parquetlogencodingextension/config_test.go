package parquetlogencodingextension

import "testing"

func TestConfigValidateCompressionCodec(t *testing.T) {
	tests := []struct {
		name        string
		codec       string
		shouldError bool
	}{
		{name: "snappy", codec: "snappy"},
		{name: "zstd", codec: "zstd"},
		{name: "gzip", codec: "gzip"},
		{name: "uncompressed", codec: "uncompressed"},
		{name: "upper_case", codec: "SNAPPY"},
		{name: "invalid", codec: "brotli", shouldError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := CreateDefaultConfig().(*Config)
			cfg.CompressionCodec = tt.codec
			err := cfg.Validate()
			if tt.shouldError && err == nil {
				t.Fatalf("expected validation error for codec %q", tt.codec)
			}
			if !tt.shouldError && err != nil {
				t.Fatalf("unexpected validation error for codec %q: %v", tt.codec, err)
			}
		})
	}
}

func TestConfigValidateRowGroupSizeWithinMaxFileSize(t *testing.T) {
	cfg := CreateDefaultConfig().(*Config)
	cfg.MaxFileSizeBytes = 10
	cfg.RowGroupSizeBytes = 11

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error when row group size exceeds max file size")
	}
}
