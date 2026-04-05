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
