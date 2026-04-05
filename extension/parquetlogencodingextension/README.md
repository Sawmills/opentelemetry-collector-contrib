# Parquet Log Encoding Extension

Buffers Datadog-shaped OTLP logs and encodes them into parquet payloads for downstream S3 export.

Wave 1 scope in this fork:

- datadog log path only
- flush metadata surface retained for later `awss3exporter` integration
- parity-focused buffering and flush telemetry
