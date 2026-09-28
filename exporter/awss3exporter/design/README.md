# SAW-11452 Archive Recovery Research

Use a small pending-archive owner around the existing stateful encoder. Prepare the object key and upload bytes once. Retain that object until upload succeeds. Stop further encoder mutation while retained capacity is unavailable.

This recommendation addresses failed uploads within a running process. It does not establish recovery after process failure. Durable delivery requires a separate storage contract for both unfinished encoder buffers and complete archives.

## Evidence and Scope

Research examined fork commit `aeb249f605dbabc0081c700867de1afddc155a93` and the exact Collector dependency revision `76ede073ee8e`. The encoder source came from collector commit `b541a8ec9ffb66df76add1f75c28cd82b2ca241e`, used by the prior source review. No product files changed during this research. The controlled staging report for SAW-11452 found missing records after exhausted uploads and repeated writes after lost responses.

The Datadog encoder copies the completed gzip buffer, then resets it before the exporter sees an upload result. Replaying only the last input batch cannot reconstruct earlier records. See [checkAndFlush and initializeBuilder][encoder].

The S3 [logs factory][factory] uses pdata queueing and timeout, with no helper retry option. Adding helper retry around `ConsumeLogs` repeats stateful encoding. This does not retain the first archive. The [upload manager][writer] also builds a new partition key on every `Upload` call. A retry must reuse the prepared key, bucket, content, and metadata.

## Existing Helper Request and Queue

The exact [custom request API][custom] accepts a converter and request sender. A custom archive request can retain immutable bytes and implement `ItemsCount`, `BytesSize`, and `MergeSplit`. Its [retry sender][retry] reuses the request unless its error handler replaces it. Timeout wraps each attempt inside retry. This is suitable for immutable, independently produced requests.

The [converter implementation][converter] runs before queue admission. It wraps converter errors as permanent. The [memory queue][memory] can reject a request because capacity is exhausted or the request exceeds capacity. Blocking admission can also fail when its context expires. There is no public reservation step before conversion.

This ordering leaves an ownership gap for this encoder. The archive can leave the encoder before the queue accepts it. A queue failure then strands earlier accepted records. Blocking admission alone does not close that gap. A separate owner must retain the completed archive across failed admission.

The queue supports bytes, items, or request sizing. Its capacity includes queued and active requests until completion. A custom archive request must disable batching or provide semantics that preserve object identity. Generic merging or splitting can invalidate a prepared key and encoded archive.

The [persistent queue][persistent] serializes custom requests through a supplied encoding. It preserves an active request on the helper-specific shutdown error. It deletes requests after ordinary terminal failures. Therefore, a persistent queue alone does not mean retention until success. Retry expiration, cancellation, permanent errors, storage failures, and shutdown need explicit tests.

The [helper shutdown order][base] stops retries, drains its queue, then calls exporter shutdown. The current exporter flushes remaining encoder data in its shutdown hook. That final archive cannot rely on admission to an already-stopped helper queue.

A retained-archive gate adds another shutdown risk. Helper consumers can block at that gate while the helper waits for queue shutdown. Its async queue waits for consumers without selecting on the shutdown context. The exporter shutdown hook then cannot unblock them. Recovery needs an outer shutdown wrapper that stops admission when the shutdown context expires, or explicit rejection of that queue combination. The implementation requires a non-blocking in-memory helper queue. It rejects disabled queues, wait-for-result, block-on-overflow, and persistent storage so upstream shutdown can reach the exporter wrapper and no persisted request is deleted by ordinary cancellation.

## Narrow Pending Archive Owner

A serialized owner can cover record-triggered, timer-triggered, and shutdown-triggered flushes in one place. It first drains the existing pending archive. Only then can it mutate the encoder or create another archive. Failed attempts preserve the same prepared upload object.

This approach reuses the existing SDK uploader and an established backoff implementation. It requires new ownership and lifecycle code because neither the current encoder nor helper queue exposes transactional handoff. It avoids changing every ordinary pdata request into a custom request type.

The owner must distinguish rejected input from accepted input. Before encoder mutation, cancellation or capacity exhaustion can return a retryable rejection. After mutation, the exporter owns those records. Returning the original batch as retryable after retaining its archive can cause duplicate records from upstream replay.

A synchronous first upload preserves healthy request timing. Ownership must transfer before that attempt. After failure, background retries use the same prepared request.

An independent retry worker is required for recovery after input stops. Its lifetime belongs to the exporter, not the incoming request. Each network attempt has a bounded deadline. Canceling one attempt must not delete the pending archive. Shutdown stops timer work and drains within its context. When that context expires, it closes admission and joins retry workers.

If shutdown cannot drain retained data, it must return an error with retained counts and bytes. Logging success would hide loss. Memory-only retention cannot survive process exit, including an unsuccessful shutdown.

## Capacity and Default Behavior

One pending archive gives a hard bound on the number of retained complete archives. It does not provide an absolute memory-byte bound. The current encoder checks its size threshold after consuming a full input batch. A single batch can exceed that threshold. Compression and upload preparation can also allocate additional copies.

A strict byte limit needs an encoder reservation or transactional flush API, plus input and output size limits. Rejecting an oversized completed archive after encoder reset loses already accepted records. A narrower initial implementation must document the single-archive oversize case and prevent further encoder mutation until recovery.

An initial optional configuration can use `archive_recovery: false`, bounded retry intervals, and the existing timeout for each attempt. The one-archive limit stays fixed and documented. Default-off preserves existing behavior, including its data-loss risk.

Recovery must not silently disappear when users retain the existing configuration. If enabled by default, the change replaces silent loss with backpressure during outages. That behavior requires load and shutdown tests. If disabled by default, the original loss remains until configuration changes explicitly enable recovery.

The pending limit, current bytes, oldest age, retry attempts, and last successful upload need bounded telemetry labels. Failure logs can include object keys. Metrics must use the existing alert system to notify the responsible team. Metrics and successful notification delivery are separate evidence.

## Durable Storage Alternative

The existing [storage extension interface][storage] provides namespaced clients with `Get`, `Set`, `Delete`, and `Batch`. It supports recovery of stored values after restart. It does not provide a complete archive scheduler or transactional transfer from the encoder.

For durable archives, the implementation must persist content, object identity, and pending metadata before acknowledging ownership. It must handle ambiguous storage writes, quota exhaustion, recovery ordering, and deletion only after upload success. The encoder must retain unfinished accepted records too, or crash durability remains incomplete.

A new custom spool duplicates queue lifecycle and persistence logic. Existing helper persistence is preferable when a transactional encoder handoff makes its admission contract safe. That broader design has more scope than the current upload-failure fix.

## Required Tests Before Rollout

- An archive combines earlier buffered records and the triggering batch. Repeated failures must preserve every record and the exact content hash.
- Outages end after input stops. Background retry must deliver retained content without a new input batch.
- Six-second uploads exceed a five-second attempt deadline. Later healthy attempts must recover the same archive.
- The destination writes the object but drops the response. Retries must use the same key and exact bytes.
- New input arrives while retention is full. Rejection must occur before mutation, with no silent success and no duplicated accepted batch.
- Timer flush, record flush, shutdown, and cancellation race. Race detection must pass, and no archive can lose its owner.
- Shutdown ends during an outage. Retained data must appear in the returned error and telemetry. A memory-only implementation must not claim restart recovery.
- A single oversized input batch exceeds the normal archive threshold. The result must match the documented capacity contract.
- Real S3 versions and downstream events need separate duplicate-delivery tests. Same-key overwrites do not establish exactly-once downstream delivery.

[encoder]: https://github.com/Sawmills/sawmills-collector/blob/b541a8ec9ffb66df76add1f75c28cd82b2ca241e/extension/datadoglogencodingextension/extension.go
[factory]: ../factory.go
[writer]: ../internal/upload/writer.go
[custom]: https://github.com/open-telemetry/opentelemetry-collector/blob/76ede073ee8e/exporter/exporterhelper/xexporterhelper/new_request.go
[converter]: https://github.com/open-telemetry/opentelemetry-collector/blob/76ede073ee8e/exporter/exporterhelper/internal/new_request.go
[retry]: https://github.com/open-telemetry/opentelemetry-collector/blob/76ede073ee8e/exporter/exporterhelper/internal/retry_sender.go
[memory]: https://github.com/open-telemetry/opentelemetry-collector/blob/76ede073ee8e/exporter/exporterhelper/internal/queue/memory_queue.go
[persistent]: https://github.com/open-telemetry/opentelemetry-collector/blob/76ede073ee8e/exporter/exporterhelper/internal/queue/persistent_queue.go
[base]: https://github.com/open-telemetry/opentelemetry-collector/blob/76ede073ee8e/exporter/exporterhelper/internal/base_exporter.go
[storage]: https://github.com/open-telemetry/opentelemetry-collector/blob/76ede073ee8e/extension/xextension/storage/storage.go
