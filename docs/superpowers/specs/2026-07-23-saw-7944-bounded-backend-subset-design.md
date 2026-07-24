# SAW-7944: Bounded Load-Balancer Backend Subsets

## Status

Approved for implementation on 2026-07-23.

Scope: opt-in, affinity-free logs only.

## Problem

The load-balancing exporter currently creates one child OTLP exporter for every
resolved backend. BigID runs roughly 400 gateway replicas, 400 worker replicas,
and three log load-balancing exporter instances per gateway. That produces
approximately 480,000 long-lived child transports:

```text
400 gateways * 400 workers * 3 log exporters = 480,000 transports
```

During a KEDA worker scale-down, removal or failure of one worker is therefore
observed by nearly every gateway. The resulting simultaneous transport
failures, quarantines, retries, and exporter lifecycle work increased central
queue age beyond 60 seconds, made gateway readiness fail, and concentrated
traffic onto the remaining ready gateways.

CPU was not the limiting fleet resource. Worker CPU averaged about 311 millicores
against a 4-core limit. Profiles instead showed the gateway's full-mesh
transport topology as the incident trigger. A separate worker profile showed
handlers blocked in the single-shard batch processor; that is a distinct
capacity investigation and does not block this topology fix.

## Goals

- Bound each gateway's child transports at `O(K)` instead of `O(all backends)`.
- Preserve existing behavior exactly when the feature is disabled.
- Limit v1 to logs that explicitly opt out of trace-ID affinity.
- Preserve endpoint-health quarantine, recovery, drain, and central-queue
  reroute behavior within the selected subset.
- Make a one-backend membership change alter at most one selected slot.
- Prevent fail-open and recovery paths from recreating full fanout.
- Expose full resolved membership and selected membership independently.

## Non-goals

- Traces, metrics, or logs requiring trace-ID affinity.
- Active probing with bounded subsets.
- Sharing gRPC transports between separate exporter instances.
- Replacing endpoint-aware routing with a Kubernetes Service VIP.
- Fixing worker batch-processor serialization in this change.
- Automatically reducing BigID worker replicas as part of the software release.

## Configuration

Add an optional top-level block:

```yaml
backend_subset:
  enabled: true
  max_endpoints: 32
  seed: optional-explicit-override
```

When `enabled` is false or the block is absent, all existing behavior remains
unchanged.

When enabled:

- `max_endpoints` must be greater than zero.
- `endpoint_health.enabled` must be true.
- `endpoint_health.active_probe.enabled` must be false.
- `log_routing.ignore_trace_id` must be true.
- An explicit `seed`, after trimming and environment expansion, must be
  non-empty.
- If `seed` is omitted, the exporter uses `os.Hostname()`. In Kubernetes this is
  the pod name, so all load-balancing exporter instances in one gateway use the
  same seed without requiring a chart environment variable.
- Trace and metric exporter constructors reject the feature.
- The logs constructor rejects it unless `ignore_trace_id` is true.

Structural validation belongs in `Config.Validate`. Signal-specific rejection
belongs in the signal constructors because the same config type creates log,
trace, and metric exporters.

## Selection

The endpoint-health manager remains the owner of the full resolved membership
and its quarantine state. After it computes eligible endpoints, it applies one
deterministic rendezvous-ranking step and returns at most `K` endpoints to every
consumer.

The rendezvous score is derived from:

```text
gateway seed + normalized backend host
```

The score deliberately excludes the backend port. BigID's three log exporter
instances resolve the same workers on different receiver ports; hashing
`host:port` would produce three uncorrelated subsets and triple the worker
removal blast radius. Selection ranks normalized hosts but returns the original
full endpoints.

Required properties:

- Input ordering does not affect the result.
- When the eligible count is at most `K`, all eligible endpoints are returned.
- Adding one endpoint changes at most one selected slot.
- Removing an unselected endpoint changes no selected slots.
- Removing a selected endpoint admits exactly the next-ranked eligible endpoint.
- Different gateway seeds distribute selections independently.
- All exporter instances in one gateway select the same backend hosts.

For BigID at `K=32`:

```text
400 gateways * 32 workers * 3 log exporters = 38,400 transports
```

This is a 12.5-times reduction from the current 480,000 transports. Each worker
is selected by about 32 gateways on average. The expected selection coefficient
of variation is about 17% at 400 gateways and 400 workers; current worker CPU
headroom can absorb the expected tail. `K=64` is the rollback-safe tuning option
if production load distribution is materially worse.

## Health and Exporter Lifecycle

Subset filtering must be centralized inside the endpoint-health manager after
eligibility is computed. No caller may independently subset a decision. This
keeps resolver updates, transport failures, successful sends, quarantine
expiry, and recovery on one selection contract.

The manager continues to calculate fleet-level health guardrails against full
membership:

- `min_eligible_backends`
- `max_quarantined_percent`
- the under-pressure threshold

Those guardrails detect global backend health. Selected-endpoint failures are
handled by ranked replacement.

Fail-open ignores quarantine but still returns only the top `K` present
endpoints. It must never return full membership while subset mode is enabled.
Otherwise a mass-quarantine event would recreate the original full-mesh failure
mode.

Every reconciliation diffs live child exporters against the selected set, not
the full resolved set. When recovery causes a higher-ranked endpoint to
displace its temporary replacement, the displaced exporter is marked stopping,
drained, and shut down through the existing lifecycle machinery. Repeated
quarantine and recovery cycles must leave exactly `K` live exporters rather
than leaking one transport per cycle.

Unselected endpoints have no passive health signal. A newly admitted replacement
may therefore incur one failed send and timeout before quarantine if it was
already unhealthy. This is accepted in v1 because failure remains bounded to
ranked candidates instead of triggering fleet-wide connection churn.

Active probes are rejected in v1. Probing full membership restores
`O(gateways * workers)` TCP fanout; probing only selected endpoints adds recovery
semantics that are unnecessary for the first safe release.

## Routing and Central Queue

The hash ring, child exporter map, routing, and central-queue reroute attempts
operate only on selected endpoints.

`routableBackendCount()` therefore becomes at most `K`. This value affects
central-queue lane and consumer policy. BigID currently renders 30 consumers,
which remains below `K=32`, but enablement requires verification of the rendered
configuration and runtime telemetry. No rollout may silently clamp active
consumers or lanes below the configured requirement.

## Telemetry

Keep the existing resolved-backend metric as full membership. Add a gauge for
selected backends, with no seed or endpoint labels:

```text
otelcol_loadbalancer_num_selected_backends
```

Also record selection displacement events using bounded-cardinality telemetry
already available to the component, or add a counter without endpoint labels if
none exists. Steady-state displacement should be approximately zero.

Metadata, generated telemetry bindings, schema, and README documentation ship
with the implementation.

## Compatibility

- Feature absent or disabled: existing code path and tests remain unchanged.
- Feature enabled for traces or metrics: configuration error.
- Feature enabled for traced logs: configuration error.
- Feature enabled without endpoint health: configuration error.
- Feature enabled with active probes: configuration error.
- Collector versions that do not support the block: collectors-service omits it
  using the existing version-gated silent-omit pattern.

## Required Tests

1. Selector property tests: order invariance, host-keyed ranking across ports,
   `N <= K`, per-membership-delta churn, and different-seed distribution.
2. Exact lifecycle tests: unselected add causes no exporter work; displacing add
   causes one create and one drain; selected removal admits the next endpoint.
3. Flap-cycle leak regression: quarantine, replace, recover, displace; exporter
   count remains `K` and the displaced exporter drains.
4. Mass-quarantine fail-open regression: routing and exporter counts never
   exceed `K`.
5. Validation matrix across signal type, trace affinity, endpoint health,
   active probes, maximum size, and explicit seed.
6. Disabled-mode compatibility test preserving current ring and exporter
   behavior.
7. Central-queue integration proving routable count, consumer acquisition, lane
   sizing, and reroutes stay inside the subset.
8. Race-enabled churn stress combining resolver updates, failures, recoveries,
   consumes, and shutdown.
9. Telemetry test distinguishing full resolved count from selected count.
10. A 400-by-400 distribution simulation bounding expected max-to-mean load.
11. Collectors-service version-gated rendering tests, including omission for old
    collector versions.

## Release and Rollout

1. Release the exporter change dark by default.
2. Bump the exporter in sawmills-collector.
3. Add version-gated collectors-service rendering.
4. Enable only for BigID logs at `K=32`.
5. Confirm:
   - selected-backend gauge equals 32 on every gateway;
   - child transports per gateway are approximately `3 * K`;
   - worker inbound transport max-to-mean ratio is at most 1.5;
   - steady-state displacement rate is approximately zero;
   - effective central-queue lanes and consumers match expectations;
   - queue oldest age remains below 60 seconds;
   - every gateway and worker is ready;
   - refused telemetry and HAProxy 5xx remain zero;
   - gateway restarts deterministically reconverge;
   - disabling the feature cleanly restores full fanout.
6. Capture post-enable gateway and worker profiles and socket counts.
7. Only after all gates pass, retry one KEDA step from 400 to 397 workers and
   re-run the same gates.

## Alternatives Rejected

### Process-wide shared `ClientConn` registry

This preserves global affinity but removes only the three-times duplication. It
still leaves approximately 160,000 transports and 400 inbound connections per
worker, remains `O(gateways * workers)`, and requires invasive transport
lifecycle sharing across exporter instances.

### Kubernetes Service VIP

This reduces explicit fanout but long-lived HTTP/2 connections can pin unevenly.
It also removes endpoint-aware health, stable routing, draining, and the
existing central-queue reroute contract.

## Follow-up: Worker Backpressure

The worker profile showed about 40 inbound handlers blocked in
`batchprocessor.singleShardBatcher.consume`. The processor has one shard because
`metadata_keys` is empty and its export loop is synchronous. The downstream S3
exporter also has queueing and a serialized batch marshaler, so the profile does
not prove which boundary is the limiting one.

After the fanout fix stabilizes topology, capture full block profiles and run a
controlled benchmark that separates batch-processor wait from downstream S3
handoff. Do not invent high-cardinality metadata keys or remove batching based
on the current stack alone.
