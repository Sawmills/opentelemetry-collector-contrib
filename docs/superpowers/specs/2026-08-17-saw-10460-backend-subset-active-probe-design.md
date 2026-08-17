# SAW-10460: Backend Subsets with Active Probes

## Status

Approved Approach A. Amir approved the Fable design for implementation on
2026-08-17.

## Decision

Allow `backend_subset.enabled=true` with
`endpoint_health.active_probe.enabled=true`.

Keep the existing endpoint-health manager as the owner of the complete
resolver-present membership and health state. Apply deterministic
`backend_subset` selection only to the eligible endpoint set used by the ring
and child exporters.

Remove only the `BackendSubsetConfig.Validate` rejection for active probes.
Keep these validation rules:

- `endpoint_health.enabled=true` is required.
- `log_routing.ignore_trace_id=true` is required.
- Existing active-probe validation remains in `EndpointHealthConfig.Validate`.

## Contracts

### Resolver and probe scope

The active-probe cycle reads `endpointHealth.presentEndpoints()`. It probes
every endpoint that the resolver currently presents, including endpoints that
are outside the selected subset. Probe concurrency remains bounded by
`active_probe.max_concurrency`.

### Selection and lifecycle

The endpoint-health manager applies the existing deterministic host-based
rendezvous selector after it computes eligible endpoints. It applies the same
selector to the full present set during fail-open. The selector returns at
most `backend_subset.max_endpoints` endpoints.

The ring and the live child-exporter map contain only the selected set. A
probe failure can quarantine a selected endpoint and admit the exact next
deterministic healthy candidate. The replacement is created before the ring
commit. The failed or displaced exporter is marked stopping and drained by the
existing cleanup path.

An unhealthy endpoint outside the selected set has no child exporter to
remove. Its probe state still updates in the complete endpoint-health map. A
successful recovery can re-admit it only when deterministic selection places
it in the selected eligible set.

Resolver updates, probe failures, probe recoveries, and fail-open must keep
both the ring and child-exporter count at or below `max_endpoints`. A rejected
candidate must not be materialized as a live child exporter.

Shutdown cancels the probe loop, waits for in-flight probes, stops resolver and
cleanup work, and then shuts down remaining child exporters. An in-flight
probe must not mutate health or exporter state after probe cancellation.

## Scope

In scope:

- The configuration acceptance change.
- Regression coverage for bounded selection, probe scope, deterministic
  replacement, non-selected failures, recovery, fail-open, and shutdown.
- README contract text and one changelog entry.

Out of scope:

- Changes to the selector, endpoint-health manager, ring installation, or
  exporter lifecycle implementation.
- Changes to traces, metrics, trace-affine logs, central-queue policy, or
  resolver behavior.
- Hotfixes to `v0.149.0-sawmills.48`, deployment, release, or runtime action.

## Safety basis

Main already contains the no-op ring-install protection for unchanged endpoint
sets. Approach A relies on that protection while active probes update health
and the selected child set. The tests must exercise unchanged and changing
sets through the real load-balancer methods.

## Required proof

The focused configuration test must first fail because the current validation
guard rejects the approved combination. Runtime safety tests must run against
unchanged production code before the guard is removed. Any runtime failure is
a blocker.

After the one-line production deletion, focused tests, race coverage for the
concurrent probe test, the complete exporter module suite, changelog checks,
component lint/check gates, and autoreview must pass.
