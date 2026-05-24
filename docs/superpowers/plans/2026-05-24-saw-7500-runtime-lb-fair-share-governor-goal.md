# SAW-7500 Runtime LB Fair-Share Governor Goal

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the static `active_load_balancer_replicas` hard cap with a runtime fair-share governor that starts safe, ramps when backends are healthy, backs off quickly under pressure, and proves the BigID APSE2 failure mode stays fixed.

**Architecture:** Keep the configured active-LB value as a conservative initial floor, not a permanent ceiling. Runtime drain concurrency should use additive healthy ramp-up when queue demand persists and backend pressure is quiet, and multiplicative backoff when backend pressure appears. The design must remain portable for customer installs and must not require Kubernetes API access; optional runtime LB discovery can be a later optimization, not a dependency.

**Tech Stack:** Go, OpenTelemetry Collector loadbalancingexporter, Sawmills `sawmills-collector` release flow, LaunchDarkly configuration, `sm`, `remote-operator`, `vgraf`, Kubernetes HPA/KEDA.

---

## Copy/paste `/goal`

```text
/goal Create and follow docs/superpowers/plans/2026-05-24-saw-7500-runtime-lb-fair-share-governor-goal.md. Implement a runtime LB drain fair-share governor that removes the need for static active_load_balancer_replicas tuning. Prove red/green that APSE2-like traffic with 4 active LBs, activeLB upper bound 10, and few workers no longer stays artificially pinned at effective_consumers=1 when backends are healthy, while still backing off under backend pressure. Merge green PRs through contrib and sawmills-collector, release the collector, update any required LD/config flags, validate in staging, then deploy only production-mt-1-ap-southeast-2 and prove no liveness cascade, no refusals, no exporter failures, queue bounded/draining, KEDA scaling works, and backend p95/p99 stays below the agreed latency gate.
```

## Intent

The previous SAW-7500 fix made the APSE2 zero-drain failure safe by ensuring a fractional backend share produces at least one consumer when backends are ready. That fixed the liveness cascade, but APSE2 live evidence showed the next limiter:

- BigID APSE2 ran 4 actual LB pods.
- Config used `active_load_balancer_replicas=10` as an HPA-safe upper bound.
- Runtime metrics showed `configured_consumers=30`, `active_load_balancer_replicas=10`, `backend_safe_consumers_per_lb=1`, `effective_consumers=1`, `effective_lanes=1`.
- KEDA did eventually scale workers when queue pressure crossed target, and the cluster stayed healthy.
- The queue still had to build before KEDA compensated because the LB side stayed pinned at the conservative floor.

The follow-up fix should preserve the safety property but avoid making static active-LB tuning the only way to get useful drain concurrency.

## Current Limiting Factor

`centralQueueConsumerPolicy.backendSafeConsumersPerLB` currently calculates:

```text
floor(ready_backends * max_inflight_sends_per_backend / configured_active_lb_replicas)
```

and treats that value as a hard cap. This is safe for an HPA upper bound, but too conservative when actual LBs are below the configured upper bound and backends are demonstrably healthy.

The real limiter should be backend health and queue demand, not a stale or intentionally conservative active-LB estimate.

## Required Behavior

Implement a runtime fair-share governor:

1. Keep a conservative backend-safe floor from configured active LB replicas.
2. Use that floor as the starting point when queue is non-empty and ready backends exist.
3. If queue demand remains above the current effective consumers and backend pressure is quiet, increase effective consumers gradually.
4. If backend pressure appears, reduce effective consumers quickly.
5. After pressure, recover gradually.
6. Never exceed configured `num_consumers`.
7. Never acquire consumers when there are zero ready backends.
8. Keep dynamic lanes derived from `effective_consumers`.
9. Do not require Kubernetes API permissions.

Approximate policy:

```text
fair_share_floor =
  max(1, floor(ready_backends * max_inflight_sends_per_backend / configured_active_lb_replicas))

queue_demand =
  ceil(queue_compressed_bytes / target_compressed_bytes)

healthy_ramp_limit =
  previous_effective_consumers + ramp_step

if no ready backends:
  effective_consumers = 0
else if backend pressure:
  effective_consumers = max(min_consumers, previous_effective_consumers / 2)
else:
  effective_consumers = min(configured_max_consumers, queue_demand, max(fair_share_floor, healthy_ramp_limit))
```

The first implementation can reuse the existing `pressureRecoveryStep` default as the additive ramp step unless a clearer local pattern exists. Do not add a new operator-facing knob unless the tests prove the default is too blunt.

## Files To Inspect First

- `exporter/loadbalancingexporter/central_queue_consumer_policy.go`
- `exporter/loadbalancingexporter/central_queue_consumer_policy_test.go`
- `exporter/loadbalancingexporter/central_queue_consumers.go`
- `exporter/loadbalancingexporter/central_queue_consumers_test.go`
- `exporter/loadbalancingexporter/central_queue_lane_policy.go`
- `exporter/loadbalancingexporter/central_queue_lane_policy_test.go`
- `exporter/loadbalancingexporter/central_queue_telemetry.go`
- `exporter/loadbalancingexporter/central_queue_telemetry_test.go`
- `exporter/loadbalancingexporter/config.go`
- `exporter/loadbalancingexporter/README.md`

## Task 1: Red Tests For Static Upper-Bound Pinning

**Files:**
- Modify: `exporter/loadbalancingexporter/central_queue_consumer_policy_test.go`
- Modify if needed: `exporter/loadbalancingexporter/central_queue_consumers_test.go`

- [x] Add a failing test named `TestCentralQueueConsumerPolicyRampsAboveStaticActiveLBUpperBoundWhenHealthy`.
- [x] Scenario:
  - `maxConsumers=30`
  - `targetCompressedBytes=256 << 10`
  - `maxInflightSendsPerBackend=1`
  - `activeLoadBalancerReplicas=10`
  - previous effective consumers `1`
  - queue demand at least `35`
  - ready backends `4`
  - backend pressure `false`
- [x] Expected red behavior before the fix: test fails because `effectiveConsumers` stays `1`.
- [x] Expected green behavior after the fix: `effectiveConsumers > 1`, `backendSafeConsumersPerLB == 1`, and the result is not limited by `backend_capacity`.
- [x] Add a controller-level red test proving repeated healthy decisions can ramp, but failed acquire attempts must not advance the ramp.

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run 'TestCentralQueueConsumerPolicyRampsAboveStaticActiveLBUpperBoundWhenHealthy|TestTryAcquireCentralQueueConsumer' -count=1
```

Expected before implementation: the new ramp test fails for `effectiveConsumers` stuck at `1`.

## Task 2: Implement Runtime Healthy Ramp

**Files:**
- Modify: `exporter/loadbalancingexporter/central_queue_consumer_policy.go`

- [x] Add a new limit reason if useful, for example `healthy_ramp`, and include it in `centralQueueConsumerLimitReasons`.
- [x] Preserve `backendSafeConsumersPerLB` as the reported conservative floor.
- [x] Change `compute` so the backend-safe value is not a hard ceiling when:
  - ready backends are nonzero,
  - queue demand is above backend-safe floor,
  - backend pressure is false.
- [x] Use previous committed `effectiveConsumers` to additively increase toward queue demand, configured max, and current backend capacity.
- [x] Keep first-sample behavior safe: if there is no previous effective consumer, start at the conservative backend-safe floor.
- [x] Keep backend pressure behavior: halve from previous effective consumers when possible, or from the current computed effective value on first sample.
- [x] Keep pressure recovery gradual and do not double-ramp in the same decision.
- [x] Keep `tryAcquire` commit semantics: a ramp increase should only commit when an acquire succeeds, while a pressure reduction can commit even when acquire fails.

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run 'TestCentralQueueConsumerPolicy|TestTryAcquireCentralQueueConsumer' -count=1
```

Expected: all policy and acquire tests pass.

## Task 3: Prove Backoff And Dynamic Lanes

**Files:**
- Modify: `exporter/loadbalancingexporter/central_queue_consumer_policy_test.go`
- Modify if needed: `exporter/loadbalancingexporter/central_queue_lane_policy_test.go`
- Modify if needed: `exporter/loadbalancingexporter/central_queue_telemetry_test.go`

- [x] Add or update a test where backend pressure reduces a ramped value quickly.
- [x] Add or update a test where pressure clears and consumers recover gradually, not all at once.
- [x] Confirm `central_queue.effective_lanes` tracks `effective_consumers`, not configured `num_consumers`.
- [x] If a new limit reason is added, update telemetry tests to assert it is exported.

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run 'TestCentralQueueConsumerPolicy|TestCentralQueueConsumerTelemetry|TestCentralQueueLane' -count=1
```

Expected: all targeted tests pass.

## Task 4: Full Contrib Verification And PR

**Files:**
- Commit only touched files.

- [x] Run:

```bash
cd exporter/loadbalancingexporter
go test . -count=1
git diff --check
```

- [ ] Commit with Conventional Commit:

```bash
git add exporter/loadbalancingexporter
git commit -m "fix(loadbalancingexporter): ramp central queue consumers when backends stay healthy" -m "Refs: SAW-7500" -m "Assisted-by: Codex"
```

- [ ] Push and open PR with `SAW-7500` in the title/body.
- [ ] Sweep all GitHub checks/reviews to green.
- [ ] Merge only after checks are green.
- [ ] Publish or confirm the next `exporter/loadbalancingexporter/v0.149.0-sawmills.N` tag.
- [ ] Update SAW-7500 Linear comment with PR link, commit SHA, tests, and release tag.

## Task 5: Bump `sawmills-collector`

**Files:**
- Modify: `builder-config.yaml`
- Modify: `cmd/sawmills-otelcol/go.mod`
- Modify: `cmd/sawmills-otelcol/go.sum`

- [ ] Create a fresh `sawmills-collector` worktree from `origin/main`.
- [ ] Bump loadbalancingexporter replace tag to the new release.
- [ ] Run:

```bash
cd cmd/sawmills-otelcol
go mod tidy
go test . -count=1
cd ../..
make build
git diff --check
```

- [ ] Commit with `Refs: SAW-7500`.
- [ ] Open PR, sweep to green, merge.
- [ ] Confirm `sawmills-collector` release tag exists.
- [ ] Update SAW-7500 Linear comment.

## Task 6: Config And LD Review

**Files/Systems:**
- LaunchDarkly production/staging flags
- `collectors-service` only if new config fields are required

- [ ] Prefer no new operator-facing knob.
- [ ] If no new field is required, keep BigID queue variation:
  - `central_queue.num_consumers=30`
  - `central_queue.active_load_balancer_replicas=10`
  - `target_compressed_bytes=262144`
  - zstd `encoder_concurrency=1`
- [ ] If a new field is required, implement and prove rendering in `collectors-service` before rollout.
- [ ] Dry-run APSE2 generated config before live deploy.
- [ ] Read back exact LD variation IDs/values into SAW-7500.

## Task 7: Staging Validation

**Systems:**
- Sawmills staging collector deployment suitable for LB central queue validation.

- [ ] Deploy the new collector to staging first.
- [ ] Use a controlled queue/traffic profile or available staging load.
- [ ] Prove:
  - effective consumers start at the conservative floor,
  - effective consumers rise above the floor while backend pressure is quiet,
  - synthetic/backend pressure reduces effective consumers,
  - pressure clears and recovery is gradual,
  - no receiver/processor/exporter refusals,
  - backend p95/p99 remains below 2 seconds unless a stricter live gate is established.
- [ ] Update SAW-7500 with staging evidence.

## Task 8: APSE2-Only BigID Validation

**Target only:**
- BigID `production-mt-1-ap-southeast-2`

- [ ] Capture pre-deploy baseline:
  - collector version,
  - worker/LB/scaler replicas,
  - HPA/KEDA targets and replicas,
  - queue bytes/items/age,
  - configured/effective consumers,
  - active LB replicas,
  - backend-safe consumers per LB,
  - effective lanes,
  - backend p95/p99,
  - receiver/processor/exporter refusals,
  - pod restarts/events,
  - CPU/RSS by worker and LB.
- [ ] Deploy only APSE2 after staging is green.
- [ ] Monitor until stable.
- [ ] Required live green proof:
  - no liveness cascade,
  - no pod restarts/OOM,
  - no sustained readiness flaps,
  - receiver refusals `0`,
  - processor refusals `0`,
  - exporter send failures `0` in the post-stabilization window,
  - queue bounded or draining,
  - KEDA scales when queue target is crossed,
  - effective consumers rise above conservative floor while backends are healthy,
  - effective consumers drop under pressure if pressure occurs or is synthetically induced in a safe staging/local environment,
  - backend p95/p99 below 2 seconds unless the user sets a different gate.
- [ ] Roll back immediately if the old APSE2 failure signature appears.
- [ ] Update SAW-7500 with final evidence table.

## Done Criteria

The goal is complete only when:

- the runtime fair-share governor is implemented,
- red/green tests prove static upper-bound pinning is fixed,
- pressure backoff is proven,
- dynamic lanes still follow effective consumers,
- contrib PR is merged and released,
- `sawmills-collector` PR is merged and released,
- LD/config readback is documented,
- staging validation is green,
- APSE2-only live validation is green,
- SAW-7500 has final evidence and rollback/tuning notes.
