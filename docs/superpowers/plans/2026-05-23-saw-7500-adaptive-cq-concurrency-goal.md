# SAW-7500 Adaptive Central Queue Concurrency Goal

## Copy/paste `/goal`

```text
/goal Follow docs/superpowers/plans/2026-05-23-saw-7500-adaptive-cq-concurrency-goal.md and implement adaptive central-queue drain concurrency end to end. Prove with red/green tests that the BigID APSE2 failure mode is fixed, merge green PRs through the required repos, release the fixed collector/config path, then validate only the problematic BigID cluster production-mt-1-ap-southeast-2 with rollback gates.
```

## Intent

Fix the control-loop bug exposed by BigID `production-mt-1-ap-southeast-2`.

- KEDA scales workers from LB central queue bytes.
- The LB central queue can currently drain faster than ready workers can safely expand and process payloads.
- With BigID's previous `central_queue.num_consumers=120` variation, total LB-to-worker send pressure multiplied by LB replica count.
- Workers hit memory/probe pressure before KEDA could add ready backend capacity.

The permanent fix is not a magic consumer count. The fix is to bound LB drain concurrency by backend capacity that actually exists and is healthy, while still letting KEDA scale the worker tier from queue pressure.

## Source Evidence

Use SAW-7500 as the recovery anchor. Keep updating it throughout the goal.

Known failure evidence from APSE2:

- Failed rollout: `sawmills-collector 1.1023.0`.
- Failed BigID config: central queue dynamic lanes with 120 CQ consumers.
- Worker `main-collector` containers hit OOM/readiness/liveness failures.
- Raising worker min `3 -> 6 -> 12` did not stabilize the rollout.
- Raising worker memory `2Gi -> 4Gi` did not stabilize the rollout.
- Rollback to `1.1013.0` restored health.
- Immediate guardrail already applied in LaunchDarkly: `collectorsServiceCollectorLbQueueConfig` BigID variation 15 now uses `central_queue.num_consumers=30` with the same 1 GiB queue cap.

Treat the 30-consumer guardrail as a temporary safety bound, not the desired architecture.

## Objective

Build adaptive central-queue drain concurrency and combine it with dynamic lanes:

1. KEDA remains the worker scaling loop from central queue bytes.
2. LB drain concurrency becomes a runtime effective value, not the raw configured maximum.
3. The configured consumer count becomes `configured_max_consumers`.
4. Runtime effective consumers are derived from queue demand, currently ready backend workers, active LB replicas if available, and backend pressure signals.
5. Dynamic lanes derive from effective consumers and backend/fill-rate math.
6. The APSE2-shaped red workload fails before the fix and passes after the fix.

## Core Behavior

Implement the runtime policy approximately as:

```text
queue_demand_consumers =
  ceil(queue_compressed_bytes / target_compressed_bytes_per_consumer)

backend_safe_consumers_per_lb =
  floor(ready_worker_backends * max_inflight_sends_per_worker / active_lb_replicas)
  with a minimum of 1 when fleet backend capacity is non-zero, so fractional
  per-LB share does not deadlock all LBs

pressure_adjusted_consumers =
  reduce quickly when backend pressure is present;
  recover slowly when backend pressure is quiet

effective_consumers =
  clamp(
    min(min_consumers, backend_safe_consumers_per_lb),
    min(configured_max_consumers, queue_demand_consumers, backend_safe_consumers_per_lb),
    configured_max_consumers
  )
```

Use ready/usable backend endpoints, not desired KEDA replicas. KEDA may be scaling up, but the LB must not spend capacity before workers are ready.

Backend pressure should include the signals already visible or available in the LB/exporter path:

- backend request p95/p99 close to timeout,
- endpoint failures, reroutes, quarantines, or 503s,
- backend send timeouts,
- worker receiver/processor refusals if available through metrics,
- readiness/probe pressure if available through backend health,
- rising in-flight bytes or queue oldest age.

Dynamic lanes should use `effective_consumers`, not the static configured consumer count. Lane count remains an internal batching/fairness decision.

## Metrics And Observability

Add or verify metrics for:

- `configured_consumers`
- `effective_consumers`
- `configured_lanes`
- `effective_lanes`
- `ready_backends`
- `active_lb_replicas` when known
- `queue_demand_consumers`
- `backend_safe_consumers_per_lb`
- pressure reduction reason/state
- pressure recovery state

These metrics must be visible in local tests and in APSE2 post-deploy validation.

## Repos And Scope

Start from a clean worktree that is up to date with `origin/main`.

Use SAW-7500 unless a narrower Linear sub-ticket is clearly required. If a new sub-ticket is created, link it from SAW-7500 and keep SAW-7500 updated as the main rollout/evidence anchor.

Implement where needed across:

- `opentelemetry-collector-contrib` / Sawmills collector fork: adaptive central queue runtime behavior and tests.
- `sawmills-collector`: dependency bump, build/release, smoke/regression proof.
- `collectors-service`: config rendering and LaunchDarkly support if fields or defaults change.
- `helm-charts`: only if runtime metadata, env, service discovery, or chart values are required.

Do not broaden the BigID rollout to other clusters as part of this goal. The live customer proof target is only `production-mt-1-ap-southeast-2`.

## Red/Green Proof

### Red: Local/APSE2-Shaped Harness

Build or extend a local deterministic harness that reproduces the APSE2 shape:

- few worker backends,
- multiple LB replicas or simulated LB replica count,
- central queue enabled,
- high configured max consumers matching the old bad config,
- BigID-like sampled/compressible log traffic,
- payload expansion and worker processing heavy enough to hit the old limiter,
- KEDA-like delayed worker readiness, or explicit ready-worker count changes.

The old behavior must show at least one of:

- worker OOM or memory-limit failure,
- worker memory limiter refusals,
- readiness/liveness stalls,
- backend request p99 pinned near timeout,
- queue not draining despite KEDA demand,
- generated/delivered mismatch.

Record the artifact path, command, config, collector versions, and analyzer summary in SAW-7500.

### Green: Same Harness With Adaptive Consumers

Run the exact same workload with the new adaptive policy.

Acceptance:

- no OOM,
- no liveness kills,
- no sustained readiness stalls,
- no sustained receiver/processor/exporter refusals,
- generated ~= delivered,
- queue drains within the agreed delay budget,
- effective consumers start bounded by ready workers,
- effective consumers rise when workers become ready,
- pressure signals reduce consumers quickly,
- recovery increases consumers slowly,
- dynamic lanes follow effective consumers.

### Unit Tests

Cover these cases:

- few workers plus many LBs caps effective consumers,
- more ready workers increases effective consumers,
- queue growth increases demand but does not exceed backend-safe cap,
- backend pressure reduces effective consumers,
- pressure recovery is gradual,
- dynamic lanes use effective consumers,
- static `lane_count` remains ignored/deprecated for normal operator config,
- configured `num_consumers` remains the max send-parallelism knob.

## PR And Merge Path

Open PRs with `SAW-7500` in title/body or footer.

Expected merge order:

1. Collector runtime change.
2. `sawmills-collector` dependency bump and release.
3. `collectors-service` config/LD rendering changes if required.
4. `helm-charts` only if required.

For every PR:

- run local red/green proof where applicable,
- get CI/review sweep green,
- update SAW-7500 with PR link, check state, artifact links, and next step,
- merge only after green proof and review/CI are clean.

## LaunchDarkly / Config Path

After the release/config path exists:

- update LD variations to expose the adaptive behavior,
- keep BigID's temporary 30-consumer guardrail until the adaptive collector is proven on APSE2,
- dry-run generated APSE2 config and verify rendered adaptive fields/metrics,
- read back exact LD variation IDs and values in SAW-7500.

## APSE2 Live Validation

Before deploying the fixed version to APSE2, capture baseline:

- deployed collector version,
- worker/LB/scaler replica counts,
- HPA/KEDA targets and current metrics,
- queue bytes, queue age, effective consumers, effective lanes,
- receiver/processor/exporter refusals,
- backend p95/p99,
- worker RSS/heap/CPU,
- LB RSS/heap/CPU,
- recent pod restarts/events.

Deploy only after local/staging green evidence exists.

After deploy, monitor until stable:

- workers and LBs fully ready,
- no OOM/restarts,
- no liveness kills,
- no sustained readiness flaps,
- no recent receiver refusals,
- no exporter send failures,
- no sustained processor refusals,
- queue drains,
- KEDA worker scaling remains active from queue bytes,
- adaptive consumers track ready backend capacity,
- dynamic lanes track effective consumers.

If CPU or memory remains high, take pprof from both LB and worker using the remote-operator download path where available.

Rollback immediately if APSE2 repeats the failure signature.

## Done Criteria

The goal is complete only when:

- adaptive central-queue drain concurrency is implemented,
- dynamic lanes are driven by effective consumers,
- red/green local harness proves the old APSE2 failure and new fix,
- all required PRs are merged with green sweeps,
- fixed collector release exists,
- generated config/LD readback proves APSE2 will use the adaptive behavior,
- APSE2 live validation is green or rollback evidence is documented,
- SAW-7500 contains final evidence table with red before, green after, PRs, merged versions, LD variation/readback, APSE2 metrics, and rollout conclusion.
