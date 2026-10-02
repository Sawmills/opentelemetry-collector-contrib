# queue-drain Local Red/Green Proof Harness

This harness is a local `kind` lab for reproducing queue-drain-style LB cascade behavior:

- 2 LB pods.
- 4 backend pods behind a headless service.
- steady exact-byte OTLP log load through the harness load generator.
- backend rollout plus deterministic slow-backend behavior.
- automatic red/green evidence capture.

The fake backend also exposes `/drain` and `/undrain` on its HTTP port. Draining
makes readiness and gRPC health return not-serving while liveness stays healthy,
which lets the lab exercise backend removal without killing the backend process.

The expected proof shape is:

- red: old/prod-like LB behavior fails under the rollout/load shape.
- green: candidate fixed LB behavior passes the same load, pod count, and rollout cadence.

## Prerequisites

```sh
kind version
kubectl version --client=true --output=yaml
docker version
```

Build the local harness image. The same image runs the fake backend, tally
server, and exact-byte load generator:

```sh
docker build -t queuedrain-fakebackend:latest test/queuedrain/fakebackend
```

Use `--skip-fake-backend-build` when the image is already present locally and
the run should avoid pulling builder base images again.

When testing a local collector build:

```sh
make docker-otelcontribcol
docker tag otelcontribcol otelcontribcol-dev:queue-drain
```

## Run

```sh
test/queuedrain/run.sh \
  --red-image public.ecr.aws/s7a5m1b4/sawmills-collector:1.936.0 \
  --green-image otelcontribcol-dev:queue-drain \
  --lb-replicas 4 \
  --active-lb-replicas-config 10 \
  --workers 4 \
  --num-consumers 30 \
  --red-num-consumers 120 \
  --target-compressed-bytes 262144 \
  --payload-profile repeated \
  --payload-size-bytes 262144
```

Artifacts are written under `artifacts/queue-drain/<timestamp>/`.

Use `--render-only` to validate red and green manifests without requiring
Docker, kind, or Kubernetes:

```sh
test/queuedrain/run.sh --render-only --phase both --artifacts /tmp/queue-drain-render
```

`--num-consumers` remains the green default. Use `--red-num-consumers` when the
red image supports the setting and the proof needs to reproduce a high
configured concurrency variation such as targetCluster's previous 120-consumer config.
Green renders `central_queue.active_load_balancer_replicas` from
`--lb-replicas` by default so backend-safe drain concurrency is divided per LB
pod. Use `--active-lb-replicas-config`, `--red-active-lb-replicas`, or
`--green-active-lb-replicas` when reproducing an HPA upper-bound config that is
larger than the actual LB pod count, such as targetCluster running 4 LBs while config
uses `active_load_balancer_replicas=10`.

Payload profiles:

- `--payload-profile repeated` generates highly-compressible log bodies.
- `--payload-profile random` generates deterministic low-compression log bodies.
- `--payload-size-bytes` controls the exact uncompressed body size. Use
  `262144` for 256 KiB, `1048576` for 1 MiB, and reserve 4 MiB for manual
  stress runs.

Request-size sweep:

```sh
test/queuedrain/sweep.sh -- \
  --green-image otelcontribcol-dev:queue-drain \
  --payload-profile repeated \
  --payload-size-bytes 1048576
```

The sweep runs `target_compressed_bytes` values `131072`, `262144`, `524288`,
and `1048576`, then writes `sweep.md` and `sweep.json` under the sweep artifact
root.

## GitHub Actions

`.github/workflows/queuedrain-hardening.yml` runs the same kind harness on manual
dispatch and nightly on `main` for `Sawmills/opentelemetry-collector-contrib`.
The default nightly profile runs highly-compressible `262144` and `1048576`
byte payloads with `target_compressed_bytes=262144`. It uploads artifacts for
all attempted payload sizes and posts to `#nightly` only on failure or
cancellation.

## Verdict

The analyzer requires:

- red has at least one incident signature: backend p99 pinned at timeout,
  queue over budget, rejected/refused records, delivery mismatch, or an LB
  liveness restart.
- green has zero LB restarts.
- green queue bytes stay below capacity.
- green oldest queue age returns near baseline.
- green refused/rejected deltas are zero.
- green backend p95 is under 2s after settle, settled over-2s count is zero
  by default, and p99 is not pinned at 5s.

Use `--green-max-over-2s-count <n>` only for intentionally noisy/manual
profiles. The nightly-equivalent proof should keep the default zero-over-2s
settled gate.

The local default does not require kubelet to kill the LB. In kind, the
deterministic proof target is the limiting factor that caused the cascade:
timeout-pinned backend exports plus queue growth. Use
`--require-red-liveness-restart` only when the local resource limits or probe
sensitivity are tight enough to reproduce the downstream kubelet kill too. The
probe can be tightened with `--liveness-timeout-seconds` and
`--liveness-failure-threshold`; use the same values for red and green.

If red does not fail any incident predicate, the load/rollout simulation is too weak.
Use `--strict-red` when you specifically need the full historical incident shape
instead of the default "any incident-shaped signature" gate.

## Dynamic lane-floor network test

`fakebackend/lane_floor_integration_test.go` exercises a collector **process** through
OTLP gRPC, a controlled DNS server, real TCP health probes, and 35 networked OTLP
sinks. It does not import the load-balancing exporter's internal implementation.
It establishes 25 lanes, grows DNS membership to 35, stops 15 sinks while keeping
DNS unchanged, recovers them, shrinks to 17, and grows again to 25. Growth runs for
35 seconds to cross the ingest-rate window. Compressible input keeps rate-derived
lanes below the backend floor, so rate growth cannot hide the hysteresis defect.
The sinks delay responses and tally unique record IDs, checking endpoint coverage,
queue pressure/drain, rejection, and missing/duplicate delivery in each run.
Both runs send the same fixed record sequence. A sink barrier holds accepted work
through every growth, quarantine, recovery, and shrink transition. The test records
the new discovery/health state alongside positive queued-item counts before releasing
delivery, then verifies all record IDs. Queue age and receiver refusal telemetry
must be present; the queue rejection counter is absent until its first rejection.

Build minimal static collector binaries from the comparison revisions with the
repository's pinned OpenTelemetry Collector Builder. Use this builder manifest,
substituting absolute paths for `CHECKOUT` and `OUTPUT`:

```yaml
dist:
  module: example.com/lanefloor
  name: otelcol-lanefloor
  version: 0.149.0
  output_path: OUTPUT
receivers:
  - gomod: go.opentelemetry.io/collector/receiver/otlpreceiver v0.149.1-0.20260402195938-76ede073ee8e
exporters:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter v0.149.0
providers:
  - gomod: go.opentelemetry.io/collector/confmap/provider/fileprovider v1.55.1-0.20260402195938-76ede073ee8e
  - gomod: go.opentelemetry.io/collector/confmap/provider/envprovider v1.55.1-0.20260402195938-76ede073ee8e
replaces:
  - github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter => CHECKOUT/exporter/loadbalancingexporter
  - github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal => CHECKOUT/pkg/batchpersignal
  - github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil => CHECKOUT/pkg/pdatautil
  - github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest => CHECKOUT/pkg/pdatatest
  - github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden => CHECKOUT/pkg/golden
  - github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics => CHECKOUT/internal/exp/metrics
```

From the repository root, with a Go toolchain satisfying the module's `go.mod`:

```sh
CGO_ENABLED=0 go tool -modfile=internal/tools/go.mod \
  go.opentelemetry.io/collector/cmd/builder --config /absolute/path/builder.yaml

# LANE_LAB is an absolute scratch directory, not a production configuration path.
LANE_LAB=/tmp/lane-floor-lab
mkdir -p "$LANE_LAB/artifacts"
(cd test/queuedrain/fakebackend && CGO_ENABLED=0 go test -c -tags integration -o "$LANE_LAB/lane-floor.test" .)
cp /absolute/path/OUTPUT/otelcol-lanefloor "$LANE_LAB/collector"
printf 'nameserver 127.0.0.1\noptions timeout:1 attempts:1\n' > "$LANE_LAB/resolv.conf"
docker run --rm --network none --read-only --tmpfs /tmp \
  --user "$(id -u):$(id -g)" --sysctl net.ipv4.ip_unprivileged_port_start=0 \
  -v "$LANE_LAB:/harness:ro" \
  -v "$LANE_LAB/resolv.conf:/etc/resolv.conf:ro" \
  -v "$LANE_LAB/artifacts:/artifacts" \
  -e QUEUEDRAIN_COLLECTOR_BINARY=/harness/collector \
  -e QUEUEDRAIN_ARTIFACTS=/artifacts \
  ubuntu:22.04 /harness/lane-floor.test \
  -test.run '^TestLaneFloorEndToEnd$' -test.v -test.timeout 4m
```

The container has only loopback networking and owns its DNS port 53; it never
changes the host resolver or an existing cluster. Run the **same test binary and
configuration** against both collectors, retaining separate artifact directories.
The pre-fix collector must fail the growth lane/coverage assertions; the patched
collector must pass. A startup error, discovery timeout, or delivery failure alone
does not count as reproducing the lane-floor defect. Collector logs, effective
configuration, Prometheus snapshots, and per-phase endpoint delivery counts are
written to the artifact directory; test output includes unique-ID verification.

This is a controlled network regression, not a customer-load capacity benchmark
or evidence of successful production deployment. The ordinary unit-test lane skips
it unless explicitly built with `integration` and given a collector binary.
