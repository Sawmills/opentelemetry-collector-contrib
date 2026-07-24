# SAW-7944 Bounded Backend Subset Exporter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in logs-only rendezvous backend subset that caps each load-balancing exporter at `K` child transports while preserving health, reroute, drain, and disabled-mode behavior.

**Architecture:** The endpoint-health manager owns full resolver membership and applies one host-keyed rendezvous selector after health eligibility. All load-balancer lifecycle commits consume that already-bounded set, diff child exporters against it, and record full-versus-selected telemetry. Structural validation requires endpoint health and rejects active probes; signal constructors restrict the feature to affinity-free logs.

**Tech Stack:** Go 1.25, OpenTelemetry Collector exporter APIs, SHA-256 rendezvous ranking, generated OTEL metrics via mdatagen, testify.

---

### Task 1: Configuration and signal contract

**Files:**
- Modify: `exporter/loadbalancingexporter/config.go`
- Modify: `exporter/loadbalancingexporter/factory.go`
- Modify: `exporter/loadbalancingexporter/config.schema.yaml`
- Modify: `exporter/loadbalancingexporter/config_test.go`
- Modify: `exporter/loadbalancingexporter/log_exporter.go`
- Modify: `exporter/loadbalancingexporter/log_exporter_test.go`
- Modify: `exporter/loadbalancingexporter/trace_exporter.go`
- Modify: `exporter/loadbalancingexporter/trace_exporter_test.go`
- Modify: `exporter/loadbalancingexporter/metrics_exporter.go`
- Modify: `exporter/loadbalancingexporter/metrics_exporter_test.go`

- [ ] **Step 1: Write failing configuration validation tests**

Add table cases that start from `createDefaultConfig()` and verify:

```go
cfg.BackendSubset = BackendSubsetConfig{
    Enabled:      true,
    MaxEndpoints: 32,
}
cfg.EndpointHealth.Enabled = true
cfg.LogRouting.IgnoreTraceID = true
require.NoError(t, cfg.Validate())
```

Then independently verify errors for `MaxEndpoints <= 0`, endpoint health
disabled, active probe enabled, traced logs, and an explicit blank seed:

```go
blank := "  "
cfg.BackendSubset.Seed = &blank
require.ErrorContains(t, cfg.Validate(), "backend_subset.seed")
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./exporter/loadbalancingexporter -run 'TestConfigValidateBackendSubset' -count=1
```

Expected: build failure because `BackendSubsetConfig` and `Config.BackendSubset`
do not exist.

- [ ] **Step 3: Add the typed configuration and structural validation**

Add:

```go
type BackendSubsetConfig struct {
    Enabled      bool    `mapstructure:"enabled"`
    MaxEndpoints int     `mapstructure:"max_endpoints"`
    Seed         *string `mapstructure:"seed"`
}

func (c BackendSubsetConfig) Validate(
    endpointHealth EndpointHealthConfig,
    logRouting LogRoutingConfig,
) error {
    if !c.Enabled {
        return nil
    }
    if c.MaxEndpoints <= 0 {
        return errors.New("backend_subset.max_endpoints must be greater than 0 when backend_subset.enabled=true")
    }
    if c.Seed != nil && strings.TrimSpace(*c.Seed) == "" {
        return errors.New("backend_subset.seed must be non-empty when set")
    }
    if !endpointHealth.Enabled {
        return errors.New("backend_subset requires endpoint_health.enabled=true")
    }
    if endpointHealth.ActiveProbe.Enabled {
        return errors.New("backend_subset is incompatible with endpoint_health.active_probe.enabled=true")
    }
    if !logRouting.IgnoreTraceID {
        return errors.New("backend_subset requires log_routing.ignore_trace_id=true")
    }
    return nil
}
```

Add `BackendSubset BackendSubsetConfig` to `Config`, call this validation before
`EndpointHealth.Validate`, and add a disabled zero-value in `createDefaultConfig`.
Add the schema definition with `seed` marked `x-pointer: true`.

- [ ] **Step 4: Write failing signal-constructor tests**

Add tests proving:

```go
cfg := simpleConfig()
enableBackendSubset(cfg, 32)

_, err := newLogsExporter(settings, cfg)
require.NoError(t, err)

_, err = newTracesExporter(settings, cfg)
require.ErrorContains(t, err, "backend_subset is only supported for logs")

_, err = newMetricsExporter(settings, cfg)
require.ErrorContains(t, err, "backend_subset is only supported for logs")
```

The helper must enable endpoint health and `LogRouting.IgnoreTraceID`.

- [ ] **Step 5: Add constructor guards**

Before creating telemetry or child factories:

```go
if cfg.(*Config).BackendSubset.Enabled {
    return nil, errors.New("backend_subset is only supported for logs")
}
```

Use this in traces and metrics. In logs, retain a defensive check:

```go
if c.BackendSubset.Enabled && !c.LogRouting.IgnoreTraceID {
    return nil, errors.New("backend_subset requires log_routing.ignore_trace_id=true")
}
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
go test ./exporter/loadbalancingexporter -run 'TestConfigValidateBackendSubset|Test.*Exporter.*BackendSubset' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add exporter/loadbalancingexporter/config.go \
  exporter/loadbalancingexporter/factory.go \
  exporter/loadbalancingexporter/config.schema.yaml \
  exporter/loadbalancingexporter/config_test.go \
  exporter/loadbalancingexporter/log_exporter.go \
  exporter/loadbalancingexporter/log_exporter_test.go \
  exporter/loadbalancingexporter/trace_exporter.go \
  exporter/loadbalancingexporter/trace_exporter_test.go \
  exporter/loadbalancingexporter/metrics_exporter.go \
  exporter/loadbalancingexporter/metrics_exporter_test.go
git commit -m "feat(loadbalancingexporter): validate bounded backend subsets" \
  -m "Refs: SAW-7944" \
  -m "Assisted-by: OpenAI Codex"
```

### Task 2: Deterministic host-keyed rendezvous selector

**Files:**
- Create: `exporter/loadbalancingexporter/backend_subset.go`
- Create: `exporter/loadbalancingexporter/backend_subset_test.go`

- [ ] **Step 1: Write failing selector property tests**

Cover:

```go
selector := backendSubsetSelector{seed: "gateway-1", maxEndpoints: 2}
got := selector.selectEndpoints([]string{
    "10.0.0.3:10417",
    "10.0.0.1:10417",
    "10.0.0.2:10417",
})
require.Len(t, got, 2)
```

Assert the result is identical for reversed input and for the same hosts on
port `10418`. Also assert `N <= K` returns all normalized endpoints, adding one
endpoint changes at most one slot, removing an unselected endpoint is a no-op,
removing a selected endpoint admits exactly the next ranked endpoint, and
different seeds produce more than one subset across a 100-seed simulation.

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./exporter/loadbalancingexporter -run 'TestBackendSubset' -count=1
```

Expected: build failure because `backendSubsetSelector` does not exist.

- [ ] **Step 3: Implement hostname seed resolution**

Add:

```go
func newBackendSubsetSelector(cfg BackendSubsetConfig) (*backendSubsetSelector, error) {
    if !cfg.Enabled {
        return nil, nil
    }
    seed := ""
    if cfg.Seed != nil {
        seed = strings.TrimSpace(*cfg.Seed)
    } else {
        hostname, err := os.Hostname()
        if err != nil {
            return nil, fmt.Errorf("resolve backend_subset seed from hostname: %w", err)
        }
        seed = strings.TrimSpace(hostname)
    }
    if seed == "" {
        return nil, errors.New("backend_subset seed is empty")
    }
    return &backendSubsetSelector{seed: seed, maxEndpoints: cfg.MaxEndpoints}, nil
}
```

- [ ] **Step 4: Implement stable host-keyed ranking**

Use:

```go
type backendSubsetRank struct {
    endpoint string
    score    [sha256.Size]byte
}

func backendSubsetHost(endpoint string) string {
    normalized := endpointWithPort(endpoint)
    host, _, err := net.SplitHostPort(normalized)
    if err != nil {
        return normalized
    }
    return strings.Trim(host, "[]")
}

func (s backendSubsetSelector) score(endpoint string) [sha256.Size]byte {
    return sha256.Sum256([]byte(s.seed + "\x00" + backendSubsetHost(endpoint)))
}
```

Normalize and deduplicate endpoints, sort ranks by descending score with full
endpoint as the deterministic collision tie-breaker, take `min(K, N)`, then sort
the returned endpoints for stable ring input.

- [ ] **Step 5: Add a 400-by-400 distribution regression**

Simulate 400 gateway seeds selecting 32 of 400 workers. Assert mean selections
is exactly 32, coefficient of variation is below `0.22`, and max-to-mean is
below `1.60`. Keep the bounds above the deterministic sample's observed values
so the test detects algorithm changes without becoming flaky.

- [ ] **Step 6: Run selector tests**

Run:

```bash
go test ./exporter/loadbalancingexporter -run 'TestBackendSubset' -count=10
```

Expected: PASS on every run.

- [ ] **Step 7: Commit**

```bash
git add exporter/loadbalancingexporter/backend_subset.go \
  exporter/loadbalancingexporter/backend_subset_test.go
git commit -m "feat(loadbalancingexporter): select stable backend subsets" \
  -m "Refs: SAW-7944" \
  -m "Assisted-by: OpenAI Codex"
```

### Task 3: Bound endpoint-health eligibility and fail-open

**Files:**
- Modify: `exporter/loadbalancingexporter/loadbalancer.go`
- Modify: `exporter/loadbalancingexporter/endpoint_health.go`
- Modify: `exporter/loadbalancingexporter/endpoint_health_test.go`

- [ ] **Step 1: Write failing endpoint-health tests**

Construct a manager with 10 present endpoints and `K=3`. Assert:

```go
eligible := manager.eligibleEndpoints()
require.Len(t, eligible, 3)
```

Quarantine all selected endpoints and assert fail-open returns three present
endpoints, never ten. Verify `pressureSnapshot.present`, `.eligible`, and
`.pressured` still describe the full membership before subsetting.

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./exporter/loadbalancingexporter -run 'TestEndpointHealth.*BackendSubset' -count=1
```

Expected: FAIL because endpoint health returns the full eligible or present set.

- [ ] **Step 3: Inject the selector once**

Resolve the selector in `newLoadBalancer` and pass it through
`endpointHealthSettings`:

```go
selector, err := newBackendSubsetSelector(oCfg.BackendSubset)
if err != nil {
    return nil, err
}
healthSettings := endpointHealthSettingsFromConfig(oCfg.EndpointHealth)
healthSettings.backendSubset = selector
```

Add:

```go
func (m *endpointHealthManager) selected(endpoints []string) []string {
    if m.settings.backendSubset == nil {
        return endpoints
    }
    return m.settings.backendSubset.selectEndpoints(endpoints)
}
```

- [ ] **Step 4: Apply the selector after fleet-level health calculation**

Keep sorting, `pressureSnapshot`, and `shouldFailOpenLocked` based on full
`present` and `eligible` slices. Change only the return:

```go
if failOpen {
    return m.selected(present), true, failOpenStarted
}
return m.selected(eligible), false, false
```

No caller performs its own subset filtering.

- [ ] **Step 5: Run endpoint-health tests**

Run:

```bash
go test ./exporter/loadbalancingexporter -run 'TestEndpointHealth' -count=1
```

Expected: PASS, including all pre-existing guardrail and pressure tests.

- [ ] **Step 6: Commit**

```bash
git add exporter/loadbalancingexporter/loadbalancer.go \
  exporter/loadbalancingexporter/endpoint_health.go \
  exporter/loadbalancingexporter/endpoint_health_test.go
git commit -m "feat(loadbalancingexporter): bound endpoint health decisions" \
  -m "Refs: SAW-7944" \
  -m "Assisted-by: OpenAI Codex"
```

### Task 4: Reconcile lifecycle strictly against the selected set

**Files:**
- Modify: `exporter/loadbalancingexporter/loadbalancer.go`
- Modify: `exporter/loadbalancingexporter/loadbalancer_test.go`

- [ ] **Step 1: Write failing lifecycle regressions**

Use a counting component factory and endpoint health with `K=2` to prove:

- 500 resolver endpoints create exactly two child exporters.
- Adding an unselected endpoint creates and drains zero exporters.
- Removing a selected endpoint creates one replacement and drains one exporter.
- Quarantine, replacement, recovery, and displacement always leave
  `len(lb.exporters) == 2`.
- Repeating the flap cycle ten times does not grow the exporter map.
- Mass quarantine fail-open never creates more than two exporters.
- `routableBackendCount()` remains two and central-queue reroutes choose only
  endpoints in the selected ring.

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./exporter/loadbalancingexporter -run 'TestLoadBalancerBackendSubset' -count=1
```

Expected: FAIL because resolver reconciliation removes against full membership
and success paths do not drain displaced exporters.

- [ ] **Step 3: Centralize health-selection commits**

Add:

```go
func (lb *loadBalancer) commitEndpointHealthSelectionLocked(
    created []createdExporter,
    replace map[string]struct{},
) (eligible []string, duplicates []createdExporter, removed []removedExporter) {
    eligible = lb.endpointHealth.eligibleEndpointsNoRefresh()
    lb.ring = newHashRing(eligible)
    for endpoint := range replace {
        endpoint = endpointWithPort(endpoint)
        if !createdExporterExists(created, endpoint) {
            continue
        }
        if exp, ok := lb.exporters[endpoint]; ok {
            exp.markStopping()
            delete(lb.exporters, endpoint)
            removed = append(removed, removedExporter{endpoint: endpoint, exporter: exp})
        }
    }
    duplicates = lb.installCreatedExportersLocked(created, eligible)
    removed = append(removed, lb.removeExtraExportersLocked(eligible)...)
    lb.refreshRoutableBackendCountLocked()
    return eligible, duplicates, removed
}
```

Use it from resolver reconciliation, transport and probe failures, transport
and probe success, and quarantine expiry. Failure paths pass the failed endpoint
in `replace` only when a forced replacement was created. Success paths pass
`nil`. Preserve synchronous-versus-asynchronous drain behavior at each caller.

- [ ] **Step 4: Run lifecycle and central-queue tests**

Run:

```bash
go test ./exporter/loadbalancingexporter \
  -run 'TestLoadBalancerBackendSubset|TestCentralQueue.*Backend' \
  -count=1
```

Expected: PASS.

- [ ] **Step 5: Run race-enabled churn**

Run:

```bash
go test -race ./exporter/loadbalancingexporter \
  -run 'TestLoadBalancerBackendSubset.*Churn|TestLoadBalancerShutdown' \
  -count=5
```

Expected: PASS with no race report or leaked goroutine.

- [ ] **Step 6: Commit**

```bash
git add exporter/loadbalancingexporter/loadbalancer.go \
  exporter/loadbalancingexporter/loadbalancer_test.go
git commit -m "fix(loadbalancingexporter): drain displaced subset exporters" \
  -m "Refs: SAW-7944" \
  -m "Assisted-by: OpenAI Codex"
```

### Task 5: Selected-set telemetry, documentation, and changelog

**Files:**
- Modify: `exporter/loadbalancingexporter/metadata.yaml`
- Regenerate: `exporter/loadbalancingexporter/internal/metadata/generated_telemetry.go`
- Regenerate: `exporter/loadbalancingexporter/internal/metadata/generated_telemetry_test.go`
- Modify: `exporter/loadbalancingexporter/loadbalancer.go`
- Modify: `exporter/loadbalancingexporter/loadbalancer_test.go`
- Modify: `exporter/loadbalancingexporter/README.md`
- Create: `.chloggen/saw-7944-bounded-backend-subsets.yaml`

- [ ] **Step 1: Add failing telemetry assertions**

After resolving ten endpoints with `K=3`, assert the new gauge records three.
After one selected endpoint is replaced, assert the displacement counter
increases by one. Initial population must not count as displacement.

- [ ] **Step 2: Define metrics**

Add metadata entries without endpoint or seed attributes:

```yaml
loadbalancer_num_selected_backends:
  enabled: true
  stability: development
  description: Current number of backends selected for routing.
  unit: "{backends}"
  gauge:
    value_type: int
loadbalancer_backend_subset_displacement_total:
  enabled: true
  stability: development
  description: Number of backends admitted by bounded subset replacement.
  unit: "{displacements}"
  sum:
    value_type: int
    monotonic: true
```

- [ ] **Step 3: Regenerate telemetry bindings**

Run:

```bash
make -C exporter/loadbalancingexporter mdatagen
```

Expected: generated telemetry files expose
`LoadbalancerNumSelectedBackends` and
`LoadbalancerBackendSubsetDisplacementTotal`.

- [ ] **Step 4: Record selected size and displacement**

Before replacing `lb.ring`, snapshot its endpoint set. After the commit, record
the selected count. When subset mode is enabled and the previous set is
non-empty, count newly admitted endpoints and add that count to the displacement
counter.

- [ ] **Step 5: Document the feature and release note**

Document the config, logs-only contract, hostname-derived seed, host-not-port
ranking, endpoint-health and active-probe requirements, fail-open cap, and
central-queue `num_consumers <= max_endpoints` rollout check.

Create:

```yaml
change_type: enhancement
component: exporter/loadbalancing
note: Add opt-in bounded backend subsets for affinity-free logs.
issues: [7944]
subtext: |
  Host-keyed rendezvous selection caps child OTLP transports while preserving
  endpoint-health replacement, fail-open bounds, and exporter draining.
change_logs: [user]
```

- [ ] **Step 6: Run metadata and component tests**

Run:

```bash
make -C exporter/loadbalancingexporter mdatagen
git diff --exit-code -- \
  exporter/loadbalancingexporter/internal/metadata/generated_telemetry.go \
  exporter/loadbalancingexporter/internal/metadata/generated_telemetry_test.go
(cd exporter/loadbalancingexporter && make test)
git diff --check
```

Expected: PASS and no generated diff beyond the expected telemetry files.

- [ ] **Step 7: Commit**

```bash
git add .chloggen/saw-7944-bounded-backend-subsets.yaml \
  exporter/loadbalancingexporter/README.md \
  exporter/loadbalancingexporter/metadata.yaml \
  exporter/loadbalancingexporter/internal/metadata/generated_telemetry.go \
  exporter/loadbalancingexporter/internal/metadata/generated_telemetry_test.go \
  exporter/loadbalancingexporter/loadbalancer.go \
  exporter/loadbalancingexporter/loadbalancer_test.go
git commit -m "docs(loadbalancingexporter): expose bounded subset telemetry" \
  -m "Refs: SAW-7944" \
  -m "Assisted-by: OpenAI Codex"
```

### Task 6: Full verification and exporter release handoff

**Files:**
- Verify only: `exporter/loadbalancingexporter/**`

- [ ] **Step 1: Format**

Run:

```bash
make -C exporter/loadbalancingexporter fmt
```

Expected: no unrelated files changed.

- [ ] **Step 2: Run the local equivalent of the Sawmills scoped gate**

Run:

```bash
make -C exporter/loadbalancingexporter lint test-twice
```

Expected: PASS. After the PR opens, the hosted required check named
`scoped-tests` must also pass on Linux and Windows.

- [ ] **Step 3: Run the component race suite**

Run:

```bash
go test -race ./exporter/loadbalancingexporter/... -count=1
```

Expected: PASS.

- [ ] **Step 4: Run autoreview**

Run:

```bash
autoreview --fast --thinking high --local \
  --parallel-tests "go test ./exporter/loadbalancingexporter/... -count=1"
```

Accept only findings verified in the real code. Fix verified defects, rerun the
focused regression, and stop after one clean review result.

- [ ] **Step 5: Verify branch state**

Run:

```bash
git status --short
git log --oneline origin/main..HEAD
git diff --check origin/main...HEAD
```

Expected: clean worktree, only SAW-7944 commits, no whitespace errors.

- [ ] **Step 6: Open the exporter PR**

Push the branch and open a PR titled:

```text
feat(loadbalancingexporter): bound affinity-free log backends
```

The body must link SAW-7944, include the 480,000-to-38,400 transport math,
Fable's five corrected failure modes, tests, dark default, and rollout gates.

- [ ] **Step 7: Release the merged module**

After merge, use the repository's module release workflow to publish:

```text
exporter/loadbalancingexporter/v0.149.0-sawmills.48
```

Do not manually tag. Verify the module proxy resolves `.48` before updating the
collector distribution.
