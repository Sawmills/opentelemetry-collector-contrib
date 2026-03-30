# SAW-6831 Upgrade Contrib To v0.148.0 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rebase the Sawmills contrib fork from the current `v0.140.x` baseline to upstream `v0.148.0`, preserving only the Sawmills patches that are still required.

**Architecture:** Treat upstream `v0.148.0` as the new source of truth. Replay the Sawmills delta as a small set of intentional patches on top, instead of dragging forward the old merge history. Regenerate builder outputs and distribution reports from the new upstream baseline, then run targeted and repo-level verification to expose upgrade regressions.

**Tech Stack:** Go, OpenTelemetry Collector Contrib, OCB builder, chloggen, Make, GitHub Actions

---

### Task 1: Audit the current fork delta

**Files:**

- Modify: `docs/plans/2026-03-27-saw-6831-upgrade-contrib-to-v0-148-0.md`
- Inspect: `versions.yaml`
- Inspect: `cmd/otelcontribcol/builder-config.yaml`
- Inspect: `cmd/oteltestbedcol/builder-config.yaml`
- Inspect: `exporter/loadbalancingexporter/**`
- Inspect: `processor/hotreloadprocessor/**`
- Inspect: `processor/logstometricsprocessor/**`

**Step 1: Identify the Sawmills-only commits after the last upstream merge**

Run: `git log --oneline a120226cd6..HEAD`
Expected: only the `SAW-6556`, `SAW-6744`, and `SAW-6764` follow-up commits remain on top of the old upstream merge commit.

**Step 2: Split intentional patches from accidental upgrade debris**

Run: `git diff --name-only a120226cd6..HEAD`
Expected: the real product delta concentrates in `exporter/loadbalancingexporter/**`, while broad module/go.sum churn is mostly old-branch dependency fallout.

**Step 3: Compare upstream churn in the patched areas**

Run: `git diff --name-status v0.140.1..v0.148.0 -- exporter/loadbalancingexporter processor/hotreloadprocessor processor/logstometricsprocessor cmd/otelcontribcol/builder-config.yaml cmd/oteltestbedcol/builder-config.yaml versions.yaml reports/distributions/contrib.yaml`
Expected: heavy upstream movement in `loadbalancingexporter` and version manifests, confirming that replaying a narrow patch set onto `v0.148.0` is safer than reusing the old generated state.

### Task 2: Re-root the work on upstream v0.148.0

**Files:**

- Modify: `versions.yaml`
- Modify: `cmd/otelcontribcol/builder-config.yaml`
- Modify: `cmd/oteltestbedcol/builder-config.yaml`
- Modify: `reports/distributions/contrib.yaml`

**Step 1: Create an execution branch from upstream `v0.148.0`**

Run: `git switch -c amiri/saw-6831-v0.148.0-replay v0.148.0`
Expected: working tree now matches upstream release `0.148.0`.

**Step 2: Verify the new baseline before replay**

Run: `sed -n '1,40p' versions.yaml`
Expected: `module-sets.contrib-base.version` and related builder pins read `v0.148.0`.

**Step 3: Regenerate release-owned artifacts after replay if needed**

Run: `make genotelcontribcol && make genoteltestbedcol`
Expected: generated builder manifests stay in sync with the new baseline plus Sawmills patches.

### Task 3: Port the Sawmills loadbalancing patches

**Files:**

- Modify: `exporter/loadbalancingexporter/config.go`
- Modify: `exporter/loadbalancingexporter/config_test.go`
- Modify: `exporter/loadbalancingexporter/factory.go`
- Modify: `exporter/loadbalancingexporter/log_exporter.go`
- Modify: `exporter/loadbalancingexporter/metrics_exporter.go`
- Modify: `exporter/loadbalancingexporter/trace_exporter.go`
- Modify: `exporter/loadbalancingexporter/README.md`
- Create or modify: `exporter/loadbalancingexporter/log_batcher.go`
- Create or modify: `exporter/loadbalancingexporter/log_batcher_test.go`
- Create or modify: `exporter/loadbalancingexporter/payload_codec.go`
- Create or modify: `exporter/loadbalancingexporter/payload_codec_test.go`
- Create or modify: `exporter/loadbalancingexporter/wrapped_exporter.go`

**Step 1: Re-apply the queue payload and log batching behavior against upstream**

Run: `git cherry-pick -n d5bed5a81f 07c10a8e35`
Expected: conflicts only in the exporter package or nearby docs/tests.

**Step 2: Manually reconcile upstream changes**

Run: `git status --short`
Expected: conflict set is limited to `exporter/loadbalancingexporter/**` and possibly the changelog/docs files.

**Step 3: Drop obsolete old-branch dependency fallout**

Run: `git restore --source=v0.148.0 -- .codecov.yml .github/ISSUE_TEMPLATE .checkapi.yaml .github/workflows/build-and-test.yml processor/hotreloadprocessor processor/logstometricsprocessor receiver/datadoglogreceiver exporter/kedascalerexporter reports/distributions/contrib.yaml`
Expected: only intentional `loadbalancingexporter` and release-note changes remain.

### Task 4: Reconcile metadata, changelog, and release notes

**Files:**

- Modify: `.chloggen/*.yaml`
- Modify: `docs/plans/2026-03-27-saw-6831-upgrade-contrib-to-v0-148-0.md`

**Step 1: Keep upgrade and Sawmills patch notes**

Run: `ls .chloggen | sort | rg 'upgrade-contrib-version|saw-6556|saw-6744|saw-6764'`
Expected: upgrade note plus the still-relevant Sawmills notes are present.

**Step 2: Write rollout-risk notes for the handoff**

Run: `git log --oneline v0.140.1..v0.148.0 -- exporter/loadbalancingexporter`
Expected: concrete upstream changes available for the final risk summary.

### Task 5: Verify the upgraded fork

**Files:**

- Test: `exporter/loadbalancingexporter/...`
- Test: repo root generated outputs

**Step 1: Run focused exporter tests first**

Run: `go test ./exporter/loadbalancingexporter/...`
Expected: new batching and compression tests pass on the upgraded exporter.

**Step 2: Run generation and API surface checks**

Run: `make genotelcontribcol && make genoteltestbedcol && make checkapi`
Expected: generated builder files and public API checks are clean.

**Step 3: Run the repo gates most likely to catch upgrade fallout**

Run: `make -j2 gotest GROUP=exporter && make -j2 gotest GROUP=processor && make -j2 gotest GROUP=other`
Expected: no regressions in the touched groups.

**Step 4: Capture final scope**

Run: `git status --short && git diff --stat`
Expected: diff is dominated by the upstream version bump, builder manifests, and the intentional Sawmills exporter patch set.

## Rollout Risks

- `loadbalancingexporter` changed upstream across the entire upgrade window, including queue batch wiring, k8s resolver defaults, routing-key support for metrics attributes, and generated metadata; keep focused regression coverage on this exporter.
- Upstream `v0.141.0` through `v0.148.0` removed or renamed some contrib components and changed the distribution contents in `versions.yaml` and builder configs; downstream consumers that assume the old component set need validation during release testing.
- Queue payload compression now depends on upstream `xexporterhelper` queue-batch wiring plus the Sawmills codec wrapper; validate both in-memory and persistent-queue paths with `snappy` and `zstd`.
- `logstometricsprocessor` ports cleanly to `v0.148.0` after updating it to the newer pointer-based OTTL parser/context API, so it can move into this fork as a standalone contrib module.
- `hotreloadprocessor` is not a standalone move: it statically imports `csvenrichmentprocessor`, `filtersamplingprocessor`, `logaggregationprocessor`, `redactprocessor`, `standardattributesprocessor`, and `throughputprocessor`, and those modules still depend on pre-`v0.148.0` OTTL/filter internals. Moving `hotreloadprocessor` fully into this fork therefore requires a second migration wave for that processor stack, not just copying two directories.
