# SAW-10460: Backend Subsets with Active Probes

## Approved execution plan

Branch: `amiri/saw-10460-active-probe-subset-main`

Base: `origin/main` at
`1ffff943c11bae1fb2a30c059aa7755fcb68aa9b`.

Owner: one writer in the SAW-10460 worktree.

### 1. Preserve the decision

Write the design and this plan before code changes. Use Approach A:

1. Keep full resolver-present endpoint state for active probes.
2. Probe every resolver-present endpoint.
3. Use deterministic backend-subset selection for eligible endpoints.
4. Keep the ring and child exporters bounded by `max_endpoints`.
5. Drain displaced exporters through existing lifecycle code.

### 2. RED: tests before production code

Change only the fenced tests.

- Change the backend-subset validation case so the approved combination is
  accepted.
- Keep a case that rejects `backend_subset` when
  `endpoint_health.enabled=false`.
- Add behavior tests for:
  - configuration acceptance;
  - active probes visiting all resolver-present endpoints;
  - selected child exporters and ring staying at or below `max_endpoints`
    during probe failure and recovery;
  - exact deterministic healthy-candidate displacement and drain;
  - failed non-selected endpoints not creating or materializing child
    exporters;
  - deterministic recovery without exporter leaks;
  - bounded fail-open behavior;
  - safe shutdown during an in-flight probe.
- Use channels and events for synchronization. Do not use sleeps where an
  event can prove state.

Run the focused configuration test first from the component module:

```shell
cd exporter/loadbalancingexporter
go test . -run 'TestConfigValidateBackendSubset/active_probe_enabled' -count=1
```

Record the expected validation failure. Then run the runtime safety tests
against unchanged production code. Stop if any runtime test fails.

### 3. GREEN: smallest production change

Delete only the active-probe incompatibility return from
`BackendSubsetConfig.Validate` in `exporter/loadbalancingexporter/config.go`.

Do not change endpoint-health requirements or `ignore_trace_id` requirements.
Run `gofmt` on the touched Go files.

### 4. Documentation and changelog

- Update `exporter/loadbalancingexporter/README.md` to state that active probes
  visit every resolver-present endpoint while only the deterministic selected
  set owns child exporters and the ring.
- Add `.chloggen/saw-10460-backend-subset-active-probe.yaml` for the supported
  configuration combination.

### 5. Verification

Run, from `exporter/loadbalancingexporter`:

```shell
go test . -run 'TestConfigValidateBackendSubset|TestLoadBalancerBackendSubset|TestLoadBalancerEndpointHealthActiveProbe' -count=1
go test -race . -run 'TestLoadBalancer.*ActiveProbe.*Shutdown|TestLoadBalancer.*ActiveProbe.*Concurrency|TestLoadBalancer.*BackendSubset.*Probe' -count=1
go test .
make lint
```

Run repository-defined changelog and component checks that cover the changed
files. Capture raw pass counts and any warnings. Check the exact file fence,
`git diff`, `git status`, branch, and HEAD.

### 6. Review and commit

Freeze the diff to these files only:

- `docs/superpowers/specs/2026-08-17-saw-10460-backend-subset-active-probe-design.md`
- `docs/superpowers/plans/2026-08-17-saw-10460-backend-subset-active-probe.md`
- `exporter/loadbalancingexporter/config.go`
- `exporter/loadbalancingexporter/config_test.go`
- `exporter/loadbalancingexporter/loadbalancer_test.go`
- `exporter/loadbalancingexporter/README.md`
- `.chloggen/saw-10460-backend-subset-active-probe.yaml`

Run the repository autoreview skill before the non-trivial commit. Verify each
finding in the real code. Fix only reproducible in-fence findings, with no
more than two fix cycles.

Commit with a Conventional Commit subject and these trailers:

```text
Assisted-by: Claude Fable 5
Assisted-by: GPT-5.6 Luna
```

Do not push, create a PR, merge, release, deploy, mutate Linear or PagerDuty,
use Kubernetes, or take customer runtime action.
