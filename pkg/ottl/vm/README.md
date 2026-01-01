# OTTL Bytecode VM

The OTTL VM is a stack-based bytecode engine that replaces the AST-walking interpreter for hot paths. It delivers ~6x speedup on the RealWorld benchmark with zero allocations when fast attribute accessors are wired.

## What it is
- Stack VM: fixed-width opcodes, const pool, zero alloc hot loop.
- Safety: gas metering (`DefaultGasLimit`), stack bounds, error codes.
- Observability: metrics (`ottl_vm_execution_count`, `ottl_vm_error_count{type}`, `ottl_vm_execution_time`, `ottl_vm_shadow_divergence_total{type}`) emitted when a MeterProvider is available.
- Shadow mode friendly: you can dual-run interpreter + VM and count divergences.

## When to use
- Transform/filter processors that already use OTTL conditions/statements.
- Workloads dominated by attribute reads/compares (logs/metrics/traces/profiles).

## Enabling
- Feature flag: `OTELCOL_OTTL_VM=true` (when parsers are built with `ottl.WithVMEnabledFromEnv[...]`).
- Programmatic: pass `ottl.WithVMEnabled[Ctx]()` to the parser options to force VM on.
- Gas: `ottl.SetDefaultVMGasLimit(limit)` (per-process) or use per-processor `vm_gas_limit` config and call it during config validation.

## Wire-up checklist (for a processor)
1. Parser options:
   - `ottl.WithVMEnabledFromEnv[Ctx]()` (or `WithVMEnabled`).
   - `ottl.WithVMAttrGetter/Setter` + `WithVMAttrContextNames` for the contexts you support (log/span/metric/resource/scope/profile/datapoint/spanevent).
   - Direct pdata getters if you use the fast field opcodes (e.g., `WithVMLogRecordGetter`, `WithVMSpanGetter`, `WithVMMetricGetter`, `WithVMResourceGetter`, `WithVMScopeGetter`).
2. Config: accept `vm_gas_limit`; on load, if >0 call `ottl.SetDefaultVMGasLimit` before building parsers.
3. Telemetry: ensure your processor wiring enables metrics export; the VM metrics are emitted automatically by the parser collection when a MeterProvider is present.
4. Tests/benches: run `go test ./...` and `go test -bench=OTTLComparisonRealWorld_VM -run=^$ ./...` (from `pkg/ottl`).
5. Fuzz (optional): `go test -run=^$ -fuzz=FuzzDifferentialVM -fuzztime=1h` to guard interpreter/VM parity.

## Example (logs parser)
```go
parser, err := ottllog.NewParser(functions, settings,
    ottllog.EnablePathContextNames(),
    ottl.WithVMEnabledFromEnv[ottllog.TransformContext](),
    ottl.WithVMAttrGetter[ottllog.TransformContext](logAttrGetter),
    ottl.WithVMAttrSetter[ottllog.TransformContext](logAttrSetter),
    ottl.WithVMAttrContextNames[ottllog.TransformContext]([]string{"log"}),
    ottl.WithVMLogRecordGetter[ottllog.TransformContext](func(tCtx TransformContext) plog.LogRecord { return tCtx.log }),
    ottl.WithVMResourceGetter[ottllog.TransformContext](func(tCtx TransformContext) pcommon.Resource { return tCtx.resource }),
    ottl.WithVMScopeGetter[ottllog.TransformContext](func(tCtx TransformContext) pcommon.InstrumentationScope { return tCtx.scope }),
)
```

## Caveats
- Keep interpreter fallback available for unsupported ops; `WithVMEnabledFromEnv` + feature flag is the safest rollout path.
- Don’t commit large fuzz corpora; keep seeds in the fuzz cache to avoid repo bloat.
- Path-key caches beyond the existing fast getters showed no win in current profiling (mapaccess2_faststr ~11%); skip unless new evidence.

## Perf snapshot (Apple M3 Max, 2026-01-01)
- RealWorld benchmark: ~225 ns/op VM vs ~1390 ns/op interpreter (~6x speedup), 0 allocs.

## Gas (safety budget)
- Each opcode costs 1 gas; execution stops with `ErrGasExhausted` at zero.
- Default: 10,000. Set process-wide via `ottl.SetDefaultVMGasLimit`, or per-processor `vm_gas_limit` (0 = default).
- Purpose: bound runaway/expensive rules (e.g., deep boolean chains, heavy regex), keep tail latency predictable.
- Ops tip: start with default; raise modestly (20–50k) for complex rule sets; avoid unbounded values.
