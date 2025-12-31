# OTTL Bytecode VM (OVM) Implementation Plan

Status: Phase 6 Complete (performance optimizations)
Owner: Sawmills.ai / OTel Collector Contrib
Scope: Replace AST-walking OTTL interpreter with stack-based bytecode VM, zero-alloc hot loop, safe execution bounds.

## Implementation Status (2025-12-30)

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0: Foundation | ✓ Complete | ir pkg, Value, Instruction, micro-VM |
| Phase 1: Core VM | ✓ Complete | All ops, gas, errors, stack pool |
| Phase 2: Compiler | ✓ Complete | AST→bytecode, folding, disasm |
| Phase 3: pdata Bridge | ✓ Core done | Fast attrs + key direct fields; remaining fields deferred |
| Phase 4: Stdlib | ✓ Core Complete | VM callsites (incl list args), native Int/IsMatch/IsNil/IsType/IsMap/IsList, PMap/PSlice VM values, dynamic IsMatch cache |
| Phase 5: Verification | ✓ Complete | Shadow mode, fuzz harness, docs |
| **Phase 6: Performance** | **✓ Complete** | Superinstructions, inline closures, VMGetter devirt |

## Current Perf Snapshot (2025-12-31)

- RealWorld benchmark (Apple M3 Max):
  - Interpreter: 1431 ns/op, 610 B/op, 21 allocs
  - VM: **227 ns/op**, 0 B/op, 0 allocs
  - Speedup: **6.3x**
- Hotspots (pprof, RealWorld VM): dispatch loop 59%, mapaccess2_faststr 11%, aeshashbody 7%.
- PGO: bench-profiled build held at 227 ns/op (no gain); skip PGO.
- Recent perf wins (all landed): VMAttrGetter fast path, static path VMGetterProvider, nested map flattening, StandardStringLikeGetter fast paths, attr-index lookup, compare superinstructions.

	## Phase 7: Production Readiness (Next Steps)

	**Status:** 🚧 In Progress
	**Goal:** Ensure safety, observability, and smooth rollout.

	### 1. Safety & Stability (The "Don't Crash" Rule)
	- [ ] **Fuzzing Gate**: 24h differential fuzz in CI. Owner: amir. Target: 2026-01-05.
	- [x] **Stack Limit Check**: Deep recursion test to confirm `maxStack` enforcement. Owner: amir. Target: 2026-01-03. (Done 2025-12-31)
	- [x] **Gas Limit Verification**: Infinite-loop script triggers `ErrGasExhausted`. Owner: amir. Target: 2026-01-03. (Done 2025-12-31)

	### 2. Observability
	- [x] **Metrics**: Add counters for execution, errors, and histogram for latency. Owner: amir. Target: 2026-01-06. (Done 2025-12-31)
	  - `ottl_vm_execution_count`
	  - `ottl_vm_error_count{type}`
	  - `ottl_vm_execution_time`
	- [x] **Logs**: Ensure error logs include rule names/indices for debugging. Owner: amir. Target: 2026-01-06. (Done 2025-12-31)

	### 3. Debuggability
	- [x] **Shadow Mode**: Implement `Compare(ctx, input)` dual-run + divergence log. Owner: amir. Target: 2026-01-08. (telemetry counter added 2025-12-31)
	- [ ] **Disassembler**: Verify `Program.String()` output readability. Owner: amir. Target: 2026-01-04.

	### 4. Integration
	- [ ] **Collector Integration**: Wire VM default path into `transformprocessor`, `filterprocessor`. Owner: amir. Target: 2026-01-09.
	- [ ] **Configuration**: Expose `gas_limit` in processor config. Owner: amir. Target: 2026-01-04.

	### 5. Documentation
	- [ ] **Feature Flag**: Document `OTELCOL_OTTL_VM_ENABLED=true`. Owner: amir. Target: 2026-01-04.
	- [ ] **Limitations**: Document known semantic differences (e.g. float precision). Owner: amir. Target: 2026-01-04.

	## 1. Goals


- **2x-10x throughput** for complex OTTL logic
- **0 allocs/op** for arithmetic/logic/compare operations
- **Deterministic safety** via gas metering
- **Semantic parity** with legacy interpreter

## 2. Non-Goals (Initial)

- Full rewrite of pdata model
- Perfect performance for all stdlib functions (phase-in)
- Removal of legacy interpreter (kept for dual-run + fallback)
- Register-based VM or JIT (future consideration)

## 3. Architecture Decisions

### 3.1 Execution Model

- Stack-based VM, linear bytecode array
- Pre-allocated stack (`sync.Pool` reuse)
- Constant pool for string/numeric literals
- Gas metering in VM loop

### 3.2 Value Struct (Tagged Union)

24-byte struct to avoid Go interface boxing in hot loop. Trades size for simplicity—no bit-packing, direct field access.

```go
// ir/value.go
type Type uint8

const (
    TypeNone Type = iota
    TypeInt
    TypeFloat
    TypeBool
    TypeString // Ptr → *string in StringPool
    TypeBytes  // Ptr → []byte in ConstPool
    TypePMap   // Ptr → pdata.Map
    TypePSlice // Ptr → pdata.Slice
)

type Value struct {
    Type Type   // 1 byte
    _    [7]byte// padding
    Num  uint64 // int64, float64 (via math.Float64bits), bool (0/1)
    Ptr  unsafe.Pointer // non-nil only for pointer types
}
```

**Size:** 24 bytes (1 + 7 padding + 8 + 8). Larger than 16B but avoids union complexity.

**String representation:**
- Strings live in `Program.Strings []string` (interned, immutable)
- `Value.Ptr` points to the `string` header in that slice
- No length field needed; Go string carries its own length

**Invariants:**
- `Ptr` is non-nil only when `Type ∈ {TypeString, TypeBytes, TypePMap, TypePSlice}`
- `Ptr` only points to:
  - `Program.Strings` entries (compile-time, immutable)
  - pdata handles (request-scoped, outlives VM execution)
- Helper methods (`AsInt()`, `AsFloat()`, `AsString()`, `AsPMap()`) validate `Type` before access
- GC-safe: pointers reference long-lived data only

### 3.3 Instruction Format

32-bit fixed-width instructions for alignment and fast decoding:

```go
type Opcode uint8

type Instruction uint32

// Encoding: [opcode:8][arg:24] — big-endian layout
// Max arg value: 16,777,215 (2^24 - 1)

func (i Instruction) Op() Opcode   { return Opcode(i >> 24) }
func (i Instruction) Arg() uint32  { return uint32(i) & 0x00FFFFFF }

func Encode(op Opcode, arg uint32) Instruction {
    return Instruction(uint32(op)<<24 | (arg & 0x00FFFFFF))
}
```

**Arg usage by opcode:**
- `LOAD_CONST`: index into `Program.NumConsts` or `Program.Strings`
- `JUMP*`: absolute instruction offset
- `LOAD_ATTR_CACHED`: index into `Program.Accessors`

### 3.4 Program Structure

```go
type Program struct {
    Code      []Instruction   // bytecode
    NumConsts []uint64        // numeric constants (int64/float64 bits)
    Strings   []string        // interned string literals (immutable)
    Accessors []PathAccessor  // pre-compiled pdata accessors
    GasLimit  uint64          // default gas budget
}
```

**Constant loading:**
- `LOAD_CONST_NUM <idx>`: push `Value{Type: TypeInt/Float, Num: NumConsts[idx]}`
- `LOAD_CONST_STR <idx>`: push `Value{Type: TypeString, Ptr: &Strings[idx]}`

**Concurrency:**
- `Program` is immutable after compilation; safe for concurrent use
- `PathAccessor` instances are read-only closures; goroutine-safe

## 4. Phases & Work Items

### Phase 0: Foundation + Micro-VM Benchmark

**Deliverable:** Proof of speed + no allocs

| Task | Description |
|------|-------------|
| Create `ir` package | `Opcode` (uint8), `Instruction`, `Value` tagged union |
| Implement micro-VM | `LOAD_CONST`, `ADD`, `EQ` only; simple stack |
| Benchmark | Legacy `Eval` vs micro-VM for `1 + 2 == 3` |

**Exit Criteria:** ≥2x speedup, 0 allocs/op

## Benchmarks (Local, 2025-12-28, Apple M3 Max)

- OTTL add/eq: interpreter 11.22 ns/op (0 allocs), VM 13.27 ns/op (0 allocs), microVM 17.01 ns/op (0 allocs)
- OTTL float mul/eq: interpreter 17.09 ns/op (1 alloc, 8 B), VM 13.18 ns/op (0 allocs), microVM 18.61 ns/op (0 allocs)
- OTTL path eq (`attributes["foo"] == 7`): interpreter 7.166 ns/op (0 allocs), VM 32.42 ns/op (0 allocs)
- OTTL complex where (two attr compares + math): interpreter 23.00 ns/op (0 allocs), VM 39.01 ns/op (0 allocs)
- OTTL deep arithmetic: interpreter 41.59 ns/op (0 allocs), VM 13.26 ns/op (0 allocs), microVM 58.50 ns/op (0 allocs), microVM specialized 28.79 ns/op (0 allocs)
- OTTL many comparisons: interpreter 34.56 ns/op (0 allocs), VM 13.24 ns/op (0 allocs), microVM 55.16 ns/op (0 allocs)
- OTTL microVM add/eq specialized: 13.76 ns/op (0 allocs)
- ctxutil mixed literal key path (map→slice→map): 62.38 ns/op (2 allocs, 112 B)
- IsMatch literal: interpreter 72.58 ns/op (0 allocs), VM 80.36 ns/op (0 allocs)
- IsMatch dynamic: interpreter 1074 ns/op (22 allocs, 2245 B), VM 118.4 ns/op (0 allocs)
- IsInt: interpreter 10.75 ns/op (0 allocs), VM 15.71 ns/op (0 allocs)
- IsMap: interpreter 30.33 ns/op (1 alloc, 16 B), VM 30.34 ns/op (0 allocs)
- IsList: interpreter 26.41 ns/op (1 alloc, 16 B), VM 30.49 ns/op (0 allocs)
- **StringEquality: interpreter 7.45 ns/op (0 allocs), VM 37.86 ns/op (0 allocs)** ← 5.1x gap
- **MultipleStringChecks: interpreter 35.7 ns/op (0 allocs), VM 87.2 ns/op (0 allocs)** ← 2.4x gap

### Phase 1: Core VM Runtime

**Deliverable:** Executes arithmetic/logic/compare + control flow

| Task | Description |
|------|-------------|
| VM loop | `switch` dispatch, instruction pointer, stack pointer |
| Stack | Fixed-size `[]Value`, bounds checks, `sync.Pool` reuse |
| Math ops | `ADD`, `SUB`, `MUL`, `DIV` |
| Logic ops | `AND`, `OR`, `NOT` |
| Compare ops | `EQ`, `NEQ`, `LT`, `LTE`, `GT`, `GTE` |
| Control flow | `JUMP`, `JUMP_IF_FALSE`, `JUMP_IF_TRUE` |
| Gas metering | See §6 for detailed rules |
| Error model | See §5 |

### Phase 2: Compiler + Tooling

**Deliverable:** AST → bytecode + disassembler

Status: `pkg/ottl/ottlc` exposes compiler helpers; `pkg/ottl/vm` provides disassembly.

| Task | Description |
|------|-------------|
| `ottlc` package | `Compile(ast) (*Program, error)` |
| Flatten AST | Recursive tree → linear instruction list |
| Jump patching | Label resolution for conditionals/loops |
| Constant pool | String interning + numeric literals table |
| Disassembler | `Disassemble(*Program) string` → human-readable dump |

**Example output:**
```
000 LOAD_CONST    0    ; "http.method"
001 GET_ATTR      1
002 LOAD_CONST    2    ; "GET"
003 EQ
004 JUMP_IF_FALSE 008
...
```

### Phase 3: pdata Bridge (Critical Perf)

**Deliverable:** Fast field access + transform integration

The slowest part of OTTL is `attributes["key"]` lookups. We optimize via path compilation.

| Task | Description |
|------|-------------|
| Path compilation | Pre-hash keys at compile time |
| PathAccessor table | `[]PathAccessor` in `Program`, stable indices |
| Accessor struct | Closure/struct capturing how to read/write a path |
| pdata opcodes | `LOAD_ATTR_CACHED <id>`, `SET_ATTR_CACHED <id>` |
| Field opcodes | `GET_BODY`, `SET_BODY`, resource/span/log/metric fields |
| Integration | Connect VM context to transform processor |

**Concurrency model:**
- `Program` and accessors are read-only, safe across goroutines
- VM instances are per-evaluation, not shared

#### Phase 3 Tracking (Current)
Completed (as of 2025-12-29):
- Fast attr getters/setters wired across contexts (log/span/resource/scope/metric/datapoint/spanevent/profile/profile-sample)
- Fast literal-key attribute access (OpLoadAttrFast) and setter fast path (OpSetAttrFast)
- Direct field opcodes:
  - log: body, severity_number, time_unix_nano
  - log: observed_time_unix_nano, severity_text, flags
  - span: name, start/end_time_unix_nano, kind, status.code, status.message
  - span: trace_id.string, span_id.string, parent_span_id.string, trace_state, dropped_* counts
  - span: trace_id/span_id/parent_span_id (raw IDs)
  - metric: name, unit, type
  - metric: description, aggregation_temporality, is_monotonic
  - resource: dropped_attributes_count
  - resource: schema_url
  - scope: name, version, dropped_attributes_count
  - scope: schema_url
  - span: trace_state key access

Remaining (deferred to future iteration):
- Direct field opcodes:
  - none
- Bench coverage for remaining direct opcodes
- OpSetAttrCached never emitted by compiler (VM impl exists; wire when needed)

**Phase 3 Status: Core complete.** High-value fields implemented; remaining fields are low-frequency access patterns. Effort: ~1-3 days if needed.

### Phase 4: Stdlib + Native Ops

**Deliverable:** Support existing OTTL function library

| Task | Description |
|------|-------------|
| Function registry adapter | VM callsites for scalar getter args (Value → interface boundary) |
| Native opcodes | `INT`, `IS_NIL`, `IS_MATCH` as fast-path instructions |
| Gradual migration | Only perf-critical functions become native ops |

**Notes:**
- Added `IS_TYPE` and dynamic `IS_MATCH` (pattern on stack).
- `IsMap`/`IsList` now mapped to `IS_TYPE` once pcommon.Map/Slice landed in VM.

**Progress (2025-12-29):**
- Callsite adapter supports list args (prebuilt slices; VM stack only for non-list args).
- VM now carries pcommon.Map/Slice values without per-call heap wrapping.
- Stack reuse via pool holders to avoid per-eval allocations.
- Dynamic IsMatch regex cache (per-program) + native IsMap/IsList.

**Adapter pattern:**
```go
func wrapLegacyFunc(fn OTTLFunc) VMFunc {
    return func(vm *VM, args []Value) (Value, error) {
        ifaces := make([]any, len(args))
        for i, v := range args {
            ifaces[i] = v.ToInterface() // only here
        }
        result, err := fn(ifaces...)
        return ValueFrom(result), err
    }
}
```

### Phase 5: Verification + Rollout

**Deliverable:** Semantic parity + safe rollout

| Task | Description |
|------|-------------|
| Differential fuzzing | Legacy vs VM parity; Go fuzz harness |
| CI gate | N CPU-hours (suggest 24h) with zero divergences |
| Dual-run mode | Shadow execute both engines; log divergences |
| Feature flag | `OTELCOL_OTTL_VM=true` (default false) |
| Gradual default | Read-only processors first, then mutation |
| Metrics | Divergence count, gas exhaustion rate, ns/op |

---

## Phase 6: VM Performance Optimizations

**Status:** ✓ Complete
**Goal:** Close the performance gap vs interpreter for simple string/path comparisons

### Optimizations Implemented (2025-12-30)

| # | Optimization | Commit | Impact |
|---|-------------|--------|--------|
| P0 | Fast runner for 1-inst programs | `7decc97` | Inline superinst execution |
| P1 | VMGetter devirtualization | `7decc97` | Eliminate interface dispatch |
| P2 | 2-instruction fast path | `16a6bb0` | OpLoadAttrCached + OpEqConst |
| P3 | Inline int/string comparison | `552ba3f` | Skip compareOp dispatch |
| P4 | Inline superinst into BoolExpr | `eff1004` | Bypass runBoolVM entirely |

### Key Techniques

1. **Superinstruction fusion** (`c197c42`): Fuse `load + compare` into single opcodes like `OpAttrFastEqConstString`
2. **Fast runner** (`7decc97`): For 1-2 instruction programs, execute inline in `RunWithStackAndContext` without VM loop
3. **BoolExpr inlining** (`eff1004`): For common patterns, create direct closure evaluators that bypass all VM machinery (stack pool, runBoolVM, ir.Value conversion)

### Future Optimizations (Not Implemented)

These were considered but not needed after BoolExpr inlining achieved near-parity:

1. **Per-run attribute cache**: Cache attribute values within single evaluation for repeated access patterns
2. **Branch superinstructions**: Fuse `load + compare + branch` into single opcode for filter expressions
3. **Stack allocation**: Eliminate sync.Pool for small programs (already achieved via inline closures)

---

### Phase 6 Results

| Benchmark | Before | After | Improvement | vs Interpreter |
|-----------|--------|-------|-------------|----------------|
| StringEquality_VM | 37 ns | **8 ns** | **78% faster** | ~1.1x (near parity) |
| PathEq_VM (int) | 23 ns | **10.3 ns** | **55% faster** | ~1.4x slower |
| PathEq_VMGetter | 17 ns | **4.3 ns** | **75% faster** | **1.7x faster!** |

### Complex Expressions (VM wins)

| Benchmark | Interpreter | VM | Winner |
|-----------|-------------|-----|--------|
| DeepArithmetic | 42 ns | 14 ns | VM **3x faster** |
| ManyComparisons | 37 ns | 14 ns | VM **2.7x faster** |
| DeepBooleanChain | 36 ns | 14 ns | VM **2.6x faster** |
| ManyNots | 48 ns | 15 ns | VM **3.2x faster** |
| IsMatchDynamic | 1164 ns | 123 ns | VM **9.4x faster** (0 allocs) |

---

## 5. Error Model

VM returns structured errors (not panics) for:

| Error | Trigger |
|-------|---------|
| `ErrTypeMismatch` | Opcode expects int, got string |
| `ErrDivideByZero` | `DIV` with zero divisor |
| `ErrStackUnderflow` | Pop from empty stack |
| `ErrStackOverflow` | Push exceeds max depth |
| `ErrInvalidOpcode` | Unknown opcode byte |
| `ErrGasExhausted` | Gas counter hits zero |
| `ErrInvalidPath` | Accessor ID out of bounds |

**Behavior on error:**
- Rule execution stops; error returned to processor
- Processor logs at debug/info level
- Span/log is NOT dropped (configurable per-processor)
- Metric incremented: `ottl_vm_errors_total{type="..."}`

## 6. Gas Metering

**Metering strategy:** Hybrid (per-instruction baseline + backward-jump penalty)

```go
// In VM loop:
gas--
if gas == 0 {
    return ErrGasExhausted
}
if isBackwardJump(inst) {
    gas -= backwardJumpCost
}
```

| Parameter | Default | Configurable |
|-----------|---------|--------------|
| `GasLimit` | 10,000 | Yes, per-program or global cap |
| Base cost | 1 per instruction | Fixed |
| Backward jump penalty | +9 (total 10) | Fixed |

**Rationale:** Backward jumps indicate loops; penalizing them bounds iteration count without complex analysis.

**Exhaustion handling:**
- Returns `ErrGasExhausted` (deterministic, not panic)
- Logs warning with rule identifier
- Increments `ottl_vm_gas_exhausted_total`

## 7. Test Plan

| Test Type | Scope |
|-----------|-------|
| Micro-VM benchmark | Phase 0 exit gate |
| Unit tests | VM ops, stack, gas, error paths |
| Compiler tests | AST → bytecode snapshots |
| Disassembler golden tests | Human-readable output stability |
| pdata access tests | Accessor correctness + perf |
| Fuzz tests | Legacy parity (semantic equivalence) |
| Integration tests | Real-world OTTL rules from existing configs |

## 8. Performance Plan

| Metric | Tracking |
|--------|----------|
| `ns/op` | Benchmark per phase; interpreter vs VM |
| `allocs/op` | Must be 0 for core ops |
| pdata access | Cached accessors vs runtime lookup |
| Regression guard | CI fails if perf degrades >10% |

**Benchmarks:**
- Simple arithmetic: `1 + 2 == 3`
- Complex logic: `a > 0 and b < 100 or c == "foo"`
- pdata heavy: `attributes["http.method"] == "GET"`
- Real rules: representative transform processor configs

## 9. Next Steps (GA Readiness)

1. Observability: metrics + error logs landed (2025-12-31); verify in CI.
2. Hardening: stack/gas tests done (2025-12-31); 24h differential fuzz gate pending; finish by 2026-01-05.
3. Debuggability: shadow divergence metric added (2025-12-31); remaining: `Compare(ctx,input)` helper + readable disassembler; target 2026-01-08.
4. Integration: enable VM path in transform/filter processors; add `gas_limit` config knob; finish by 2026-01-09.
5. Docs: feature flag + limitations page; finish by 2026-01-04.

## 10. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| pdata lookup dominates runtime | VM gains erased | Path compilation + cached accessors (Phase 3, done) |
| GC pressure from Value slices | Latency spikes | Stack reuse via `sync.Pool`; Ptr only to long-lived data |
| Debuggability of bytecode | Hard to troubleshoot | Disassembler required; tracing mode (future) |
| Semantic drift | Silent bugs | Differential fuzzing gate; dual-run shadow mode |
| Gas config abuse | DoS via huge limits | Global cap; warn on excessive values |

## 11. Artifacts

| Artifact | Description |
|----------|-------------|
| `ir` package | Opcodes, Value, Program, Instruction |
| `vm` package | Runtime execution loop |
| `ottlc` package | Compiler (AST → bytecode) |
| `ottl-disasm` | CLI disassembler |
| Fuzz harness | Differential fuzzing infrastructure |
| Feature flag | `OTELCOL_OTTL_VM` environment variable |
| Shadow mode | Dual-run with divergence logging |
| Docs update | User guide for VM mode |

## 12. Success Criteria

| Milestone | Criteria |
|-----------|----------|
| Phase 0 complete | ≥2x speedup, 0 allocs on micro-benchmark |
| Phase 1 complete | All core ops pass unit tests; gas works |
| Phase 2 complete | Compiler handles 100% of parser test cases |
| Phase 3 complete | pdata access matches interpreter; no extra allocs |
| Phase 4 complete | All stdlib functions callable from VM |
| Phase 5 complete | 24h fuzz with 0 divergences; shadow mode in prod |
| **Phase 6 complete** | **String comparisons at parity or faster than interpreter** |
| GA ready | Default on for read-only processors |

## 13. Future Considerations

Revisit if profiling shows bottlenecks:
- **Compact Value union** (Plan B style) if GC scanning is hot
- **Register-based VM** if stack overhead significant
- **JIT compilation** for ultra-hot programs
- **Bytecode tracing** for debugging complex rules

## Benchmarks (Local, 2025-12-29, MixedPath Update)
- OTTL mixed path (mock): interpreter 7.33 ns/op (0 allocs), VM 32.40 ns/op (0 allocs)
  *Note: VM overhead is higher here because we are not using the cached accessor optimization in the mock benchmark, so it pays the generic getter adapter cost.*
