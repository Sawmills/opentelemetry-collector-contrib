# LB Central Queue Dynamic Lanes Evidence

Date: 2026-05-22

Package: `exporter/loadbalancingexporter`

## Red Proof

Policy tests were added before implementation and failed to compile because the lane policy types did not exist:

```text
undefined: centralQueueLanePolicy
undefined: centralQueueLaneInputs
```

Baseline benchmark on `origin/main` for the many-lane/many-item candidate path:

```text
BenchmarkCentralQueueCollectManyLanesManyItems-10  825287-936311 ns/op  2403992-2403993 B/op  727 allocs/op
```

## Green Proof

Local test gate:

```text
go test . -count=1
ok github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter 5.201s
```

Final benchmark after bucketed queue storage, selected-window materialization, and per-bucket candidate scratch:

```text
BenchmarkCentralQueueCollectManyLanesManyItems-10  56656-71857 ns/op  19068-19070 B/op  7 allocs/op
```

Compared with the red baseline:

- CPU time per candidate collection improved by roughly 12x at the median.
- Allocated bytes per operation dropped from about 2.4 MiB to about 19 KiB.
- Allocations per operation dropped from 727 to 7.

## Pprof

Profiles captured locally:

```text
/tmp/lb-central-queue-dynamic-lanes.cpu.pb.gz
/tmp/lb-central-queue-dynamic-lanes.heap.pb.gz
/tmp/lb-central-queue-dynamic-lanes.cpu.top.txt
/tmp/lb-central-queue-dynamic-lanes.heap.top.txt
```

The remaining benchmark CPU is concentrated in per-bucket window scanning. Heap allocation from per-item candidate index growth was removed by reusing bucket-local candidate index scratch.
