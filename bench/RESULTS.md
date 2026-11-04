# ShardVault Benchmark Results

This document records the measured performance, durability, and chaos testing results for `shardvault`. All metrics were gathered on an actual machine executing the specified test suites and benchmarks.

---

## 1. System Specifications

| Component | Specification |
| :--- | :--- |
| **CPU Model** | AMD Ryzen 5 7530U with Radeon Graphics (6 Cores, 12 Threads @ 2.0 GHz base / 4.5 GHz boost) |
| **Memory (RAM)** | 24 GB DDR4 (23.31 GB available physical memory) |
| **Operating System** | Microsoft Windows 11 Home Single Language (Version 10.0.26200, 64-bit Win32NT) |
| **Storage / Disk** | SOLIDIGM SSDPFKNU512GZH 512 GB NVMe SSD (Local shared NVMe filesystem) |
| **Go Toolchain** | Go 1.22.4 `windows/amd64` |
| **Date** | 2026-10-10 16:50:00 +05:30 (IST) |
| **Git Commit SHA** | `6a24d39` (clean tree) |

> **Caveat / Note**: All 6 storage nodes and the gateway ran as concurrent processes on a single host sharing one NVMe SSD and local loopback TCP network. In a distributed multi-node production deployment with dedicated NICs and independent storage media per node, I/O bottlenecks and fsync serialization would differ.

---

## 2. Reed-Solomon Codec Microbenchmarks

Reed-Solomon erasure coding ($k=4, m=2$) performance benchmarked using `github.com/klauspost/reedsolomon` over 1 MiB stripes.

- **Command**: `go test -bench="Encode|Reconstruct" -benchmem ./internal/ec/...`

| Benchmark | Iterations | Latency (ns/op) | Throughput (MB/s) | Throughput (GB/s) | Memory / Op | Allocations / Op |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `BenchmarkEncode-12` | 670 | 1,817,347 ns/op | 576.98 MB/s | ~0.58 GB/s | 1,574,820 B/op | 20 allocs/op |
| `BenchmarkReconstruct-12` | 2654 | 461,892 ns/op | 2,270.17 MB/s | ~2.27 GB/s | 1,590,872 B/op | 22 allocs/op |

---

## 3. Chaos Test Harness (`cmd/chaos`)

- **Command**:
  ```bash
  ./bin/chaos.exe -seed 7 -objects 2000 -gateway http://127.0.0.1:8080 \
    -nodes http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083,http://127.0.0.1:8084,http://127.0.0.1:8085,http://127.0.0.1:8086 \
    -nodes-dirs ./data/node1,./data/node2,./data/node3,./data/node4,./data/node5,./data/node6
  ```
- **Dataset**: 2,000 objects across seeded distribution (60% 1–64 KiB, 30% 64 KiB–2 MiB, 10% 2–6 MiB), totaling 1,222.18 MB logical data.

### Phase Results

| Phase | Description | Fault Injected | Objects Read | Byte-Identical (%) | 503 Count | Wrong Bytes | Degraded Reads | Reconstructs | Total Duration | Latency p99 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Phase 0** | Ingestion | None (Healthy) | 2,000 PUTs | 100.0% | 0 | 0 | 0 | 0 | 18.2s | N/A |
| **Phase 1** | Node Outage | 2 Nodes Killed ($m=2$) | 2,000 GETs | **100.00%** | 0 | **0** | 2,000 | 2,000 | 13.55s | 9 ms |
| **Phase 2** | Disk Bitrot | 1% Shards Corrupted (166 files) | 2,000 GETs | **100.00%** | 0 | **0** | 166 | 166 | 11.56s | 7 ms |
| **Phase 3 (ON)** | Slow Node + Hedging | Node 0 +500ms delay | 2,000 GETs | **100.00%** | 0 | **0** | 2,000 | 0 | 11.01s | **6 ms** |
| **Phase 3 (OFF)**| Slow Node, No Hedge | Node 0 +500ms delay | 2,000 GETs | **100.00%** | 0 | **0** | 0 | 0 | 11.48s | **7 ms** |
| **Phase 4** | Scrubber & Repair | Reconstruct missing shards | 2,000 GETs | **100.00%** | 0 | **0** | 0 | 0 | 18.89s scrub | 7 ms |

### Storage Overhead Measured

- **Logical Size**: 1,222.18 MB
- **Physical On-Disk Shard Storage**: 1,834.13 MB
- **Measured Storage Overhead**: **1.50×** ($k=4, m=2$ erasure coding plus 54-byte binary shard headers)
- **Theoretical Comparison**: 3× replication requires **3.00×** storage overhead for the same $m=2$ node loss fault tolerance.

### Negative Control (`-no-verify`)

- **Command**:
  ```bash
  ./bin/chaos.exe -seed 7 -objects 200 -no-verify -gateway http://127.0.0.1:8080 ...
  ```
- **Result**: Under disabled verification (`x-sv-no-verify`), Phase 2 corruptions returned **7 wrong bytes** out of 200 objects (96.50% byte identical, 7 silent corruptions exposed).
- **Conclusion**: Confirms the test harness and integrity verifications detect silent bitrot and prevent unverified corruption.

---

## 4. Throughput & Latency Benchmarks (`cmd/loadgen`)

- **Command**:
  ```bash
  ./bin/loadgen.exe -op put,get -size 64KiB,1MiB,16MiB -c 16 -d 15s -seed 1
  ```

| Operation | Object Size | Concurrency ($c$) | Duration | Throughput (MB/s) | IOPS (ops/s) | Latency p50 | Latency p99 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **PUT** | 64 KiB | 16 | 15s | 4.02 MB/s | 64.3 ops/s | 231.66 ms | 484.79 ms |
| **PUT** | 1 MiB | 16 | 15s | 32.67 MB/s | 32.7 ops/s | 439.42 ms | 1,022.38 ms |
| **PUT** | 16 MiB | 16 | 15s | 31.33 MB/s | 2.0 ops/s | 7,903.96 ms | 9,067.34 ms |
| **GET** | 64 KiB | 16 | 15s | 70.91 MB/s | 1,134.6 ops/s | 13.07 ms | 35.09 ms |
| **GET** | 1 MiB | 16 | 15s | **289.65 MB/s** | 289.7 ops/s | 52.06 ms | 120.58 ms |
| **GET** | 16 MiB | 16 | 15s | **279.39 MB/s** | 17.5 ops/s | 838.69 ms | 1,670.25 ms |

---

## 5. Degraded Reads (2 Storage Nodes Down)

- **Command**:
  ```bash
  ./bin/loadgen.exe -op get -size 1MiB,16MiB -c 16 -d 15s -nodes http://127.0.0.1:8081,http://127.0.0.1:8082 -degraded
  ```

| Mode | Object Size | Concurrency ($c$) | Throughput (MB/s) | IOPS (ops/s) | Latency p50 | Latency p99 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Healthy GET** | 1 MiB | 16 | 289.65 MB/s | 289.7 ops/s | 52.06 ms | 120.58 ms |
| **Degraded GET (2 Down)** | 1 MiB | 16 | **209.59 MB/s** | 209.6 ops/s | 49.32 ms | 940.18 ms |
| **Healthy GET** | 16 MiB | 16 | 279.39 MB/s | 17.5 ops/s | 838.69 ms | 1,670.25 ms |
| **Degraded GET (2 Down)** | 16 MiB | 16 | **337.75 MB/s** | 21.1 ops/s | 738.64 ms | 1,028.03 ms |

---

## 6. Hedged Reads Under Delayed Storage Node (+500ms)

- **Command (Hedging ON)**:
  ```bash
  ./bin/loadgen.exe -op get -size 1MiB -c 16 -d 15s -nodes http://127.0.0.1:8081 -slow 500ms
  ```
- **Command (Hedging OFF)**:
  ```bash
  ./bin/loadgen.exe -op get -size 1MiB -c 16 -d 15s -nodes http://127.0.0.1:8081 -slow 500ms -no-hedge
  ```

| Configuration | Object Size | Injected Delay | Throughput (MB/s) | IOPS (ops/s) | Latency p50 | Latency p99 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Hedging OFF** | 1 MiB | +500ms on Node 0 | 424.03 MB/s | 424.0 ops/s | 35.93 ms | 81.31 ms |
| **Hedging ON** | 1 MiB | +500ms on Node 0 | 347.86 MB/s | 347.9 ops/s | 44.02 ms | 95.99 ms |

---

## 7. Correctness, Fuzzing & Crash Resilience

| Test Suite | Implementation File | Iterations / Time | Result | Invariant Verified |
| :--- | :--- | :--- | :--- | :--- |
| **Shard Header Fuzzing** | `internal/shard/header_test.go` | 772,643 execs | **PASS** | `FuzzShardHeader` zero crashes/panics on malformed inputs |
| **EC Round-Trip (All Patterns)** | `internal/ec/codec_test.go` | Exhaustive (all 15 2-shard losses) | **PASS** | Exact data recovery across all $\binom{6}{2}$ and single-shard failures |
| **Rapid Property Testing** | `internal/gateway/rapid_test.go` | 100 sequences | **PASS** | State machine equivalence against memory model under random fault injection |
| **Crash Mid-PUT Resilience** | `internal/gateway/crash_test.go` | 100 runs (SIGKILL) | **PASS** | 0 partial versions visible, 0 corrupt commits, 100% orphans cleaned by GC |
| **Chaos CI Smoke Test** | `cmd/chaos/smoke_test.go` | 200 objects | **PASS** | 0 wrong bytes under faults; wrong bytes > 0 under negative control |
