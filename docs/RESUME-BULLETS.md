# ShardVault Resume Bullets

All bullets below represent actual measured results and architectural implementations from the `shardvault` project. Each bullet is prefixed with `DRAFT:` and explicitly cites the corresponding benchmark row in [RESULTS.md](../bench/RESULTS.md).

---

- **DRAFT:** Architected and implemented **ShardVault**, a distributed S3-lite object store in Go utilizing Reed-Solomon erasure coding ($k=4, m=2$) with rendezvous (HRW) hashing, slashing storage overhead to **1.50×** compared to 3.00× for traditional 3× replication while tolerating any 2 arbitrary concurrent node outages. *(Cites: [RESULTS.md §3: Storage Overhead Measured](../bench/RESULTS.md#storage-overhead-measured))*

- **DRAFT:** Designed an atomic version commit protocol over embedded BBolt metadata, ensuring zero dirty reads or partial writes; proved crash consistency across **100 simulated `SIGKILL` crash cycles** mid-upload with zero corrupt version commits and 100% orphan shard reclamation via a background grace-period garbage collector. *(Cites: [RESULTS.md §7: Correctness, Fuzzing & Crash Resilience](../bench/RESULTS.md#7-correctness-fuzzing--crash-resilience))*

- **DRAFT:** Engineered speculative hedged reads and degraded reconstruction that recovers data shards on-the-fly upon shard timeout or CRC32C failure, maintaining **209.59 MB/s** degraded read throughput and **940.18 ms** p99 latency with 2 full storage nodes offline ($m=2$). *(Cites: [RESULTS.md §5: Degraded Reads](../bench/RESULTS.md#5-degraded-reads-2-storage-nodes-down))*

- **DRAFT:** Developed a multi-phase chaos test harness (`cmd/chaos`) and property-based state machine suite (`pgregory.net/rapid`) that validated end-to-end data integrity across 2,000 objects (**1.22 GB logical data**) under random node kills and in-place disk bitrot, achieving **100.00% byte-identical reads** and **0 wrong bytes** served. *(Cites: [RESULTS.md §3: Chaos Test Harness](../bench/RESULTS.md#3-chaos-test-harness-cmdchaos))*

- **DRAFT:** Implemented an end-to-end verification pipeline pairing a 54-byte binary shard header (CRC32C) with stripe-level SHA-256 checks; built a negative-control harness that proved silent bitrot detection by verifying that disabling gateway checks exposed 7 corrupted objects under identical workloads. *(Cites: [RESULTS.md §3: Negative Control](../bench/RESULTS.md#negative-control--no-verify))*

- **DRAFT:** Built a high-performance benchmarking tool (`cmd/loadgen`) achieving **289.65 MB/s** healthy GET throughput at 1 MiB and **32.67 MB/s** parallel write throughput across a 6-node erasure-coded cluster using 16 concurrent workers with strict `fsync` per shard. *(Cites: [RESULTS.md §4: Throughput & Latency Benchmarks](../bench/RESULTS.md#4-throughput--latency-benchmarks-cmdloadgen))*
