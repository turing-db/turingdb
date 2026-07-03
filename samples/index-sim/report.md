# Indexing a versioned graph — five designs under 2,000 commits

A cost-model comparison of five ways to maintain a string-property index over TuringDB's
immutable, git-like commit history, as parts/commits scale **1 → 2,000**.

> **There is no single winner.** Picking an index is a *routing decision* on two properties of
> each indexed column: how its reads arrive (HEAD vs historical, point vs range) and how it
> churns (cardinality × update rate). Each design wins a different corner.

This is the prose counterpart of the interactive `report.html`. Both are generated from
`index_sim` in this directory — reproduce with `g++ -std=c++23 -O2 -o index_sim index_sim.cpp && ./index_sim`.

## Setup

- **Workload:** 100,000 distinct dictionary codes (keys), 32 writes/commit, 1 → 2,000 commits (parts), uniform key churn.
- **Cost model (fixed estimated ns):** cache-miss hop `80`, node alloc `60`, CAS `20`, mutex lock `25`, hash `5`, compare `2`, Bloom probe `30`.
- **It is a cost model, not a benchmark.** The workload is *executed* so operation counts (tree depth, layers walked, parts collected, versions skipped) reflect the real access distribution; each operation is then charged a fixed nanosecond cost. Concurrency wait time is *derived* from those constants and a writer count — no threads are spawned. The shapes are the message; retune the constants for your hardware.

## The five designs

| # | Design | Read at HEAD | Submit | Notes |
|---|--------|--------------|--------|-------|
| 1 | **HAMT per commit** | `O(log₃₂)` | path-copy per write | new root per commit; untouched subtries shared |
| 2 | **COW B-tree per commit** | `O(log₃₂)` | path-copy per write | int dictionary-code key → free range scans |
| 3 | **Multiversion hash (global)** | `O(1)` | prepend version + concurrency control | one structure for all versions; must lock or be lock-free |
| 4 | **COW hash layers per commit** | `O(layers)`, stop at newest | build one small layer | cheapest writes; read walks the stack |
| 5 | **Per-DataPart index, collect at read** | `O(parts)`, probe all + merge | build one small per-part index | LSM/ClickHouse model; TuringDB's existing per-part model |

## Results

### Read latency at HEAD — point lookup

| design | @ 2,000 parts | scaling in parts |
|--------|--------------:|------------------|
| Multiversion hash | **165 ns** | flat — O(1) slot probe |
| HAMT | 328 ns | flat — depth ramps 1→4 then holds |
| COW B-tree | 360 ns | flat |
| Per-DataPart, compacted (K=64) | 5.5 µs | flat after K |
| Per-DataPart, + Bloom/zone-map prune | 60 µs | rises, O(parts) |
| COW layers | 79 µs | rises, O(parts) |
| Per-DataPart, naive collect | **170 µs** | rises, O(parts) |

The three "logical map" designs are independent of history; the two per-commit-local designs climb
linearly with parts. Pruning lowers the constant; only compaction changes the slope (→ O(K)).

### Submit latency — apply one commit (32 writes)

| design | @ 2,000 parts | note |
|--------|--------------:|------|
| Per-DataPart / COW layers | ~2.1 µs | cheapest — append one small map |
| Multiversion hash (uncontended) | 4.5 µs | prepend 32 version nodes |
| HAMT | 18.2 µs | path-copy allocation, ramps with depth |
| COW B-tree | 19.2 µs | widest nodes |
| Per-DataPart, compacted | 12.8 µs | **write amplification** from merging (see below) |

All flat in part count. The COW trees pay allocation on every write; the per-commit-local designs just append.

### Retained memory after 2,000 parts

| design | memory |
|--------|-------:|
| COW layers / Per-DataPart | 1.6 MiB |
| Multiversion hash | 2.7 MiB |
| HAMT | 13.1 MiB |
| COW B-tree | 78.8 MiB (widest nodes) |
| *naive full copy per commit* | *2,161 MiB* |

Structural sharing is the whole game: every sharing design stays in the low tens of MiB vs the ~2.1 GiB a naive full copy would cost.

### Write concurrency — multiversion submit vs concurrent writers (derived)

The multiversion table is the only *global* structure, so commits contend on it.

| writers | global mutex | striped ×256 | lock-free CAS |
|--------:|-------------:|-------------:|--------------:|
| 1 | 4.5 µs | 5.3 µs | 2.6 µs |
| 64 | **166 µs** | 5.9 µs | 2.6 µs |

A single mutex serializes (linear in writers). Striping nearly erases it. Lock-free is flat — until a hot key (`--hot`) turns every write into a CAS-retry storm.

### Read concurrency — reader latency vs concurrent writers

| design | @ 64 writers | shape |
|--------|-------------:|-------|
| HAMT / COW B-tree | 328 / 360 ns | **flat** — wait-free |
| COW layers / Per-DataPart | 79 µs / 170 µs | **flat** — wait-free (at base cost) |
| Multiversion, lock-free | 165 ns | **flat** — wait-free |
| Multiversion, global mutex | **164 µs** | rises — as slow as the per-part fan-out |

Every copy-on-write / persistent design publishes a new version with a single atomic root/manifest swap, so its reads are **wait-free** regardless of writers. Only the in-place-mutable multiversion table contends — and only under a lock. **Reader/writer contention is a property of in-place mutation, not of versioning.**

### Time-travel — as-of read latency vs commits behind HEAD

The ranking **reverses** when you read into the past:

| design | at HEAD | 2,000 commits back |
|--------|--------:|-------------------:|
| Multiversion hash | **165 ns** (best) | **164 µs** (worst) — filters every newer version |
| Per-DataPart collect | 170 µs (worst) | **95 ns** (best) — only old parts existed |
| COW layers | 79 µs | 85 ns |
| HAMT / COW B-tree | ~330 ns | ~85 ns (flat-ish; smaller old tree) |

Persistent trees jump to a retained root (history free). Per-part/layers touch only the parts live at the snapshot, so they get *cheaper* into the past. The multiversion table must filter past every newer version, so it gets *costlier*. **What wins the present loses the past.** (The multiversion line is the worst case — a key updated every commit; a stable, low-cardinality key stays flat.)

### Per-DataPart deep-dive — collect, prune, compact

| @ 2,000 parts | read | submit |
|---------------|-----:|-------:|
| collect all parts (naive) | 170 µs | 2.1 µs |
| collect + per-part Bloom/zone-map prune | 60 µs | 2.1 µs |
| **compacted to K=64 live parts** | **5.5 µs (flat)** | **12.8 µs** |

Pruning lowers the read constant but keeps the `O(parts)` slope. **Compaction** is the only thing that flattens reads (→ `O(K)`), bought with **write amplification** (tiered merge rewrites each entry ~log₂(parts/K) times). A version-preserving store can't discard old parts the way ClickHouse does, so compaction buys less here than there.

## Global conclusions

**No single winner.** The choice routes on *how reads arrive* (HEAD vs historical, point vs range) × *how the column churns* (cardinality × update rate).

### Decision matrix

| Design | Strongest at | Falls over when | Reach for it when |
|--------|--------------|-----------------|-------------------|
| **Multiversion hash** | O(1) HEAD reads; smallest memory | historical/as-of reads (version filtering), hot keys, coarse write locks | low-cardinality, stable, HEAD-read properties — with lock-free or striped writes |
| **COW B-tree** | balanced read+write; free range scans on dict codes | per-write allocation; heaviest memory | ordered or range queries |
| **HAMT** | balanced; flat in history; as-of as cheap as HEAD | per-write allocation | the safe default when the workload is unknown |
| **Per-DataPart, compacted** | cheapest writes; tiny memory; cheap historical reads | reads O(parts) without compaction; compaction adds write amplification | write-heavy, scan-tolerant columns — TuringDB's existing per-part model |
| **COW layers** | cheapest writes | reads walk the whole stack, O(parts) | append-mostly data, paired with Bloom filters |

### Cross-cutting laws

1. **Global structures buy flat reads** — but the only mutable global one (multiversion hash) pays in write contention unless it goes lock-free.
2. **Local structures buy cheap writes** and tiny memory — but a read fans out across all parts, `O(parts)`.
3. **Compaction is the only slope-killer** — it bounds read fan-out to `O(K)`, traded for write amplification; a version-preserving store can't merge history away, so it buys less here.
4. **Time-travel inverts the ranking** — persistent trees stay flat, per-part/layer reads get cheaper into the past, the multiversion table gets costlier. What wins HEAD loses history.
5. **Wait-free reads come from copy-on-write, not versioning** — a single atomic version-publish makes readers wait-free; only the in-place-mutable multiversion table must go lock-free to avoid reader/writer contention.

### Recommendation for TuringDB

One structure should not index every property. **Route by cardinality × churn:**

- **Low-cardinality, stable string properties** (status, type, label — the "identical mostly everywhere" case) → a **multiversion structure keyed by dictionary code**, lock-free or striped. Flat 165 ns HEAD reads and ~MiB memory from shared, rarely-rewritten posting lists; version filtering bites only on the rare as-of read, and a stable key's chain is short.
- **High-cardinality or high-churn properties** → **per-DataPart local indexes** (what TuringDB already builds) plus **version-preserving compaction** to bound fan-out and **zone-map / Bloom pruning** to cut the constant.
- **Range or ordered access** → a **COW B-tree** per commit; dictionary codes are ints, so range scans come free.
- **Unknown or mixed workload** → the **persistent trees** (HAMT / COW B-tree) are the conservative default — logarithmic and flat on both reads and writes, history free to query.

Compaction is the universal lever, but it must be **version-preserving** — which is exactly why the global multiversion structure stays attractive for the stable, low-cardinality slice, and why per-part fan-out is a harder problem here than in a store that can merge history away.

## Method & caveats

- **Fixed-cost model.** Op counts come from executing the workload; latencies from the constants above. Retune with flags (`--hop`, `--alloc`, `--cas`, `--lock`, …) and rerun.
- **Uniform churn.** Real property workloads are skewed; the stable/low-cardinality case helps the multiversion table further (shorter chains), and a Zipfian read mix softens the per-part read curve.
- **HEAD reads by default.** The headline read/submit sweeps assume HEAD reads; the time-travel section sweeps the as-of dimension explicitly.
- **Multiversion as-of is worst case.** The rising as-of/version-filter line assumes a key updated every commit (chain depth = snapshot lag); a stable key stays flat.
- **Compaction is version-preserving.** The compacted line bounds *live* parts; as-of reads still touch the older parts retained for history. Per-part pruning assumes a near-zero Bloom false-positive rate.

## Files

- `index_sim.cpp` — the simulator (`--help` for flags).
- `report.html` — interactive charts (same content as this document).
- `results_sweep.csv`, `results_concurrency.csv`, `results_mvcc_reads.csv` — raw curves.
- `README.md` — build/run and a condensed bottom line.
