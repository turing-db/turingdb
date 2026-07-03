# index-sim

A cost-model simulator that compares five ways of maintaining a **versioned string-property
index** over TuringDB's immutable, git-like commit model, as the number of parts/commits
scales from 1 to 2000.

It answers: *what happens to read latency and submit (commit) latency as history grows?*

**The full findings — every measured number, the decision matrix, and the recommendation — are in
[`report.md`](report.md)** (prose) and `report.html` (interactive charts). This file is build/run docs.

A follow-up study, **[`report_hamt.md`](report_hamt.md)**, drills into one question the first report
raised: *for string keys, how far can the HAMT's read/submit/memory gap to the lock-free multiversion
hash be closed without losing the structural wins?* It adds the string front-end, folded (transient)
commits, a fan-out sweep, CHAMP, and a persistent Adaptive Radix Tree (ART) to the model.

Because that is still a cost model, **[`index_bench.cpp`](index_bench.cpp)** builds the three core
structures (multiversion hash, HAMT, ART) for real and times them; **[`report_bench.md`](report_bench.md)**
reports the measured-vs-modeled comparison. Short version: the model's structural conclusions hold
(time-travel and long-key reads favor the tries) but its absolute latencies and short-key read ranking
do not — see that file.

```bash
g++ -std=c++23 -O2 -o index_bench index_bench.cpp
./index_bench                  # correctness-checked, then real ns for read / submit / memory + a
                               # snapshot-lag sweep (reading a past snapshot under submit activity)
./index_bench --key-len 100    # long keys — where ART's no-hashing advantage shows
./index_bench --churned 256    # smaller churned set => deeper version chains => steeper as-of cost
```

## The five approaches

| # | Approach | Read at HEAD | Submit | Notes |
|---|----------|--------------|--------|-------|
| 1 | **HAMT per commit, structural sharing** | `O(log₃₂ keys)` | path-copy `O(log₃₂ keys)` per write | new root per commit, untouched subtries shared |
| 2 | **COW B-tree per commit, dictionary-code key** | `O(log₃₂ keys)` | path-copy per write | int code key → cheap compares + range scans |
| 3 | **Multiversion hash table, global** | `O(1)` (slot → newest version) | prepend version + concurrency control | one structure for all versions; must lock or be lock-free |
| 4 | **COW layers of hash tables per commit** | `O(layers walked)`, stop at newest | build one small layer | cheapest submit; read fan-out grows with parts |
| 5 | **Per-DataPart index, collect at read** | `O(parts)`, probe all + merge | build one small per-part index | LSM/ClickHouse model; same write path as #4 but reads every part. Modeled naive, with a per-part Bloom/zone-map prune, and **compacted** (bound live parts to `--compact-parts K`: read → `O(K)`, submit pays write amplification) |

## It is a cost model, not a wall-clock benchmark

The workload is *executed* so that operation counts reflect the real access distribution —
tree depth grows as distinct keys accumulate, and the **layers walked** by approach 4 comes
from the actual "commits since this key was last written" distribution. But each operation
is then charged a **fixed estimated cost in nanoseconds** rather than timed:

| constant | default (ns) | meaning |
|----------|-------------|---------|
| `--hop` | 80 | one pointer chase that misses cache and hits DRAM |
| `--alloc` | 60 | allocate + construct one node |
| `--cas` | 20 | one uncontended atomic compare-and-swap |
| `--lock` | 25 | one uncontended mutex lock + unlock |
| (hash 5, compare 2, bloom 30) | | per-op costs, see `CostModel` in the source |

**No real threads are spawned.** The concurrency cost of the global multiversion table is
*derived* from these constants plus a writer-thread count (`--threads`), under three policies:

- **global mutex** — the commit's critical section serializes behind the other writers ⇒ wait `∝ threads`
- **striped locks** — collide only on a shared stripe ⇒ wait `∝ threads / stripes`
- **lock-free CAS** — no wait under uniform keys; retries explode only on a hot key (`--hot`)

The multiversion **reads** are modeled too (the flat `O(1)` read is only the HEAD, lock-free best case):
**version filtering** — a reader on an older snapshot skips one hop + tag check per version newer than its
snapshot, so a stale or hot-key read is `O(chain depth)`; and **reader contention** — lock-free reads are
wait-free against any number of writers, but under a global mutex a read blocks behind in-flight commits and
gets as contended as the writes. Both are emitted to `results_mvcc_reads.csv`.

## Build & run

```bash
g++ -std=c++23 -O2 -o index_sim index_sim.cpp     # clang++ works too

./index_sim                       # defaults: 2000 parts, 100k keys, 32 writes/commit
./index_sim --threads 16          # derive wait time for 16 concurrent writers
./index_sim --hot 0.05            # 5% of writes hit one hot key (stresses lock-free)
./index_sim --bloom               # add a per-layer Bloom filter to approach 4
./index_sim --key-len 100         # 100-byte string keys (drives the string front-end; see report_hamt.md)
./index_sim --writes 1000         # bulk commits — folding (transient) submit win grows
./index_sim --hop 120 --alloc 90  # retune to your machine's estimates
./index_sim --help
```

Outputs summary tables + a memory comparison to stdout, and full curves to
`results_sweep.csv`, `results_concurrency.csv`, `results_mvcc_reads.csv` (the five-design study) plus
`results_hamt_variants.csv`, `results_fanout.csv`, `results_string_frontend.csv` (the HAMT study).

## How to read the results (defaults)

- **Read latency** — HAMT/B-tree ramp slightly (depth 1→4) then flatten; multiversion is
  flat (`O(1)` HEAD read); the two per-commit-local strategies grow with parts. **COW layers**
  (stop at newest) reaches ≈80 µs at 2000 parts; the **per-DataPart index** (collect across
  every part) is unconditional `O(parts)` — ≈170 µs naive, ≈60 µs with a per-part Bloom/zone-map
  prune. **Compaction** (bounding live parts to K=64) is the only thing that changes the slope:
  reads flatten at ≈5.5 µs, paid for by submit climbing 2.1 → ≈12.8 µs (write amplification).
- **Submit latency** — layers cheapest, multiversion next, HAMT/B-tree higher (path-copy
  allocations). All flat in part count.
- **Memory** — structural sharing keeps every approach in the low tens of MiB vs the
  ~2 GiB a naive full-copy-per-commit would cost.
- **Concurrency** — global-mutex submit grows linearly with writers; striped stays nearly
  flat; lock-free is flat until a hot key makes it retry.

The point is the *shapes*, not the absolute nanoseconds — retune the constants for your hardware.

## Bottom line

No design wins on every axis — picking one is a routing decision on two properties of each indexed
column: **how its reads arrive** (HEAD vs historical, point vs range) and **how it churns** (cardinality ×
update rate). Three laws fall out:

- **Global structures buy flat reads**, but the only mutable global one (multiversion hash) pays in write
  contention unless it's lock-free — and its flat read is only the HEAD, lock-free, low-chain best case.
- **Per-part local structures buy cheap writes** and tiny memory, but reads fan out `O(parts)`; only
  **compaction** bounds that, trading read fan-out for write amplification.
- **Persistent trees** (HAMT / COW B-tree) make history free to read (as-of = HEAD), at a per-write
  allocation cost — the conservative default for an unknown workload.

Two read-side findings round it out. **Reader/writer contention is a property of in-place mutation, not
versioning**: every copy-on-write / persistent design publishes a version with one atomic swap, so reads
are wait-free (flat in writer count) — only a lock-based multiversion table contends. And **time-travel
inverts the ranking**: reading into the past, persistent trees stay flat, per-part/layer reads get *cheaper*
(fewer parts existed then), but the multiversion table gets *costlier* (more versions to filter) — what wins
HEAD loses history.

For TuringDB: route by cardinality × churn — a lock-free **multiversion** index (keyed by dictionary code)
for low-cardinality, stable, HEAD-read string properties; **per-DataPart** local indexes + **version-preserving
compaction** + zone-map/Bloom pruning for high-cardinality or high-churn; a **COW B-tree** where range scans
matter. Compaction must be version-preserving, which is exactly why the global multiversion structure stays
attractive for the stable, low-cardinality slice. The full verdict, with the decision matrix, is in `report.html`.
