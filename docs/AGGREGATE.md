# Aggregation — implementation study

Background research for implementing **grouped (`GROUP BY`) aggregation** in TuringDB.
This document is the literature / systems study: how the operator works, the design
space, and what ClickHouse, Kùzu, and Neo4j actually do. The closest existing analog
in our engine (a multi-row pipeline breaker) is [`ORDER_BY.md`](ORDER_BY.md).

Scope: in-memory, column-oriented, chunk-at-a-time execution — the regime TuringDB
operates in.

---

## 1. What an aggregation operator does

A grouped aggregate maintains a map `grouping-key → aggregate-state` and, for each input
row, finds or creates the group and folds the row into its state. Every engine decomposes
each aggregate function into four primitives, and this decomposition governs everything
that follows (parallelism, spilling, mergeability):

- **init(state)** — zero the accumulator.
- **update(state, value)** — fold one input value (the hot path).
- **merge(stateA, stateB)** — combine two *partial* states for the same key.
- **finalize(state) → value** — produce the output cell.

Functions split by how much state `merge` needs — the Gray et al. data-cube taxonomy,
which directly sizes the state struct:

| Class | Examples | State | Mergeable? |
|---|---|---|---|
| **Distributive** | `COUNT`, `SUM`, `MIN`, `MAX` | one value | yes — `merge` is the same function |
| **Algebraic** | `AVG`, `STDDEV` | fixed-size tuple of distributive sub-aggregates | yes — via the tuple |
| **Holistic** | `MEDIAN`, `PERCENTILE`, exact `COUNT(DISTINCT)` | unbounded (full multiset) | no bounded state — need a per-group set or an approximate sketch (HyperLogLog, t-digest) |

Two consequences worth internalising:

- **`AVG` must be carried as `(sum, count)`** and divided only at `finalize` — pre-divided
  averages cannot be merged.
- **Design `merge` in from day one**, even for a single-threaded first implementation. It
  costs nothing now and is the difference between "extends to parallel / partitioned
  aggregation" and "rewrite." Holistic aggregates are the category that breaks a
  fixed-width-blob state assumption; plan for them as a separate path.

---

## 2. The two foundational strategies: hash vs. sort

| | **Hash aggregation** | **Sort aggregation** |
|---|---|---|
| Cost | O(n), random memory access | O(n log n) sort + O(n) linear pass, sequential access |
| Live state | O(#groups) — whole table resident | O(1) — only the current run's group |
| Wins when | low / moderate group cardinality (large fold factor) | high cardinality, tight memory, **or input already sorted on the key** |
| Output order | unordered | sorted on the grouping key (a free `ORDER BY`) |
| Spilling | needs partitioning to spill cleanly | external merge-sort, entirely natural |

The empirical rule (Müller/Sanders/Leis, *Cache-Efficient Aggregation: Hashing Is
Sorting*[^hashissort]): hashing wins when the *distinct/total* ratio is low (lots of
collapsing); the gap shrinks as cardinality rises and reverses under memory pressure or
when output must be ordered. They further prove the two have identical cache complexity in
the external-memory model and build one framework that starts hashing and falls back to
sorting.

**Implication:** hash is the right default; sort-based aggregation is best kept as a
*specialization* for input already ordered on the grouping key, where it degenerates to a
streaming O(n) pass with O(1) state and no hash table. That special case is Neo4j's
`OrderedAggregation` (§9) and ClickHouse's `optimize_aggregation_in_order` (§7).

---

## 3. Parallel strategies

Three ways to parallelize, differing entirely in how they handle the cross-thread merge:

- **(a) Partitioned / radix.** Partition rows by hash of the key into P partitions; equal
  keys always hash identically, so each partition owns a disjoint key-space and one thread
  aggregates it to completion with **no cross-thread merge of the same key**. Scales to high
  thread counts and high cardinality; risks are **skew** (a hot partition overloads one
  thread) plus the shuffle cost. Also what makes spilling clean — a partition evicts and
  finalizes independently.
- **(b) Partial (thread-local) pre-aggregation + merge.** Each thread folds its input into a
  private table, then a second phase merges the partials. Contention-free, and *extremely*
  cheap at low cardinality (each small table absorbs almost everything). Downside:
  **memory blow-up at high thread × high cardinality** — every thread materialises a
  near-full table (a 2025 analysis measured ≈39 GB thread-local vs ≈5 GB partitioned at 48
  threads / 100M keys[^ghtsb]).
- **(c) Shared global table with atomics / latching.** One table, per-slot atomic updates.
  Memory-optimal; when purpose-built it is competitive with or better than partitioning at
  *low* cardinality, but suffers contention on hot groups and resize stalls[^ghtsb].

No single method dominates — it is a function of cardinality and skew. High cardinality with
skew → partitioning is the safe fallback. This is why modern systems went adaptive.

---

## 4. The modern convergence: adaptive two-phase

HyPer, Umbra, and DuckDB independently land on the same design, and the adaptivity is
largely **emergent from a fixed-size thread-local table acting as a cache/filter**[^morsel][^duckdb-agg]:
phase 1, each thread pre-aggregates into a small, cache-resident table; when it fills, it
flushes into radix partitions and resets. Phase 2 finalises partitions independently in
parallel.

- **Low cardinality** → the small table catches the heavy hitters, most rows collapse
  locally, little spills; also **skew-resistant**, because hot keys are pre-collapsed by
  every thread before partitioning.
- **High cardinality** → the table constantly overflows and the scheme degrades gracefully
  to pure partitioning.

DuckDB makes the switch concrete: thread-local until a thread's table crosses a fixed entry
limit (reported ≈10k — a version-specific tunable, not an invariant), then partitioned mode;
it also *abandons* the probe table when it detects pre-aggregation is not reducing[^duckdb-agg].
Umbra/Umami make the cardinality tiers explicit (local → larger-local / global → partition →
spill).

---

## 5. The hash-table micro-design (industry consensus)

DuckDB, Kùzu, and ClickHouse independently converged on the same structure:

- **Open addressing + linear probing** — contiguous, cache-friendly, no per-entry allocation.
- **Salted / fingerprinted slots** — store a few bits/bytes of the hash *inline in the slot*
  and compare the tag before dereferencing the payload; rejects the vast majority of negative
  probes with no cache miss. Kùzu packs a **7-bit fingerprint + 57-bit pointer** into one
  `uint64_t`[^kuzu-src]; DuckDB uses 1–2 salt bytes[^duckdb-agg].
- **Payload row = `[key columns | packed aggregate states | cached hash]`** — a *row-oriented*
  record inside the otherwise columnar engine, because the aggregate state is the hot object
  and you want key + state on one cache line. Caching the hash in the row lets a **resize
  reallocate only the slot array without rehashing**.
- **Vectorized / batch probing** — hash a whole chunk, gather salts, compare tags, then apply
  updates. Hides memory latency by overlapping independent probes; fits a fixed-chunk model
  directly.

---

## 6. Spilling / out-of-core

Out-of-core aggregation piggybacks on partitioning: partition boundaries are independent, so
a partition spills and finalises without touching others. DuckDB over-partitions (more
partitions than threads) so any one partition likely fits in phase 2, and recursively
repartitions any that does not; thread-local tables are not resized — when full they reset and
unpin pages, letting the buffer manager decide what spills[^duckdb-ext]. Sort-based
aggregation spills even more naturally (external merge sort), the classic reason to keep a
sort path for high-cardinality, memory-starved queries.

---

## 7. ClickHouse

Verified against `master` source. The design centres on the `Aggregator` class plus the
`IAggregateFunction` state protocol.

- **~50 specialised hash-table variants** (`AggregatedDataVariants`) chosen at runtime from
  the key types: directly-addressed arrays for `key8`/`key16` (256 / 65 536 slots, no hashing,
  no collisions); CRC32-hashed open addressing for `key32`/`key64`; a 5-way length-dispatched
  `StringHashMap` (short strings become register-width integer keys); bit-packed multi-key
  `keysN`; a `serialized` universal fallback; and crucially **`low_cardinality_*`**, which
  groups on dictionary *indices* rather than strings. Philosophy: **match the physical table
  to the key** so the common case hits a branchless array or a register-width probe.
- **Two-level hash table** — 256 buckets keyed on the **top 8 bits** of the hash (high bits,
  to decorrelate from the low-bit slot index). A thread starts single-level and converts once
  it crosses `group_by_two_level_threshold` (100 000 keys / 50 MB). Two-level is the
  prerequisite for both **contention-free parallel merge** (bucket *b* holds disjoint keys
  across all threads → one thread merges each bucket) and **bucket-by-bucket spilling** (only
  ≈1/256 resident at once).
- **Aggregate states + combinators** — the elegant core. `IAggregateFunction` defines state as
  a fixed-footprint blob (`create`/`add`/`merge`/`serialize`/`insertResultInto`); all
  aggregates in a query pack their states **contiguously in one arena allocation** at fixed
  offsets. Combinators (`-If`, `-Array`, `-Map`, `-State`, `-Merge`, …) are decorators over that
  interface. The payoff: **the intermediate state is one unified representation** serving
  per-thread merge, disk spill, network transfer, *and* `AggregatingMergeTree` / projection
  pre-aggregation — one concept, not four[^ch-combinators].
- **Vectorization** — columnar batches (65 536 rows), batch entry points (`addBatch`,
  `addBatchSinglePlace`, `addBatchLookupTable8`, which skips hashing for 8-bit keys), hardware
  CRC32, and optional JIT-compilation of the combined `add` of all aggregates into one
  function.

---

## 8. Kùzu (KuzuDB)

Blog offline (Kùzu Inc. wound down in 2025); claims re-verified against GitHub `master`. The
most architecturally relevant system here — a vectorized columnar *graph* engine.

- **Vectorized, pull-based, morsel-driven parallel.** Aggregation is a **sink** that terminates
  its pipeline, split across **three** operators: `HashAggregate` (thread-local partial),
  `HashAggregateFinalize` (which is *also a source* — that is what lets finalize parallelise),
  and `HashAggregateScan`. This three-op split mirrors a `sort_buffer` / `sort_collect` / `sort`
  decomposition — external validation of the pipeline-breaker shape TuringDB already uses for
  `ORDER BY`.
- **Hash table** — `[groupKeys… | aggStates… | hash]` payload rows in a row-major
  `FactorizedTable`, plus a slot array of 7-bit-fingerprint + 57-bit-pointer tagged
  `uint64_t`s, `checkFingerprint()` fast-reject, linear probing (§5)[^kuzu-src].
- **Factorization × aggregation** (the graph-critical technique): Kùzu represents many-to-many
  join intermediates as a *factorized* (Cartesian-product-of-vector-groups) representation, and
  threads a per-row **multiplicity** through `append`/`updateAggStates`, with specialised update
  kernels per flat/unflat combination of key vs value vector — so it aggregates over compressed
  intermediates *without flattening* the m×n explosion. A 2-hop neighbourhood of size k² is
  aggregated from `k + k` stored values instead of k² rows[^kuzu-cidr].
- **Distinct aggregation** — each distinct aggregate gets its *own* hash table keyed on
  `(groupKeys…, distinctValue)`; only first-seen values update the real state.
- **Parallel** — fixed-size per-thread table → radix-partition on the **MSB of the hash** →
  lock-free MPSC per-partition queues → each partition finalised by whichever thread wins its
  `try_lock` (no cross-partition contention, since same key → same partition).

---

## 9. Neo4j — Cypher semantics & operators

The system to match on **semantics** (OpenCypher) rather than mechanism.

### 9.1 Semantics (the part an OpenCypher engine must replicate exactly)

- **There is no `GROUP BY` clause.** The grouping key is *inferred* from each `RETURN`/`WITH`
  projection: **every projected expression that does not contain an aggregate becomes part of
  the grouping key; every one that does contain an aggregate is an aggregation.** In
  `RETURN a, b, count(*)`, the key is `(a, b)` and one row is emitted per distinct `(a,b)`.
  `WITH` groups identically — that is how aggregation chains[^neo4j-agg].
- **The mixed-expression rule** (Neo4j error `42I18`): inside an aggregating projection, only a
  bare variable / property access / map access counts as an implicit grouping key. `n.a +
  count(*)` is legal (`n.a` is a key); `n.a + n.b + count(*)` is **rejected** because `n.b` is
  not itself projected. The fix is to project the compound key in a preceding `WITH`[^neo4j-42i18].
- **Empty-input rules split on whether a grouping key is present.** A *bare* aggregate over zero
  rows still emits one row (`count`→0, `sum`→0, `collect`→[], but `avg`/`min`/`max`→null). A
  *grouped* aggregate over zero rows emits **no rows** — the engine refuses to invent a null
  key[^neo4j-zero].
- **DISTINCT operates inside an aggregate** (`count(DISTINCT x)`, `collect(DISTINCT x)`),
  distinct from `RETURN DISTINCT` (whole-row dedup). `count(*)` counts every row incl. nulls;
  `count(expr)` counts non-nulls only; `collect` drops nulls; nested aggregation is disallowed.

### 9.2 Operators & runtimes

- **`EagerAggregation`** — hash-based, a pipeline breaker: buffers the whole grouping map before
  emitting. **`OrderedAggregation`** — *not eager*: streams when input is already sorted on the
  key, holding only the current group's state. The planner picks ordered when an index or prior
  sort delivers the order for free; a related rewrite lets `min()`/`max()` skip aggregation
  entirely by reading the first/last index entry. Mirrored by `Distinct` / `OrderedDistinct`[^neo4j-ops].
- **Grouping structure** — hash map from grouping-key tuple → per-function accumulator
  (`count`→counter, `avg`→(sum,count), `collect`→growing list); `OrderedAggregation` keeps only
  the current group + last-seen key.
- **Runtimes** — interpreted (pull) → slotted (pull, fixed register offsets) → **pipelined**
  (default Enterprise, push-based, fused operators exchanging ~100–1000-row morsels,
  single-threaded per task) → **parallel** (multi-threaded pipelined, read-only). Aggregation is
  a hard pipeline break in all of them; the standard morsel-driven strategy is thread-local
  partial tables merged at the break[^neo4j-runtime].

---

## References

[^hashissort]: Müller, Sanders, Lacurie, Lehner, Färber — *Cache-Efficient Aggregation: Hashing Is Sorting* (SIGMOD 2015). <https://dl.acm.org/doi/10.1145/2723372.2747644>
[^morsel]: Leis, Boncz, Kemper, Neumann — *Morsel-Driven Parallelism* (SIGMOD 2014). <https://db.in.tum.de/~leis/papers/morsels.pdf>
[^ghtsb]: Xue, Marcus — *Global Hash Tables Strike Back! An Analysis of Parallel GROUP BY Aggregation* (VLDB 2025). <https://arxiv.org/abs/2505.04153>
[^duckdb-agg]: DuckDB — *Parallel Grouped Aggregation in DuckDB* (2022). <https://duckdb.org/2022/03/07/aggregate-hashtable>
[^duckdb-ext]: DuckDB — *No Memory? No Problem — External Aggregation* (2024). <https://duckdb.org/2024/03/29/external-aggregation>; Kuiper, Boncz, Mühleisen — *Robust External Hash Aggregation in the Solid State Age* (ICDE 2024).
[^ch-combinators]: ClickHouse — *Aggregate function combinators*. <https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators>. Source: `src/Interpreters/Aggregator.h`, `AggregatedDataVariants.h`, `src/Common/HashTable/TwoLevelHashTable.h`, `src/AggregateFunctions/IAggregateFunction.h`.
[^kuzu-cidr]: Jin et al. — *Kùzu Graph Database Management System* (CIDR 2023). <https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf>
[^kuzu-src]: kuzudb/kuzu `master` — `src/include/processor/operator/aggregate/aggregate_hash_table.h`, `src/processor/operator/aggregate/hash_aggregate.cpp`; v0.8.0 release notes (parallel hash aggregation).
[^neo4j-agg]: Neo4j — *Aggregating functions*. <https://neo4j.com/docs/cypher-manual/current/functions/aggregating/>
[^neo4j-zero]: Neo4j — *Understanding aggregations on zero rows*. <https://neo4j.com/developer/kb/understanding-aggregations-on-zero-rows/>
[^neo4j-ops]: Neo4j — *Operator summary*. <https://neo4j.com/docs/cypher-manual/current/execution-plans/operator-summary/>
[^neo4j-42i18]: Neo4j — implicit-grouping-expression error `42I18`. <https://github.com/neo4j/neo4j/issues/13200>
[^neo4j-runtime]: Neo4j — *Runtime concepts*. <https://neo4j.com/docs/cypher-manual/current/planning-and-tuning/runtimes/concepts/>

## Verification caveats

- DuckDB's ≈10k thread-local switch threshold and the exact salt-byte / entry sizes are
  **version-specific tunables** — verify against current source before hard-coding.
- The *Global Hash Tables Strike Back* figures are directional (read via summary, not
  line-by-line from the tables).
- Neo4j's thread-local-then-merged parallel aggregation state and whether `EagerAggregation`
  spills to disk are **by-design inference / third-party-corroborated**, not spelled out in the
  primary Neo4j docs consulted.
- Kùzu's blog/docs were unreachable; every Kùzu claim above was re-verified against current
  GitHub source, which reflects the post-0.8.0 engine (not necessarily the 2023 paper).
