# Grouped Aggregation — Implementation Plan for TuringDB

> **Status:** design / decision record. **Not implemented.** This is the recommended
> approach and phasing for adding `GROUP BY` aggregation to the **`db` / `nl` MLIR
> engine**. The literature and systems study behind these choices is in
> [`docs/AGGREGATE.md`](docs/AGGREGATE.md); the closest existing analog (a multi-row
> pipeline breaker) is [`docs/ORDER_BY.md`](docs/ORDER_BY.md).

---

## 1. Goal & current state

**Goal:** grouped aggregation — `MATCH … RETURN k1, k2, …, agg(x), …` producing one row per
distinct grouping-key tuple — in the `db`/`nl` dialects and the `NLExecutor`.

**Current state (verified):**

- **Grouped aggregation exists nowhere.** The live pipeline engine throws
  `"Group by keys are not supported yet"` (`query/plan/PipelineGenerator.cpp:1194`), and the
  grouping-key list on `AggregateEvalNode::addGroupByKey` (`query/plan/nodes/AggregateEvalNode.h:24`)
  has **zero callers** — grouping keys are never populated.
- **Scalar (whole-stream) aggregation exists** in both engines. In the MLIR engine:
  `db.count` / `db.sum` / `db.min` / `db.max` / `db.avg` lower (via `DBLowering::lowerCount:198`,
  `lowerAggregate:211`) to one-row pipeline breakers `nl.count*` / `nl.aggregate*`, executed by
  `NLAggregateState` (`query/ir/interpreter/NLProgram.h:909`) with reset/update/result
  primitives (`NLProgram.h:932–934`). All collapse the stream to **one row** — no grouping.
- **There are two engines.** The live server path is plan → pipeline/processors
  (`QueryInterpreterV2`); the `db`/`nl` MLIR engine is reachable today only from samples and
  unit tests. **We target the MLIR engine** per the directive ("db and nl dialect"); it is the
  next-gen engine and already has the pipeline-breaker machinery we need.

---

## 2. Architectural shape — fixed by precedent

A grouped aggregate is a **multi-row pipeline breaker**: it must consume the entire input
before emitting, then emit *many* rows (one per group). This is the **`ORDER BY` shape**, not
the one-row `count`/`aggregate` shape.

Model it on `db.sort` → `DBLowering::lowerSort` (`query/ir/lowering/DBLowering.h:178`):

1. hoist a **state handle** (`nl.sort_buffer` → for us, a group-aggregate state);
2. an **update op inside the producing loop** (`nl.sort_collect` → fold each chunk into the
   state);
3. a **source-iterator op after the loop** (`nl.sort`, wrapped in an emit `nl.for` →
   yield group rows in 64K-row chunks).

Contrast the one-row `nl.count`/`nl.aggregate` shape, which has **no emit loop** — wrong for
grouped output.

**The three ingredients already exist independently** and grouped aggregation composes them:

| Ingredient | Existing symbol | Role in grouped aggregation |
|---|---|---|
| Composite-key serialization | `NLKeyAppendFunction` (`NLProgram.h:721`), `selectKeyAppendFunction` (`NLExecutor.h:159`) — used by DISTINCT | build the grouping key per row |
| Per-key accumulator | `NLAggregateState` + reset/update/result (`NLProgram.h:909,932–934`) | fold values within a group |
| Accumulate-then-emit-as-source breaker | `runSortLoop` (`NLExecutor.h:86`), `nl.sort_buffer`/`sort_collect`/`sort` | drive the breaker + multi-row emit |

---

## 3. Recommended approach

Ordered by when to build, not by sophistication:

- **A — Single global open-addressing hash aggregation (build first).** One hash table for the
  whole stream, built during the producing loop, emitted as a source iterator afterward.
  Simplest correct thing; reuses all three ingredients above; single-threaded (matches current
  execution).
- **C — Ordered / streaming aggregation (build next).** When input is already sorted on the
  grouping key, aggregate in one streaming pass holding only the current group's state — emit on
  key change. O(1) memory, output pre-sorted, no hash table. Reuses the existing `nl.sort`; the
  best bang-for-buck optimization. (= Neo4j `OrderedAggregation`, ClickHouse
  `optimize_aggregation_in_order`.)
- **B — Two-phase partitioned aggregation (later).** Thread-local partials → radix-partition on
  the high hash bits → finalize partitions independently. The parallel evolution, and it aligns
  with the **METIS/partitioning roadmap**. Because A's state is designed mergeable and the table
  is already the partitionable structure, this is an increment, not a new operator.

**Rationale.** The study ([`docs/AGGREGATE.md`](docs/AGGREGATE.md) §2–4) shows hash is the right
default, sort-based aggregation is a high-value *specialization* for pre-sorted input, and the
adaptive two-phase design is the end state everyone converges on — but only matters once
execution is parallel. TuringDB is single-threaded in the query engine today, so A is the
correct first cut and C is the cheapest meaningful win. **The one non-negotiable up front is
designing a `merge` primitive into the aggregate state** (see §4), so A → B is not a rewrite.

---

## 4. Hash-table & state design decisions

Follow the industry-consensus structure ([`docs/AGGREGATE.md`](docs/AGGREGATE.md) §5):

- **Open addressing + linear probing**, with a **salt/fingerprint** in each slot for fast
  negative-probe rejection.
- **Payload row = `[grouping key | packed per-function aggregate states | cached hash]`** — a
  row-oriented record (key + state on one cache line); cache the hash so a resize reallocates
  only the slot array.
- **Grouping key** built with the existing `NLKeyAppendFunction` row serialization (the same
  normalization DISTINCT uses; conceptually the "normalised key" of
  [`ORDER_BY.md`](docs/ORDER_BY.md)). Equality via `memcmp`, hash over the serialized bytes.
- **New state object** `NLGroupAggregateState` = the hash map from serialized key →
  per-function `NLAggregateState`. Reuse the existing `NLAggregateReset/Update/Result`
  primitives per group.
- **Add an `NLAggregateMergeFunction`.** The scalar path has reset/update/result but **no
  merge** (`NLProgram.h:932–934`) — grouped aggregation does not strictly need it single-threaded,
  but adding it now is what makes Approach B free later. Carry `AVG` as `(sum, count)`.
- **Functional-dependency key elimination (optimization).** Keys functionally dependent on
  another key (e.g. `RETURN n, n.name` — `n.name` depends on `n`) can be *stored but not hashed*.
  This is Kùzu's `dependentKeysPos` and matches Remy's existing DISTINCT FD notes referenced in
  [`ORDER_BY.md`](docs/ORDER_BY.md) §"Key elimination via functional dependencies". Defer until
  after A works.

---

## 5. Cypher semantics — the correctness backbone

Independent of the algorithm, and where most of the *design* subtlety lives
([`docs/AGGREGATE.md`](docs/AGGREGATE.md) §9.1). OpenCypher has **no `GROUP BY` clause**:

- **Implicit grouping key.** In a `RETURN`/`WITH` projection, every expression *without* an
  aggregate is a grouping key; every expression *with* one is an aggregation. `WITH` groups
  identically (enables chaining).
- **Mixed-expression rule (`42I18`).** Inside an aggregating expression, only a bare variable /
  property / map access counts as an implicit key. `n.a + count(*)` is legal; `n.a + n.b +
  count(*)` is rejected (`n.b` not projected). Enforce with a clear `TuringException`.
- **Empty-input split.** Bare aggregate over zero rows → one default row (`count`→0, `sum`→0,
  `collect`→[]; `avg`/`min`/`max`→null). Grouped aggregate over zero rows → **no rows**.
- **DISTINCT inside an aggregate** (`count(DISTINCT x)`) is separate from `RETURN DISTINCT`
  (whole-row dedup, already implemented as `db.remove_duplicates`).

**Where this lives:**

- **Validation** (mixed-expression rule, nested-aggregation ban) → the analyzer, which already
  marks aggregate projections (`query/analyzer/CypherAnalyzer.cpp`, `ExprAnalyzer.cpp`,
  `ReadStmtAnalyzer.cpp`; `Projection::isAggregate`). Shared by both engines.
- **Key derivation → codegen** → the MLIR frontend `DBProgramGenerator`
  (`query/ir/codegen/DBProgramGenerator.{h,cpp}`), which today emits **only** traversal + output
  (`generateTraversal:44`, `generateOutput:46`) and no aggregate ops at all. It must learn to
  emit the new `db.group_aggregate` op with the derived grouping keys.

---

## 6. Aggregate function set

First cut: `count` (incl. `count(*)` vs `count(expr)` null semantics), `sum`, `min`, `max`,
`avg`. All distributive/algebraic — bounded, mergeable state. (Note `sum` is not currently even
declared in `query/AST/FunctionDecls.cpp` for the live engine — check the AST declares it.)

Later / holistic (separate path, unbounded or approximate state): `collect`, `count(DISTINCT)`,
`percentile*`, `stdev*`. These break the fixed-width-blob assumption — design them after A/C.

---

## 7. Implementation phases

- **Phase 0 — semantics & op contract.** Implicit-grouping-key derivation + `42I18` validation
  in the analyzer. Define the `db.group_aggregate` op contract (grouping-key operands, aggregate
  kinds, result columns). No execution yet.
- **Phase 1 — MLIR operator, testable via hand-written IR.**
  1. `db.group_aggregate` op in `query/ir/dialect/db/DBOps.td` (follow the `AggregateOp` base).
  2. `DBLowering::lowerGroupAggregate` modeled on `lowerSort` (`DBLowering.h:178`): state handle
     + in-loop update + post-loop source iterator.
  3. `nl` ops + state type in `NLOps.td`/`NLTypes.td`: `nl.group_aggregate` /
     `_update` / `_result` (result = source iterator) and an `!nl.group_aggregate_state` handle.
  4. `NLGroupAggregateState` (§4) in `NLProgram.h`; handlers + a `runGroupAggregateLoop` modeled
     on `runSortLoop` in `NLExecutor.cpp`; register ops in `NLTranslator.cpp:199–210`.
  - **Verifiable exactly like the existing scalar-aggregate tests** — `DBLoweringTest.cpp`
    `sumsScores` (~3153), `lowersAggregateToPipelineBreaker` (~3216) — with hand-written `db` IR
    fed through `DBLowering` + `NLInterpreter`.
- **Phase 2 — Cypher frontend.** `DBProgramGenerator` emits `db.group_aggregate` from
  aggregating `RETURN`/`WITH`; end-to-end tested `MATCH … RETURN k, agg(x)` (extend
  `CypherOutputTest.cpp`).
- **Phase 3 — ordered fast path (Approach C).** Planner recognises input pre-sorted on the
  grouping key (e.g. downstream of `nl.sort` or an ordered scan) and emits a streaming variant.
- **Phase 4 — parallel/partitioned (Approach B), when the query engine parallelizes.** Add
  `merge`-based two-phase partitioned aggregation; aligns with the partitioning workstream.

---

## 8. Testing (per project conventions)

- **Test-first**: write the failing test before the fix.
- Build the real **SimpleGraph** fixture (`SimpleGraph::createSimpleGraph`), not a bespoke
  minimal graph; assert against hand-derived group counts (Remy=0, Adam=1, …).
- New behavior → its **own** `GroupAggregate*Test.cpp` with its own CMake target, not appended to
  an existing catch-all test.

---

## 9. Out of scope / future

- **Factorization (Kùzu's multiplicity trick).** Powerful for graph aggregation, but TuringDB's
  `nl.cross_product` **flattens** to N×M rows (no factorized intermediate to exploit). Not a
  lever on the current engine; revisit only if factorized traversal is ever adopted.
- **Spilling / out-of-core.** In-memory only for now; partitioning (Phase 4) is the prerequisite
  for clean spilling.
- **Distribution.** Aggregation is **non-monotonic** — under the CALM lens it needs coordination
  (see [`docs/DISTRIBUTED.md`](docs/DISTRIBUTED.md) §2.1). Out of scope here, but the mergeable
  state design (§4) is exactly what a distributed rollup would reuse.

---

## Key files

**Frontend / semantics:** `query/ir/codegen/DBProgramGenerator.{h,cpp}`,
`query/analyzer/{CypherAnalyzer,ExprAnalyzer,ReadStmtAnalyzer}.cpp`, `query/AST/Projection.h`,
`query/AST/FunctionDecls.cpp`.

**MLIR op → lowering → nl:** `query/ir/dialect/db/DBOps.td`,
`query/ir/dialect/nl/{NLOps.td,NLTypes.td}`, `query/ir/lowering/DBLowering.{h,cpp}` (`lowerSort`,
`lowerCount`, `lowerAggregate`).

**Executor:** `query/ir/interpreter/NLProgram.h` (`NLKeyAppendFunction:721`,
`NLDistinctState:730`, `NLAggregateState:909`, primitives `932–934`),
`query/ir/interpreter/NLExecutor.{h,cpp}` (`runSortLoop:86`, `selectKeyAppendFunction:159`),
`query/ir/interpreter/NLTranslator.cpp:199–210`.

**Columns / memory:** `storage/columns/{ColumnVector,ColumnOptVector,ColumnConst}.h`,
`memory/LocalMemory.h`, `storage/iterators/ChunkConfig.h` (`CHUNK_SIZE = 64*1024`).

**Tests:** `test/query/ir/DBLoweringTest.cpp` (`sumsScores`, `lowersAggregateToPipelineBreaker`),
`test/query/ir/CypherOutputTest.cpp`.
