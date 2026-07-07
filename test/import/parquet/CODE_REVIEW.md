# Parquet Import — Code Review Report

**Scope:** the Parquet import feature (`io/parquet/`, `import/parquet/`, and the
`LOAD PARQUET` plan/analyzer path).
**Base:** `main` · **Review branch:** `parquet-import-review-findings` · **PR:** #708
**Method:** workflow-backed review at `xhigh` effort — 30 agents across 6 independent
finder angles (5 correctness + 1 cleanup), then an independent adversarial verifier for
every distinct `(file, line)` candidate. 27 candidates → 27 verified → 5 refuted → 12
reported.

---

## Executive summary

The import path is functionally correct for the "happy path" the fixtures exercise
(every node has exactly one label, every property is present, string columns fit in one
data page). It is **not robust to valid-but-unusual or malformed input**, and it has one
outright **data-corruption bug** on valid input.

The single highest-impact issue is **finding 1**: string (BYTE_ARRAY) property values are
captured with an overwriting span per Parquet data page, so any string column that spans
more than one page within a row group loses all but the last page's values. The remaining
correctness issues are import-time failures on inputs that are either valid (empty label
lists) or that should be rejected with a clear message but instead trip an internal
assertion (nullable edge type, LIST property columns, wrong-typed required columns).

### Important clarification — `bioassert` throws, it does not abort

Several findings were originally framed as "crashes the server." That framing is wrong,
and it changes their severity. `bioassert` (`common/BioAssert.h`) calls
`__bioAssertImpl`, which **unconditionally throws `FatalException`** (a `TuringException`)
in `common/BioAssert.cpp`; the `abort()` in the header is therefore **dead code**. And
`GraphManager::loadParquetDB` runs the importer inside a `try { … } catch (...) { throw; }`
on the calling thread, so a tripped `bioassert` propagates to the caller as a catchable
exception rather than terminating the process.

Consequence: the "crash" findings are really **"input X raises an internal-error
exception."** For malformed input the correct behavior is a *clean, user-facing*
`TuringException` (as the missing-column checks already produce) — not an internal
`bioassert`. For valid input (empty label list) the correct behavior is a successful
import.

---

## Status legend

| Mark | Meaning |
|------|---------|
| 🧪 **test** | Demonstrated by a failing regression test on this branch (asserts the correct behavior; goes green when fixed). |
| ✅ **fixed** | Fixed on this branch. |
| ⏭️ **skipped** | Deliberately not addressed (rationale given). |
| 📋 **open** | Verified, not yet addressed — tracked for follow-up. |

---

## Findings (severity-ranked)

| # | Finding | Location | Category | Verdict | Status |
|---|---------|----------|----------|---------|--------|
| 1 | String property values corrupted across data-page boundaries | `io/parquet/ParquetImportVisitor.cpp:102` | correctness | CONFIRMED | 🧪 test |
| 2 | Node with an empty/null `__labels` list misaligns labels with node ids | `io/parquet/ParquetNodeVisitor.cpp:128` | correctness | CONFIRMED | 🧪 test |
| 3 | Nullable `__type` on edges produces fewer EdgeTypeIDs than rows | `io/parquet/ParquetEdgeVisitor.cpp:120` | correctness | CONFIRMED | 🧪 test |
| 4 | LIST-typed property column misclassified as scalar | `io/parquet/ParquetImportVisitor.cpp:46` | correctness | CONFIRMED | 🧪 test |
| 5 | Wrong-typed required column trips an internal assertion | `io/parquet/ParquetNodeVisitor.cpp:41` | correctness | CONFIRMED | 🧪 test |
| 6 | `isalnum` on a signed `char` (undefined behavior) | `query/analyzer/CypherAnalyzer.cpp:437` | correctness | PLAUSIBLE | ⏭️ skipped |
| 7 | `LoadParquetNode` took the path by value + `std::move` | `query/plan/nodes/LoadParquetNode.h:14` | style | CONFIRMED | ✅ fixed |
| 8 | Per-row hash lookup in `applyNodeProperties` | `io/parquet/ParquetNodeVisitor.cpp:179` | cleanup | CONFIRMED | 📋 open |
| 9 | Node/edge property-apply logic duplicated | `io/parquet/ParquetNodeVisitor.cpp:200` | cleanup | CONFIRMED | 📋 open |
| 10 | Node import never chunks (whole row group buffered) | `io/parquet/ParquetReader.cpp:391` | cleanup | PLAUSIBLE | 📋 open |
| 11 | Abbreviated member/constant names | `io/parquet/ParquetImportVisitor.h:63` | cleanup | CONFIRMED | 📋 open |
| 12 | `break;` indentation slip in a switch | `io/parquet/ParquetImportVisitor.cpp:69` | cleanup | CONFIRMED | 📋 open |

---

## Correctness findings

### 1. String property values corrupted across data-page boundaries 🧪
**`io/parquet/ParquetImportVisitor.cpp:102`** — CONFIRMED (flagged independently by 5 of 6 finders)

`capturePropertyByteArray` stores the delivered values by **overwriting** assignment:

```cpp
_propByteArrayVals[columnIndex] = values;   // span, not an accumulating copy
```

For BYTE_ARRAY columns the reader delivers values **once per data page**
(`ParquetReader::readSlice`, `isPointerValue` branch): `parquet::ByteArray::ptr` points
into page-owned memory that the next `ReadBatch` invalidates, so values must be consumed
per sub-batch. Scalar columns (Int64/Double/Bool) are safe because the reader *accumulates*
them into a single span before delivering; only the byte-array path overwrites.

Meanwhile the definition levels accumulate for the whole chunk. So at `onChunkEnd`,
`applyNodeProperties`/`applyEdgeProperties` index the retained span (the **last page only**)
by a whole-chunk value index. Early rows silently receive strings from the wrong page, and
once the index passes the last page's size it trips the range guard at
`ParquetNodeVisitor.cpp:229` / `ParquetEdgeVisitor.cpp:223`.

**Failure scenario / test evidence:** importing a `nodes.parquet` whose string property
spans multiple pages fails with:

```
Internal Error: The assertion 'valueIndex < values.size()' failed at ParquetNodeVisitor.cpp:229
String property 'name': value index 4 out of range (captured 4)
```

i.e. only the last page's 4 values survived. **This is data corruption on valid input** and
the top-priority fix. Correct behavior: accumulate byte-array values across sub-batches (as
the scalar path already does), or apply them per sub-batch before the next page overwrites.
Test: `ImportsStringPropertySpanningManyPages`.

### 2. Node with an empty/null `__labels` list misaligns labels with node ids 🧪
**`io/parquet/ParquetNodeVisitor.cpp:128`** — CONFIRMED

`fillLabels` skips non-present label entries *before* allocating the per-node label slot:

```cpp
const bool hasValue = _chunkLabelDefLevels[i] == _lblMaxDefLevel;
if (!hasValue) {
    continue;                       // <-- skips the emplace_back below
}
const bool nextNode = _chunkLabelRepLevels[i] == 0;
if (nextNode) {
    _chunkNodeLabels.emplace_back();
}
```

A node whose `__labels` list is empty (or null) produces a level entry with
`repLevel == 0` and `defLevel < maxDefLevel`. The `continue` fires first, so no
`_chunkNodeLabels` slot is created for that node. `_chunkNodeLabels` ends up shorter than
`_chunkNodeIds`, and `createNodes` trips:

```
Internal Error: The assertion '_chunkNodeIds.size() == _chunkNodeLabels.size()' failed at ParquetNodeVisitor.cpp:148
NodeID, Label mismatch
```

An empty label list is **valid input** — a node may legitimately have no labels — so the
correct behavior is a successful import with that node carrying no labels. The test suite
never exercises this because every fixture node has exactly one label.
Test: `ImportsNodeWithEmptyLabelList`.

### 3. Nullable `__type` on edges produces fewer EdgeTypeIDs than rows 🧪
**`io/parquet/ParquetEdgeVisitor.cpp:120`** — CONFIRMED

`fillEdgeTypes` pushes one `EdgeTypeID` per delivered value, and `ParquetEdgeVisitor::onLevels`
only captures definition levels for *property* columns — the `__type` column's def levels
are never consulted. (Node `__labels` handles nulls via def levels; `__type` has no
equivalent.) So an OPTIONAL/nullable `__type` with any null delivers fewer values than there
are rows, and there is no way to map the missing types back to their edges. `createEdges`
trips:

```
Internal Error: The assertion '_chunkSrcIds.size() == _chunkEdgeTypes.size()' failed at ParquetEdgeVisitor.cpp:135
Edge, Type mismatch
```

Every edge requires a type, so a null `__type` is invalid input and should be rejected with
a clear, user-facing error rather than an internal assertion (or, if typeless edges are ever
supported, handled explicitly). Test: `RejectsNullEdgeTypeCleanly`.

### 4. LIST-typed property column misclassified as scalar 🧪
**`io/parquet/ParquetImportVisitor.cpp:46`** — CONFIRMED

`discoverPropertyColumn` classifies a property column purely by physical type and ignores
repetition (there is even a `// FIXME: Byte arrays always strings. Check for lists, etc.`
at line 45). A repeated (LIST) property column — which the documented format permits ("any
other columns are interpreted as properties") — is registered as a scalar property. The
reader then decodes it through the repeated branch (per-sub-batch, overwriting values),
while `capturePropertyLevels` appends a level stream longer than the row count. At
`onChunkEnd`:

```
Internal Error: The assertion 'defLevels->size() == numRows' failed at ParquetNodeVisitor.cpp:186
Definition levels, row mismatch.
```

An unsupported LIST property should be rejected with a clear message at schema-discovery
time, not crash mid-import. Test: `RejectsListPropertyColumnCleanly`.

### 5. Wrong-typed required column trips an internal assertion 🧪
**`io/parquet/ParquetNodeVisitor.cpp:41`** — CONFIRMED

`onFileStart` enforces required-column physical types with `bioassert`:

```cpp
const bool isInt64 = type == NODE_COL_TYPE;
bioassert(isInt64, "Node column was not integral.");
```

whereas a *missing* required column a few lines later throws a clean `TuringException`
(`ParquetNodeVisitor.cpp:54`). So a schema mismatch (e.g. `__id` written as INT32) produces
an internal-error exception:

```
Internal Error: The assertion 'isInt64' failed at ParquetNodeVisitor.cpp:41
Node column was not integral.
```

instead of the clean user-facing error the missing-column branch produces. The same pattern
applies to `__labels`, and to `__source`/`__target`/`__type` in `ParquetEdgeVisitor::onFileStart`.
This is a **consistency** issue: both are user-input errors and should surface the same clean
way. Test: `RejectsWrongTypedRequiredColumnCleanly`.

### 6. `isalnum` on a signed `char` — undefined behavior ⏭️
**`query/analyzer/CypherAnalyzer.cpp:437`** — PLAUSIBLE (skipped)

```cpp
for (char c : graphName) {
    if (!(isalnum(c) || c == '_')) { … }
}
```

`isalnum` is only defined for arguments representable as `unsigned char` or `EOF`; passing a
negative `char` (a byte with the high bit set, e.g. from a non-ASCII directory basename used
as the derived graph name) is undefined behavior. The mechanism is real, but on glibc
`isalnum` tolerates `[-128, 255]`, so it does **not** crash or misclassify on this platform —
which is why the verifier rated it PLAUSIBLE, not CONFIRMED.

**Skipped intentionally:** because the UB does not manifest on glibc, no unit test can
reliably demonstrate a failure here. The one-line hardening (`isalnum(static_cast<unsigned
char>(c))`) is still worth doing opportunistically, but it is not covered by this branch.

### 7. `LoadParquetNode` took the path by value + `std::move` ✅
**`query/plan/nodes/LoadParquetNode.h:14`** — CONFIRMED (fixed)

The constructor took `fs::Path filePath` by value and `std::move`d it into the member,
using move semantics contrary to the codebase convention ("pass by pointer or reference")
and inconsistent with the sibling plan nodes `LoadGMLNode` / `LoadJsonlNode`, which take
`const fs::Path&`.

**Fixed on this branch** by taking the path by const reference, matching the siblings:

```cpp
LoadParquetNode(PlanGraphNodeID id, std::string_view graphName, const fs::Path& filePath)
    : PlanGraphNode(id, PlanGraphOpcode::LOAD_PARQUET),
    _graphName(graphName),
    _filePath(filePath)
```

The call site in `PlanGraphGenerator::generateLoadParquetQuery` needs no change — the
existing `loadParquet->getFilePath()` (a `const fs::Path&`) binds directly — and the
now-unused `<utility>` include was removed.

---

## Non-correctness findings (cleanup / style — verified, open)

These do not change behavior on well-formed input, but they affect performance,
maintainability, and consistency with the codebase conventions. All are verified; none are
addressed on this branch.

### 8. Per-row hash lookup in `applyNodeProperties` (performance) 📋
**`io/parquet/ParquetNodeVisitor.cpp:179`** — CONFIRMED · category: cleanup

The property-application loop re-resolves each node's `NodeID` through a hash map, once per
row **per property column**:

```cpp
for (const auto& [columnIndex, prop] : _propertyColumns) {   // W property columns
    …
    for (size_t row = 0; row < numRows; row++) {             // N rows per chunk
        const NodeID nodeID = _nodeIDs.at(_chunkNodeIds[row]);   // W × N map lookups
        addNodeProperty(nodeID, prop, columnIndex, row);
    }
    …
}
```

`_nodeIDs` is `std::unordered_map<int64_t, NodeID>&` and grows to the entire graph's node
count, so import CPU scales with property-column width for no reason. The assigned `NodeID`s
are already known the moment `createNodes` runs. The **edge** visitor already avoids this by
recording the assigned records in a per-chunk vector and indexing it directly:

```cpp
const EdgeRecord& edgeRecord = _chunkEdgeRecords[row];   // O(1), no hashing
```

Fix: have `createNodes` push the assigned `NodeID`s into a per-chunk `std::vector<NodeID>`
(mirroring `_chunkEdgeRecords`) and index that in `applyNodeProperties`, turning `W × N`
hash lookups into `W × N` array indexes.

### 9. Node/edge property-apply logic duplicated (maintainability) 📋
**`io/parquet/ParquetNodeVisitor.cpp:200`** — CONFIRMED · category: cleanup

`applyNodeProperties`/`addNodeProperty` (`ParquetNodeVisitor.cpp:160-249`) and
`applyEdgeProperties`/`addEdgeProperty` (`ParquetEdgeVisitor.cpp:153-243`) are near-verbatim
copies. Both contain the identical def-level/null-handling preamble:

```cpp
const auto defLevelsIt = _propDefLevels.find(columnIndex);
const bool haveDefLevels = defLevelsIt != end(_propDefLevels);
const std::vector<int16_t>* defLevels = nullptr;
if (haveDefLevels) { … }
// fast path (no def levels) … then the nullable path with the same maxDefLevel check
```

and the identical `Int64/Double/Bool/String` value switch; only the per-row handle
(`NodeID` vs `EdgeRecord`) and the `addNodeProperty` vs `addEdgeProperty` call differ. Any
change to the null handling or a newly supported `ValueType` must be made in both places and
can silently drift.

This is not merely cosmetic: **finding 1 (the string-corruption bug) lives in exactly this
duplicated block**, so the fix has to be applied twice unless the logic is unified first. The
shared logic belongs in `ParquetImportVisitor`, parameterized by the per-row apply callback —
which is why extracting it is listed as a prerequisite in the fix order below.

### 10. Node import never chunks — whole row group buffered (memory) 📋
**`io/parquet/ParquetReader.cpp:391`** — PLAUSIBLE · category: cleanup

`nextChunk` special-cases files that contain a repeated column: it ignores `maxRows` and
consumes the whole row group so the flat and repeated columns stay aligned.

```cpp
const size_t chunkRows = _hasRepeatedColumn ? rowsRemaining
                                            : std::min(maxRows, rowsRemaining);
```

`_hasRepeatedColumn` is set whenever any projected column has `max_repetition_level > 0`
(`ensureFileOpen`, lines 68-73). **Node files always carry the repeated `__labels` column**,
so node import always takes this branch and is *never* chunked — all node ids, all per-node
label vectors, and every property span for the entire row group are materialized at once,
defeating the `DEFAULT_CHUNK_SIZE` streaming design and spiking memory on large row groups
(e.g. the 70 000-row `rowgroup` fixture). A deeper fix would align the flat and repeated
columns within each `maxRows` chunk instead of special-casing the whole file.

(Rated PLAUSIBLE: the memory concern is real, but the originally-claimed "silently drops the
`maxRows` cap" *test failure* was refuted — see the refuted table — so this is a design/scaling
issue rather than an observable bug in the current tests.)

### 11. Abbreviated member and constant names (style) 📋
**`io/parquet/ParquetImportVisitor.h:63`** — CONFIRMED · category: cleanup
(also `ParquetNodeVisitor.h:51`, `ParquetEdgeVisitor.h:68`, `ParquetNodeVisitor.cpp:34`)

The codebase's naming rule forbids abbreviations ("spell them out — `rowGroup` not `rg`,
`column` not `col`, `index` not `idx`"). The Parquet import files violate it pervasively:

| Current | Should be |
|---------|-----------|
| `_propInt64Vals`, `_propDoubleVals`, `_propBoolVals`, `_propByteArrayVals` | `…Values` |
| `_propDefLevels` | `_propertyDefinitionLevels` |
| `INVALID_COL_IDX` | `INVALID_COLUMN_INDEX` |
| `NODE_COL_PATH`, `LABELS_COL_PATH`, `EDGE_TYPE_COL_PATH`, … | `…_COLUMN_PATH` |
| `_nodeColIdx`, `_lblColIdx`, `_srcColIdx`, `_tgtColIdx`, `_edgetypeColIdx` | `_nodeColumnIndex`, `_labelColumnIndex`, `_sourceColumnIndex`, … |
| locals `desc`, `lvls` | `descriptor`, `levels` |

Purely mechanical renames, but they make the shared header inconsistent with the rest of the
codebase.

### 12. `break;` indentation slip in a switch (style) 📋
**`io/parquet/ParquetImportVisitor.cpp:69`** — CONFIRMED · category: cleanup

In `discoverPropertyColumn`'s switch, every value-assigning case aligns `break;` with `case`
(8 spaces), but the throwing case indents `break;` at the case-body level (12 spaces):

```cpp
    case parquet::Type::BOOLEAN:
        valueType = ValueType::Bool;
    break;                                   // 8 spaces — aligned with `case`
    …
    case parquet::Type::UNDEFINED:
        throw TuringException(
            fmt::format("Unsupported column type …"));
            break;                           // 12 spaces — the slip
```

Trivial; realign the `break;` to 8 spaces for uniformity.

---

## Refuted candidates

These surfaced during finding but did **not** survive adversarial verification. Recorded so
they are not re-raised.

| Candidate | Why refuted |
|-----------|-------------|
| `createEdges` bioassert "calls `abort()` instead of throwing" (`ParquetEdgeVisitor.cpp:139`) | False. `__bioAssertImpl` unconditionally `throw`s `FatalException` (a `TuringException`); the `abort()` in `BioAssert.h` is unreachable. bioassert failures are catchable. |
| `nextChunk` "silently drops the `maxRows` cap" for list files (`ParquetReader.cpp:391`) | The *named* concrete failure does not manifest — the reader test writes only REQUIRED, non-repeated columns, so the repeated-column path isn't exercised there. The memory concern survives, restated as finding 10. |
| `resetChunk()` called twice per chunk (`ParquetNodeVisitor.cpp:70`) | Both calls exist, but it is pure style with no observable effect — the second reset clears already-consumed per-chunk state. |
| `PropertyColumn::propertyTypeID` has no in-class default initializer (`ParquetImportVisitor.h:50`) | Every construction site sets it via designated initializer; no uninitialized read occurs. |
| `int` vs `size_t` column loop in `onFileStart` (`ParquetNodeVisitor.cpp:33`) | Style-only; the column counts involved are far within `int` range and match the third-party API's `int` boundary. |

---

## Current status & recommended next steps

**On this branch / in PR #708:**
- Findings **1–5** each have a fail-now regression test in
  `test/import/parquet/ParquetImporterTest.cpp` (asserting the correct behavior; red on
  `main` today, green once fixed). Fixtures are generated by
  `test/import/parquet/generate_bug_fixtures.py`.
- Finding **7** is fixed.
- Finding **6** is skipped (platform-invisible UB).
- Findings **8–12** are documented here for follow-up; not yet addressed.

**Recommended fix order:**
1. **Finding 1** — data corruption on valid input; highest priority. Fix in *both*
   visitors, or first extract the shared apply logic (**finding 9**) and fix once.
2. **Findings 2, 4** — valid/permitted input that currently fails; make empty labels import
   and LIST properties reject cleanly.
3. **Findings 3, 5** — convert the malformed-input `bioassert`s into clean user-facing
   `TuringException`s, consistent with the missing-column checks.
4. **Finding 8** — cheap, self-contained performance win.
5. **Findings 10, 11, 12, 6** — quality/hardening as capacity allows.
