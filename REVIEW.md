# Parquet import — correctness review

This branch adds data-model-invariant guards to the Parquet importer, but the
guards are incomplete. Every malformed-input case the invariant section of
`CLAUDE.md` promises to reject *cleanly* instead trips an internal `bioassert`
and surfaces as a `FatalException` **"Internal Error"** — not the clean
`TuringException` that a malformed-user-input path is required to throw. The
fixtures that demonstrate these cases are committed to the tree; the tests that
exercised them were removed.

## Findings

### 1. Single-level LIST node properties are not rejected
`io/parquet/ParquetNodeVisitor.cpp:56` — **CONFIRMED**

The nested-property guard uses `isNested = maxRepLevel > 1`. A single-level
`LIST<scalar>` has `maxRepLevel == 1`, so `1 > 1` is false — the list column
flows into `discoverPropertyColumn` and is treated as a scalar. Because a
repeated column now exists, the reader emits one def-level per list element
rather than per row, and `applyNodeProperties` trips
`bioassert(defLevels->size() == numRows, "Definition levels, row mismatch.")`
→ "Internal Error" (or, if counts happen to align, silently assigns list values
to the wrong nodes).

Fixture: `list_property_nodes.parquet` (`tags = [[1,2],[3],[4,5,6]]`).
Fix: gate on `maxRepLevel >= 1`, mirroring the labels guard at line 48.

### 2. Edge visitor has no nested/list-property guard at all
`io/parquet/ParquetEdgeVisitor.cpp:63` — **CONFIRMED**

Unlike `ParquetNodeVisitor`, the edge `else`-branch calls
`discoverPropertyColumn` with no `maxRepLevel` check whatsoever. Any
repeated/list edge property is registered as a scalar, and
`applyEdgeProperties` trips the same def-level `bioassert` → "Internal Error"
instead of rejecting the unsupported list property with a clean
`TuringException`.

Fix: add the equivalent guard the node visitor has (once finding 1 is corrected).

### 3. Empty label list silently drops the node
`io/parquet/ParquetNodeVisitor.cpp:160` — **CONFIRMED**

A node with `__labels == []` has `hasValue == false` in `fillLabels`, so nothing
is `emplace_back`ed — leaving `_chunkNodeLabels` shorter than `_chunkNodeIds`.
`createNodes` then trips
`bioassert(_chunkNodeIds.size() == _chunkNodeLabels.size(), "NodeID, Label mismatch")`.
Per the data-model invariants, a node with an empty label list is malformed
input to reject cleanly, not an internal error.

Fixture: `empty_labels_nodes.parquet` (`__labels = [[Person],[],[Company]]`).

### 4. Nullable `__type` is accepted, then asserts on a null
`io/parquet/ParquetEdgeVisitor.cpp:56` — **CONFIRMED**

The `__type` check requires `maxRepLevel == 0` but not `maxDefLevel == 0`. An
`OPTIONAL BYTE_ARRAY __type` containing a null passes `onFileStart`,
`fillEdgeTypes` pushes only present values, and `createEdges` trips
`bioassert(_chunkSrcIds.size() == _chunkEdgeTypes.size(), "Edge, Type mismatch")`.
A null `__type` is malformed input per the invariants.

Fixture: `nulltype_edges.parquet`.

### 5. Nullable `__id` is accepted, then asserts on a null
`io/parquet/ParquetNodeVisitor.cpp:41` — **CONFIRMED**

The rewritten `__id` check validates only the physical type (INT64), not that
the column is required (`maxDefLevel == 0`). pyarrow marks columns nullable by
default, so a null `__id` is silently dropped from the value span (`numRows-1`
vs `numRows`), and `createNodes` trips its size-mismatch `bioassert`.

Fixture: `wrongtype_id_nodes.parquet`.

### 6. Nullable `__source`/`__target` — same gap for edge endpoints
`io/parquet/ParquetEdgeVisitor.cpp:42` — **CONFIRMED**

The `__source`/`__target` checks validate only INT64, not `maxDefLevel == 0`. A
null endpoint is dropped from the value span, and `createEdges` trips its
size-mismatch `bioassert`. Same root cause as findings 4 and 5, for the two
integer endpoint columns.

> Findings 4–6 share one fix: require `maxDefLevel == 0` (required column) on
> `__id`, `__source`, `__target`, and `__type`, and throw a `TuringException`
> otherwise.

### 7. `std::string` constructed from a possibly-null `ByteArray::ptr`
`io/parquet/ParquetImportVisitor.cpp:112` — **PLAUSIBLE**

`capturePropertyByteArray` does
`emplace_back(reinterpret_cast<const char*>(bytes.ptr), bytes.len)`. If the
decoder delivers an empty string as `{ptr=nullptr, len=0}`, this constructs
`std::string(nullptr, 0)` — a violated `[s, s+n)` precondition (UB; traps under
`-fsanitize=nonnull-attribute`). Hinges on whether the decoder actually yields a
null pointer for empty strings; worth a guard regardless
(`bytes.ptr ? std::string(...) : std::string{}`).

## Note

Findings 1–6 are the exact bugs demonstrated by the tests removed in commit
`7e744c1ed` ("remove tests - discussed removing tests for falsely flagged
bugs"). Those tests were not false positives: their fixtures are still in the
tree, and each malformed input still reaches an internal assertion. The tests
were removed; the bugs were not fixed.
