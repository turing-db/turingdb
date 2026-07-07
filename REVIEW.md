# Parquet import — correctness review

This branch adds data-model-invariant guards to the Parquet importer, but the
guards are incomplete. Every malformed-input case the invariant section of
`CLAUDE.md` promises to reject *cleanly* instead trips an internal `bioassert`
and surfaces as a `FatalException` **"Internal Error"** — not the clean
`TuringException` that a malformed-user-input path is required to throw. The
fixtures that demonstrate these cases are committed to the tree; the tests that
exercised them were removed.

## Status (re-checked against `d1f570357` and `12c05a306`)

- `d1f570357` "Address bug with nullable column" reworked row counting (the
  chunk row count now comes from `onChunkEnd`'s `rows` argument, not from
  counting `__id`/`__source` definition levels) and fixed a separate, real bug
  in nullable **property** columns. It did **not** add `maxDefLevel == 0` guards
  to the required primary columns, so findings 4–6 persist through a new path.
- `12c05a306` "Add invariants…" is documentation only — it added the invariants
  text to `CLAUDE.md` and changed no code, so findings 3 and 4 are documented
  but not fixed.
- Findings 1 and 2 are now fixed on this branch.

| # | Finding | Status |
|---|---------|--------|
| 1 | LIST node property misread as scalar | Fixed |
| 2 | Edge visitor has no list-property guard | Fixed |
| 3 | Empty label list → internal assertion | Still valid |
| 4 | Nullable `__type` → internal assertion | Still valid |
| 5 | Nullable `__id` → internal assertion | Still valid (mechanism changed) |
| 6 | Nullable `__source`/`__target` → internal assertion | Still valid (mechanism changed) |
| 7 | `std::string(nullptr, 0)` for empty string | Still valid (plausible) |

## Findings

### 1. Single-level LIST node properties are not rejected — FIXED
`io/parquet/ParquetNodeVisitor.cpp:56`

The nested-property guard used `isNested = maxRepLevel > 1`. A single-level
`LIST<scalar>` has `maxRepLevel == 1`, so `1 > 1` was false — the list column
flowed into `discoverPropertyColumn` and was treated as a scalar, later tripping
a def-level `bioassert`.

Fixed: the gate is now `isListProperty = maxRepLevel >= 1`, which rejects any
repeated column with a clean `TuringException` ("properties must be
scalar-valued").

### 2. Edge visitor has no nested/list-property guard — FIXED
`io/parquet/ParquetEdgeVisitor.cpp:63`

The edge `else`-branch called `discoverPropertyColumn` with no `maxRepLevel`
check whatsoever, so any repeated/list edge property was registered as a scalar.

Fixed: the same `maxRepLevel >= 1` guard and `TuringException` were added,
matching the node visitor.

### 3. Empty label list silently drops the node — STILL VALID
`io/parquet/ParquetNodeVisitor.cpp:160`

A node with `__labels == []` has `hasValue == false` in `fillLabels`
(`ParquetNodeVisitor.cpp:137-141`), so nothing is `emplace_back`ed — leaving
`_chunkNodeLabels` shorter than `_chunkNodeIds`. `createNodes` then trips
`bioassert(_chunkNodeIds.size() == _chunkNodeLabels.size(), "NodeID, Label mismatch")`.
`fillLabels`/`createNodes` were not touched by the new commits. Per the
data-model invariants, a node with an empty label list is malformed input to
reject cleanly, not an internal error.

Fixture: `empty_labels_nodes.parquet` (`__labels = [[Person],[],[Company]]`).

### 4. Nullable `__type` is accepted, then asserts on a null — STILL VALID
`io/parquet/ParquetEdgeVisitor.cpp:56`

The `__type` check requires `maxRepLevel == 0` but not `maxDefLevel == 0`. An
`OPTIONAL BYTE_ARRAY __type` containing a null passes `onFileStart`,
`fillEdgeTypes` pushes only present values, and `createEdges` trips
`bioassert(_chunkSrcIds.size() == _chunkEdgeTypes.size(), "Edge, Type mismatch")`
(`ParquetEdgeVisitor.cpp:151`). A null `__type` is malformed input per the
invariants.

Fixture: `nulltype_edges.parquet`.

### 5. Nullable `__id` is accepted, then asserts on a null — STILL VALID (mechanism changed)
`io/parquet/ParquetNodeVisitor.cpp:40`

The `__id` check validates only the physical type (INT64), not that the column
is required (`maxDefLevel == 0`). pyarrow marks columns nullable by default, so a
null `__id` is silently dropped from the value span, leaving `_chunkNodeIds` one
short.

`d1f570357` removed the old `_chunkNodeIdDefLevels` counting, but the bug now
surfaces through the commit's own new guard: `applyNodeProperties` runs
`bioassert(_chunkNodeIds.size() == numRows, "Node id count does not match chunk rows")`
(`ParquetNodeVisitor.cpp:173`), where `numRows` is the true row count from
`onChunkEnd`. (If labels are fully populated, `createNodes` at line 160 asserts
first.) Either way it is an internal `FatalException`, not a clean
`TuringException`.

Fixture: `wrongtype_id_nodes.parquet`.

### 6. Nullable `__source`/`__target` — same gap for edge endpoints — STILL VALID (mechanism changed)
`io/parquet/ParquetEdgeVisitor.cpp:41`

The `__source`/`__target` checks validate only INT64, not `maxDefLevel == 0`. A
null endpoint is dropped from the value span. As with finding 5, `d1f570357`
removed the `_chunkSrcIdDefLevels` counting, but a null now trips
`bioassert(_chunkSrcIds.size() == _chunkTgtIds.size(), "Edge source/target mismatch")`
(`ParquetEdgeVisitor.cpp:150`) or, when both endpoints are short by the same
amount, `applyEdgeProperties`'s
`bioassert(_chunkEdgeRecords.size() == numRows, "Edge count does not match chunk rows")`
(`ParquetEdgeVisitor.cpp:170`). Still an internal assertion, not a clean
rejection.

> Findings 4–6 share one fix: require `maxDefLevel == 0` (required column) on
> `__id`, `__source`, `__target`, and `__type` in `onFileStart`, and throw a
> `TuringException` otherwise.

### 7. `std::string` constructed from a possibly-null `ByteArray::ptr` — STILL VALID (plausible)
`io/parquet/ParquetImportVisitor.cpp:112`

`capturePropertyByteArray` does
`emplace_back(reinterpret_cast<const char*>(bytes.ptr), bytes.len)`. If the
decoder delivers an empty string as `{ptr=nullptr, len=0}`, this constructs
`std::string(nullptr, 0)` — a violated `[s, s+n)` precondition (UB; traps under
`-fsanitize=nonnull-attribute`). This line was not touched by the new commits.
Hinges on whether the decoder actually yields a null pointer for empty strings;
worth a guard regardless (`bytes.ptr ? std::string(...) : std::string{}`).

## Note

Findings 3–6 are the bugs demonstrated by the tests removed in commit
`69642130b` ("remove tests - discussed removing tests for falsely flagged
bugs"). Those tests were not false positives: their fixtures are still in the
tree, and each malformed input still reaches an internal assertion. `d1f570357`
addressed a genuine adjacent bug (nullable property columns) and added a
required-columns test, but the malformed-input rejection for the required
primary columns remains unimplemented and untested.
