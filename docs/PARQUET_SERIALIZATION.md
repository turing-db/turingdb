# Parquet Serialization — Implementation Plan

Migrate TuringDB's on-disk serialization from the in-tree custom binary format
(`storage/dump/`, page-aligned `FilePageWriter`, magic `0x1BADCAFE`) to a
Parquet-backed format, reusing the engineering already in Apache Arrow/Parquet
(page management, compression, row groups, column chunks) instead of maintaining
our own.

This document is the authoritative plan. Every claim about the current engine is
grounded in the tree; references are cited as `path:line` and collected in the
[Appendix](#appendix--verified-source-references).

---

## 1. Goals and non-goals

**Primary goal: retire the custom binary format.** TuringDB today owns ~6,500 lines of
bespoke serialization code — ~5,800 lines across 66 files implementing 40
`*Dumper`/`*Loader` classes (`storage/dump/`), ~715 lines of page/alignment/`O_DIRECT`
machinery (`FilePageWriter`/`FilePageReader`/`AlignedBuffer`/`DumpConfig`), plus 7
dump-constants/helper headers of per-page packing math. The point of this migration is
to **delete that code** and lean on Parquet for the non-trivial parts of a DB file
format (paging, alignment, compression, encodings, checksums, a versioned container).
What remains is thin structure↔column adapters. Dump-size reduction and on-disk-iterator
readiness are secondary benefits, not the driver.

| # | Goal |
|---|---|
| 1 | Replace the custom per-part binary encoders with Parquet for every structure in a `DataPart`. |
| 2 | Retire the hand-rolled page/compression machinery (`FilePageWriter`, `DumpConfig` page layout) for the migrated data. |
| 3 | Get compression and row-group structure "for free" — shrink dumps and lay the groundwork for partially-on-disk iterators. |
| 4 | Keep the `DataPart` as the unit of dump/load/transfer (unchanged, and required by the GraphHub design). |
| 5 | Coexist with the binary format during transition: old dumps stay loadable. |

**Non-goals (explicit):**

- **Interop with external engines.** Parquet is a *substrate* — its
  page/compression/row-group machinery plus TuringDB-specific semantics encoded in
  columns and file metadata. The schema is **not** constrained to be meaningful to
  DuckDB/pandas/Spark.
- **Embedding storage efficiency.** Embeddings land as raw `FIXED_LEN_BYTE_ARRAY`.
  Quantization / tiered storage is a separate, later effort.
- **Content-addressing.** The content hash is taken over the *in-memory* `DataPart`
  (the only faithful representation), not the Parquet bytes
  ([§8](#8-interaction-with-graphhub-content-addressing)).

---

## 2. Design principles

1. **Dump everything; reconstruct nothing on load.** Every in-memory structure —
   including the derived indexers (`EdgeIndexer`, `PropertyIndexer`,
   `StringPropertyIndexer`) and **both** edge directions — is serialized in full and
   read straight back into the same in-memory structure. We do **not** re-run any
   build algorithm on load (no re-sorting edges to derive the in-adjacency, no
   re-inserting strings to rebuild prefix trees, no recomputing index offsets).
   Rationale: on 100 M-node embedding-heavy graphs, rebuilding indexers at load is
   prohibitively expensive and risks divergence from the faithful in-memory state
   we content-hash. Larger dumps are acceptable; Parquet compression mitigates, and
   load stays a straight column read.

   > This includes the one structure the *current* binary format reconstructs:
   > `EdgeIndexer::_patchNodeOffsets` is not written by `EdgeIndexerDumper` today and
   > is rebuilt on load. To honor this principle we serialize it explicitly
   > ([§5.4](#54-edgeindexer)), making the Parquet dump strictly more self-contained
   > than the binary one.

2. **Parquet is a storage encoding, not an identity.** No random local ID and no
   format padding influences object identity; identity is computed over the
   in-memory form ([§8](#8-interaction-with-graphhub-content-addressing)). Codec,
   row-group sizing, and Arrow version are free to change.

3. **One Parquet file per logical table.** Tables of differing row counts (nodes,
   edges, each property type, each indexer table) map to independent Parquet files —
   Parquet is one schema per file. This preserves the current per-part file
   decomposition; file count stays comparable to the binary layout.

4. **Arrow stays behind the `io/parquet` boundary.** `storage/` never includes an
   Arrow/Parquet header. The low-level writer lives in `io/parquet/`; the
   per-structure encoders live in `storage/dump/parquet/` and drive it through an
   Arrow-free API.

5. **Format is selected by a marker; both readers coexist.** New dumps write a
   `PARQUET` type marker; the loader dispatches on it. The binary loader is retained
   until existing dumps are migrated.

---

## 3. Current state (what we are replacing)

### 3.1 The dump tree

`GraphDumper::dump(const Graph&, const fs::Path&)` (`storage/dump/GraphDumper.h:12`,
reached from `GraphSerializer::dump()` `storage/GraphSerializer.cpp:23`, uploaded
wholesale by `S3PushProcessor`):

```
graphDir/
  info                 # GraphInfo: file header + metadata
  type                 # text: "BINARY"            <- format marker
  commitlog            # header + commit-hash list
  commits/<hash>/      # labels, edge-types, property-types, labelsets,
                       #   journal, tombstones, metadata
  dataparts/<id>/      # per part (see 3.2)
```

### 3.2 Per-DataPart files (binary)

`DataPartDumper::dump` (`storage/dump/DataPartDumper.h:12`, `.cpp:78-274`):

| File | Dumper | Structure |
|------|--------|-----------|
| `info` | `DataPartInfoDumper` | `DataPartID`, counts |
| `nodes` | `NodeContainerDumper` | per-node label-set **and** label-set ranges |
| `edges` | `EdgeContainerDumper` | out-edges **and** in-edges |
| `edge-indexer` | `EdgeIndexerDumper` | adjacency ranges + labelset span indexes |
| `node-prop-indexer` / `edge-prop-indexer` | `PropertyIndexerDumper` | property presence ranges |
| `node-props-<ptID>` / `edge-props-<ptID>` | `Trivial`/`String`/`Embedding` `PropertyContainerDumper` | `(ids[], values[])` per type |
| `node-string-prop-indexer(+ -owners)` / `edge-string-prop-indexer(+ -owners)` | `StringIndexerDumper` | prefix tree + owners |

Every file starts with a 12-byte header (`0x1BADCAFE` + version,
`storage/dump/DumpConfig.h:10-23`) in 1 MB pages via `FilePageWriter`
(`io/fs/FilePageWriter.h:48-65`).

### 3.3 The Parquet integration that already exists

- `io/parquet/ParquetReader` — SAX reader over `parquet::ParquetFileReader`, typed
  callbacks on `ParquetSaxVisitor` (incl. `onFixedLenByteArrayValues(..., byteWidth)`,
  the embedding path). Catches `parquet::ParquetException` → `TuringException`
  (`ParquetReader.cpp:37-41,203-245`).
- Built as `turing_db_io_parquet_s` linking `Parquet::parquet_static`
  (`io/parquet/CMakeLists.txt`); `find_package(Parquet REQUIRED)` at `CMakeLists.txt:222`.
- The **low-level write path is already exercised in tests**
  (`test/io/ParquetReaderTest.cpp:124-161`): `ParquetFileWriter::Open` →
  `AppendRowGroup()` → `NextColumn()` → typed `WriteBatch(...)`. The writer follows
  this path (not `arrow::Table`), for symmetry with the reader.

---

## 4. Target format

### 4.1 Type mapping

| TuringDB `ValueType` (`storage/metadata/PropertyType.h:15-25`) | Parquet physical | Note |
|---|---|---|
| `Int64` | `INT64` | — |
| `UInt64` / IDs (`EntityID`/`NodeID`/`EdgeID`/`LabelSetID`/`EdgeTypeID`/`PropertyTypeID`/`size_t`) | `INT64` | raw value; typed wrapper restored on load |
| `Double` | `DOUBLE` | — |
| `Bool` | `BOOLEAN` | — |
| `String` | `BYTE_ARRAY` | `UTF8` |
| `Embedding` | `FIXED_LEN_BYTE_ARRAY` | `byteWidth = dimension × 4`; dimension in metadata |

All structural-index columns (offsets, counts, span first/count) are `INT64`.

### 4.2 Per-DataPart files (Parquet)

Mirrors the binary decomposition; each binary file becomes one or more Parquet
files (split only where row counts differ). Scalars go in per-file Parquet
key-value metadata.

```
dataparts/<id>/
  info.parquet                        # dataPartID, firstNodeID, firstEdgeID, counts, formatVersion (metadata only)
  node-records.parquet                # labelset_id                      (row i = node firstNodeID+i)
  node-ranges.parquet                 # labelset_id, first_node_id, count
  edges-out.parquet                   # edge_id, node_id, other_id, edge_type_id
  edges-in.parquet                    # edge_id, node_id, other_id, edge_type_id
  edge-indexer-nodedata.parquet       # out_first, out_count, in_first, in_count   (row i = node i)
  edge-indexer-out-spans.parquet      # labelset_id, offset, count
  edge-indexer-in-spans.parquet       # labelset_id, offset, count
  edge-indexer-patch.parquet          # node_id, offset                  (explicit; see 5.4)
  node-prop-indexer.parquet           # property_type_id, labelset_id, offset, count
  edge-prop-indexer.parquet           # property_type_id, labelset_id, offset, count
  node-props-<ptID>.parquet           # entity_id, value
  edge-props-<ptID>.parquet           # entity_id, value
  node-string-index-nodes.parquet     # property_type_id, node_id
  node-string-index-children.parquet  # property_type_id, parent_node_id, child_index, child_node_id
  node-string-index-owners.parquet    # property_type_id, node_id, entity_id
  edge-string-index-nodes.parquet     # (as node-string-index-*)
  edge-string-index-children.parquet
  edge-string-index-owners.parquet
```

### 4.3 Commit / graph metadata

The commit-level structures (`labels`, `edge-types`, `property-types`,
`labelsets`, `journal`, `tombstones`, `metadata`; `storage/dump/CommitDumper.cpp:23-178`)
are small. Tabular ones (`labels`/`edge-types`/`property-types` → `(id, name)`;
`tombstones` → id list) → small Parquet tables; structural ones (`journal`,
`labelsets`, commit `metadata`) → Parquet or `nlohmann_json`. Decided in Phase 3.

### 4.4 Row groups and compression

Phase 1: fixed rows-per-row-group (start 1 M) and `ZSTD` for value columns;
dictionary for low-cardinality id columns. Forward: align row-group boundaries to
node-ID / partition ranges for partial on-disk scans. `BYTE_STREAM_SPLIT` is not
applicable to `FIXED_LEN_BYTE_ARRAY` embeddings (revisit with the embedding
effort).

---

## 5. Faithful column schemas

Grounded in the verbatim member layouts (Appendix). Each table's row order is
load-bearing where noted (positional reconstruction, no recompute).

### 5.1 NodeContainer (`storage/datapart/NodeContainer.h:82-90`)

Members: `_firstID`, `_nodeCount`, `LabelSetIndexer<NodeRange> _ranges`,
`NodeRecords _nodes`. `NodeRange = {NodeID _first, size_t _count}`;
`NodeRecord = {LabelSetHandle _labelset}`.

- **`node-ranges.parquet`** — one row per range, ordered as the `_ranges` map
  (by `LabelSetID`): `labelset_id`, `first_node_id`, `count`.
- **`node-records.parquet`** — one row per node in node-id order: `labelset_id`.
  Node id is positional (`firstNodeID + rowIndex`).
- Scalars `firstNodeID`, `nodeCount` in `info.parquet` metadata.

### 5.2 EdgeContainer (`storage/datapart/EdgeContainer.h:34-42`)

`EdgeRecord = {EdgeID, NodeID node, NodeID other, EdgeTypeID}`. Both directions
serialized (no rebuild of `getIns()` from `getOuts()`).

- **`edges-out.parquet`** / **`edges-in.parquet`** — 4 columns each:
  `edge_id`, `node_id`, `other_id`, `edge_type_id`. `EdgeRecord` is
  array-of-structs in memory, so each column is gathered into a contiguous scratch
  buffer before `WriteBatch` (O(n) transpose per column).
- Scalars `firstEdgeID`, `firstNodeID` in metadata.

### 5.3 PropertyManager containers (`storage/properties/PropertyContainer.h:117,221,300`)

Per property type, the container exposes contiguous `ids()` and `all()`:

- **`node-props-<ptID>.parquet`** / **`edge-props-<ptID>.parquet`** — `entity_id`
  (`INT64`) + `value` (type per [§4.1](#41-type-mapping)). Trivial types
  (`Int64`/`UInt64`/`Double`/`Bool`) write `all()` near-zero-copy. Strings build
  `ByteArray` descriptors over the existing `string_view` bytes. Embeddings write
  each view's flat floats as `FIXED_LEN_BYTE_ARRAY(byteWidth = dim×4)`; dimension in
  metadata.

### 5.4 EdgeIndexer (`storage/indexers/EdgeIndexer.h:60-84`)

Members: `_firstNodeID`, `_firstEdgeID`, `std::vector<NodeEdgeData> _nodes`,
`std::span<NodeEdgeData> _coreNodes`/`_patchNodes`,
`std::unordered_map<NodeID,size_t> _patchNodeOffsets`,
`LabelSetIndexer<EdgeSpans> _outLabelSetSpans`/`_inLabelSetSpans`.
`NodeEdgeData = {OutEdgeRange{first,count}, InEdgeRange{first,count}}`.

- **`edge-indexer-nodedata.parquet`** — one row per `_nodes` entry, in order:
  `out_first`, `out_count`, `in_first`, `in_count`. `_coreNodes`/`_patchNodes` are
  spans over `_nodes`; their boundary is the scalar `coreNodeCount` (metadata), so
  the spans are restored by slicing, not recomputed.
- **`edge-indexer-out-spans.parquet`** / **`edge-indexer-in-spans.parquet`** — one
  row per span, grouped by labelset in map order: `labelset_id`, `offset`, `count`.
- **`edge-indexer-patch.parquet`** — `node_id`, `offset`: the `_patchNodeOffsets`
  map, **serialized explicitly** (the binary dumper rebuilds it; we do not — see
  principle 1).
- Scalars `firstNodeID`, `firstEdgeID`, `coreNodeCount`, `patchNodeCount` in metadata.

### 5.5 PropertyIndexer (`storage/indexers/PropertyIndexer.h`)

`using PropertyIndexer = std::unordered_map<PropertyTypeID, LabelSetIndexer<std::vector<PropertyRange>>>`.
`PropertyRange = {size_t _offset, size_t _count}`.

- **`node-prop-indexer.parquet`** / **`edge-prop-indexer.parquet`** — one row per
  `PropertyRange`, grouped by `(property_type_id, labelset_id)` in map order:
  `property_type_id`, `labelset_id`, `offset`, `count`. Range index is positional
  (row order within a group). Group counts are read from the grouping, not stored.

### 5.6 StringPropertyIndexer / StringIndex (`storage/indexes/StringIndex.h:26-145`)

`StringIndex` members: `_nextFreeID`, `std::vector<std::unique_ptr<PrefixTreeNode>>
_nodeManager`, `PrefixTreeNode* _root`. `PrefixTreeNode = {std::vector<EntityID>
_owners, std::vector<PrefixTreeNode*> _children (size ALPHABET_SIZE), size_t _id}`.
`StringPropertyIndexer` holds `std::map<PropertyTypeID, std::unique_ptr<StringIndex>>`.

The tree is already linearized to a node array on disk: `node._id` == its index in
`_nodeManager`; children are written as `(alphabet_index, child_id)` pairs. Faithful
columnization:

- **`*-string-index-nodes.parquet`** — `property_type_id`, `node_id`: one row per
  node, restoring `_nodeManager` size and node ids.
- **`*-string-index-children.parquet`** — `property_type_id`, `parent_node_id`,
  `child_index` (0..ALPHABET_SIZE-1), `child_node_id`: one row per non-null child
  link. Child pointers restored by id lookup into `_nodeManager`.
- **`*-string-index-owners.parquet`** — `property_type_id`, `node_id`, `entity_id`:
  one row per owner (replaces the binary `-owners` aux file).
- Scalars per property type: `node_count`, `root_node_id`, `next_free_id`,
  `ALPHABET_SIZE` in metadata. (`root_node_id` and `next_free_id` are stored so the
  `_root` pointer and `_nextFreeID` are restored, not inferred.)

> This is the structurally hardest table. If a future refactor makes the flattened
> schema fragile, the fallback (allowed by the "encode as one or more columns"
> latitude) is a single `BYTE_ARRAY` column holding the existing linearized tree
> bytes — keeping `StringIndexerDumper`'s serialization but inside a Parquet
> container. Phase 1 implements the flattened tables; the blob fallback is the
> escape hatch.

---

## 6. Module and class design

### 6.1 `io/parquet/` — low-level writer (Arrow-free public API)

Push counterpart to the SAX reader. Added to the existing `turing_db_io_parquet_s`
target (no new link dependency).

```cpp
// io/parquet/ParquetWriter.h
class ParquetWriter {
public:
    ParquetWriter(const fs::Path& path, const ParquetWriteSchema& schema);
    ~ParquetWriter();
    // non-copyable, non-movable (matches ParquetReader)

    void beginRowGroup(size_t rowCount);
    void writeInt64Column(size_t columnIndex, std::span<const int64_t> values);
    void writeDoubleColumn(size_t columnIndex, std::span<const double> values);
    void writeBoolColumn(size_t columnIndex, std::span<const bool> values);
    void writeStringColumn(size_t columnIndex, std::span<const std::string_view> values);
    void writeFixedLenColumn(size_t columnIndex, std::span<const float> flat, size_t byteWidth);
    void setMetadata(std::string_view key, std::string_view value);
    void finish();
    // throws TuringException on Parquet/Arrow failure (mirrors ParquetReader)
};
```

- Owns `arrow::io::FileOutputStream` + `parquet::ParquetFileWriter` internally; only
  primitive spans cross the public boundary (cleaner than the reader, which leaks
  `parquet::` types into its visitor). `string_view`/`float` are wrapped into
  `ByteArray`/`FixedLenByteArray` inside the `.cpp`.
- `ParquetWriteSchema` maps `ParquetColumnType` + names to
  `parquet::schema::{PrimitiveNode,GroupNode}` in the `.cpp`.
- Catches `parquet::ParquetException`, rethrows `TuringException` (as
  `ParquetReader.cpp:37-41`).

### 6.2 `storage/dump/parquet/` — per-structure encoders/decoders

New subdirectory sibling to the binary dumpers; both coexist. Each keeps the
existing convention `static DumpResult<void> dump(...)` / `static DumpResult<...>
load(...)`, translating the io-layer `TuringException` into `DumpResult` at the
boundary. One pair per structure in [§5](#5-faithful-column-schemas):
`DataPartParquetDumper/Loader`, `NodeContainerParquetDumper/Loader`,
`EdgeContainerParquetDumper/Loader`, `EdgeIndexerParquetDumper/Loader`,
`PropertyIndexerParquetDumper/Loader`, `StringIndexParquetDumper/Loader`,
`PropertyContainerParquetDumper<T>`/`Loader`. Naming follows CLAUDE.md's area-local
rule (the `Dumper`/`Loader` family, `Parquet`-qualified, under `storage/dump/parquet/`).

The loaders read columns straight into the in-memory members — no build algorithms
re-run. `EntityID`/`NodeID`/etc. are reconstructed from raw `int64`; span members
(`_coreNodes`, `_patchNodes`) are sliced from the loaded `_nodes` vector using the
scalar counts; child pointers are resolved by id lookup.

### 6.3 Orchestration and format dispatch

- `GraphFileType` (`storage/dump/GraphFileType.h:9-13`) gains `PARQUET`.
- `GraphDumper`/`CommitDumper` select the encoder family by configured format
  (default `BINARY` until Phase 3); the `type` marker records the choice.
- Loaders (`GraphSerializer`/`CommitLoader`/`DataPartLoader`) read the `type` marker
  and dispatch. Old dumps keep loading via the unchanged binary path.

---

## 7. Phasing

**Phase 0 — writer primitive.** `ParquetWriter` + `ParquetWriteSchema` + unit tests
(round-trip every column type through `ParquetWriter` → `ParquetReader`, incl.
`FIXED_LEN_BYTE_ARRAY`). No storage changes.

**Phase 1 — DataPart, fully faithful.** All encoders/decoders in
[§6.2](#62-storagedumpparquet--per-structure-encodersdecoders), all structures in
[§5](#5-faithful-column-schemas). DataPart round-trip test: build → dump → load →
assert the loaded part is structurally identical, **including** indexers (compare
`_nodes`, span boundaries, labelset spans, prefix-tree nodes/children/owners,
patch offsets byte-for-byte against the original).

**Phase 2 — full graph, payload + indexers in Parquet.** Wire into
`GraphDumper`/`CommitDumper` behind `GraphFileType::PARQUET`; commit metadata stays
binary for now (mixed dump is fine). Full dump→load regression.

**Phase 3 — metadata + dispatch + back-compat.** Migrate commit/graph metadata
([§4.3](#43-commit--graph-metadata)); `type`-marker dispatch; parametrize the
regression suite over both formats; flip the default to `PARQUET`.

**Phase 4 — cleanup & tuning.** Compression/row-group tuning; row-group alignment to
node ranges; once existing dumps are migrated, sunset the binary dumpers and the
`FilePageWriter` path.

---

## 8. Interaction with GraphHub content-addressing

The hub design (`docs/GRAPHHUB.md` §4) content-addresses dataparts over the
**in-memory** `DataPart`, not the Parquet bytes. Consequences:

1. The Parquet writer is free to change codec, row-group sizing, and Arrow version
   without affecting identity or dedup.
2. **Cheap byte-stream integrity verification is lost** — a downloaded Parquet
   object cannot be verified by a streaming SHA over its bytes; it must be decoded
   and re-hashed. The hub therefore needs **two hashes**: a content-identity hash
   (in-memory, for dedup) and a storage hash (over the Parquet bytes, for transport
   integrity). Hub-side concern; noted for consistency.
3. The canonical in-memory traversal for the identity hash is `DataPartCanonicalizer`'s
   job (`GRAPHHUB.md` §4.3); the Parquet writer need not be canonical.

---

## 9. Testing

- **Unit (Phase 0):** per-type column round-trip, following `test/io/ParquetReaderTest.cpp`.
- **DataPart round-trip (Phase 1):** structural-equality after dump→load, asserting
  every indexer matches the original (the dump-everything guarantee is the property
  under test).
- **Regression (Phase 2–3):** `regress/` dump/load suites parametrized over `BINARY`
  and `PARQUET`.
- **Back-compat:** a checked-in binary dump that must keep loading after the default flips.

---

## 10. Risks and open questions

- **String prefix-tree columnization** ([§5.6](#56-stringpropertyindexer--stringindex-storageindexesstringindexh26-145))
  — the hardest schema; opaque-`BYTE_ARRAY`-blob fallback retained.
- **Load-time CPU** — Parquet decode is more work than a near-memory-image binary
  load (though we avoid index *rebuild*, which is the bigger cost). Quantify on a
  100 M-node graph in Phase 1; parallelize decode via `JobSystem` if needed.
- **Small-file overhead** — a part with many property/index tables yields many small
  Parquet files (footer overhead each). Phase 4 may consolidate; faithful
  decomposition first.
- **`UInt64`/`size_t` round-trip** — confirm full-range lossless round-trip through
  the `INT64` encoding.
- **`coreNodes`/`patchNodes` span restoration** — confirm slicing `_nodes` by
  `coreNodeCount` reproduces the exact spans (validate in the Phase 1 equality test).

---

## Appendix — verified source references

| Fact | Reference |
|---|---|
| Format enum, magic/version/page size | `storage/dump/GraphFileType.h:9-13`; `DumpConfig.h:10-23`; `io/fs/PageSizeConfig.h` |
| Dump entry + caller; page writer | `storage/dump/GraphDumper.h:12`; `storage/GraphSerializer.cpp:23`; `io/fs/FilePageWriter.h:48-65` |
| Per-part dump order | `storage/dump/DataPartDumper.h:12`; `.cpp:78-274` |
| Commit-level dump | `storage/dump/CommitDumper.cpp:23-178` |
| DataPart accessors | `storage/datapart/DataPart.h:47-74` |
| Typed property contiguous buffers | `storage/properties/PropertyContainer.h:117,221,300`; `PropertyManager.h:149-171` |
| EdgeContainer out/in spans; EdgeRecord | `storage/datapart/EdgeContainer.h:34-42` |
| NodeContainer members; NodeRange; NodeRecord | `storage/datapart/NodeContainer.h:82-90`; `NodeRange.h:7-9`; `NodeRecord.h:8-10` |
| EdgeIndexer members; NodeEdgeData | `storage/indexers/EdgeIndexer.h:60-84`; `NodeEdgeData.h:7-17` |
| EdgeIndexer dump order; `_patchNodeOffsets` rebuilt on load | `storage/dump/EdgeIndexerDumper.h:20-183` |
| PropertyIndexer alias; PropertyRange; dump order | `storage/indexers/PropertyIndexer.h`; `storage/dump/PropertyIndexerDumper.h:19-77` |
| StringIndex / PrefixTreeNode members; dump linearization | `storage/indexes/StringIndex.h:26-145`; `storage/dump/StringIndexerDumper.cpp:16-92` |
| LabelSetIndexer backing map; LabelSetHandle; LabelSetID | `storage/indexers/LabelSetIndexer.h:19`; `storage/metadata/LabelSetHandle.h:273-277`; `storage/ID.h` |
| ValueType enum | `storage/metadata/PropertyType.h:15-25` |
| ParquetReader API + exception wrapping | `io/parquet/ParquetReader.h`; `.cpp:37-41,203-245` |
| Low-level Parquet write APIs in use | `test/io/ParquetReaderTest.cpp:124-161` |
| Parquet CMake wiring | `CMakeLists.txt:222`; `io/parquet/CMakeLists.txt` |
