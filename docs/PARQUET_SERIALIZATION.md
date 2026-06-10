# Parquet Serialization — Implementation Plan

Migrate TuringDB's on-disk serialization from the in-tree custom binary format
(`storage/dump/`, page-aligned `FilePageWriter`, magic `0x1BADCAFE`) to a
Parquet-backed format, reusing the engineering already in Apache Arrow/Parquet
(page management, compression, row groups, column chunks) instead of maintaining
our own.

This document is the authoritative plan **and status record**. Every claim about the
current engine is grounded in the tree; references are cited as `path:line` and
collected in the [Appendix](#appendix--verified-source-references).

---

## 0. Status (as built)

Phases 0–2 are **implemented and committed** on the `parquet-dumper` branch — the binary
serializer has a complete, faithful Parquet counterpart from the `DataPart` up through the
commit and graph level. A body of hardening, memory-bounding and compression work has landed on
top of Phase 2 (see [§0.1](#01-since-phase-2--hardening-memory-bounding-compression-committed)),
short of the Phase 3 format-dispatch wiring.

| Phase | Scope | Status |
|---|---|---|
| 0 | `ParquetWriter` / `ParquetWriteSchema` primitive (`io/parquet/`) | ✅ committed |
| 1 | All six `DataPart` structure adapters + the `DataPart` orchestrator (`storage/dump/parquet/`), each round-trip-tested; gated by `DataPartComparator::same`; EdgeIndexer patch path covered | ✅ committed |
| 2 | Commit-level metadata (`GraphMetadata` maps, journal, tombstones, commit metadata) + `GraphDumper`/`CommitDumper` wiring | ✅ committed — all four adapters plus the `GraphParquet{Dumper,Loader}` / `CommitParquet{Dumper,Loader}` orchestrators; full graph dump→load round-trip gated by `GraphComparator::same` on the Reactome sample — see [§4.3](#43-commit--graph-metadata) |
| 3 | `PARQUET` format-marker dispatch + back-compat with binary dumps | not started |
| 4 | Retire the binary path; compression / row-group tuning | 🔶 partial — ZSTD compression and memory-bounded row-group streaming committed ([§4.4](#44-row-groups-and-compression)); binary retirement and range-aligned row groups not started |

Phase 1 shipped these in `storage/dump/parquet/` (sibling lib `turing_db_storage_dump_parquet_s`,
which keeps Arrow out of `turing_db_storage_s`): `PropertyContainerParquet{Dumper,Loader}`,
`NodeContainerParquet*`, `EdgeContainerParquet*`, `EdgeIndexerParquet*`,
`PropertyIndexerParquet*`, `StringPropertyIndexerParquet*`, and the
`DataPartParquet{Dumper,Loader}` + `DataPartParquetLayout.h` orchestrator. Each has a
round-trip test in `test/storage/dump/parquet/`; the capstone builds a real graph
(`SimpleGraph`) and asserts `DataPartComparator::same`, plus a deterministic two-commit
test that forces (and verifies) an EdgeIndexer patch node.

**Two deliberate deviations from the original draft** (corrected in the sections below):
- **Exceptions, not `DumpResult`.** Adapters throw — `FatalException` for storage-level
  errors; the `io/parquet` layer's `TuringException` propagates — rather than returning
  `DumpResult`. (Per maintainer direction.)
- **Loaders assemble via friend access.** A loader constructs the in-memory type through
  its public ctor and fills private members directly (mirroring `DataPartLoader`/
  `NodeContainerLoader`/etc.), because the in-memory types expose no public "install a
  whole pre-built structure" API. `*ParquetLoader` is added as a `friend` alongside the
  existing binary `*Loader`. (Public-only structures — `PropertyIndexer`,
  `StringPropertyIndexer` — need no friend.)

### 0.1 Since Phase 2 — hardening, memory bounding, compression (committed)

A body of robustness and performance work has landed on the branch on top of Phase 2, short of
the Phase 3 format-dispatch wiring. The Parquet path is still driven directly (the
`parquet-roundtrip` sample and the tests), not yet through `GraphSerializer`.

- **Corrupt-dump hardening.** Every loader declares the schema it expects to
  `ParquetReader::setExpectedSchema` — column count, per-column name and physical type (and byte
  width for fixed-len), checked when the file opens before any callback fires
  (`ParquetReader.h:137-142`). Loaders validate cross-column lengths, edge-range bounds and
  embedding dimensions and throw `FatalException` rather than reading out of bounds: edge-indexer
  ranges and label-set spans are bounds-checked against the edge arrays
  (`EdgeIndexerParquetLoader.cpp:151-292`); property-indexer ranges are checked against the
  loaded container's value count (`DataPartParquetLoader.cpp:170-187`); the `EdgeContainer`
  loader refuses an out/in file whose first-id metadata is missing or disagrees between the two
  files (`EdgeContainerParquetLoader.cpp:148-156`); `info` / `graph-info` files must have exactly
  one row; a commit datapart count larger than the dump carries is refused
  (`CommitParquetLoader.cpp`); `GraphParquetLoader` refuses a target graph that already has
  history (`GraphParquetLoader.cpp:113-119`). Metadata scalars parse through a shared
  `parseMetadataUint64` (`std::from_chars`, full-string) that turns a malformed value into a
  `FatalException` instead of a leaked `std::invalid_argument`/`out_of_range`
  (`ParquetMetadataParsing.{h,cpp}`). The writer refuses a string longer than the
  `parquet::ByteArray` 32-bit length rather than truncating (`ParquetWriter.cpp:208-213`); the
  prefix-tree `setChild`/`getChild`/`indexToChar` carry `>= ALPHABET_SIZE` bounds
  (`StringIndex.cpp:32-55`).

- **Safe-publish dump.** `GraphParquetDumper::dump` takes the version-controller lock like the
  binary `GraphDumper`, writes into a sibling `<dir>.dumping` temp directory, and renames it over
  the final path on success, so a partial dump (crash, disk full) is never mistaken for a
  complete one; a leftover temp directory is a crashed dump and is removed
  (`GraphParquetDumper.cpp:72-133`). A dump-wide format version is stamped into
  `graph-info.parquet`'s key/value metadata and checked first on load — an absent or mismatched
  version is rejected before anything else is interpreted (`GraphParquetLayout.h:20-21`,
  `GraphParquetLoader.cpp:47-74`).

- **Shell commits and orphaned ancestor dataparts.** A full dump walks every commit. A shell
  ancestor (a lazily-loaded commit whose `CommitData` has expired) has no in-memory data and
  dumps only its metadata file with empty datapart lists; once a loaded graph is committed to,
  the previous head becomes a shell whose dataparts are still referenced through the new head's
  `allDataparts`, so both dumpers now dump any datapart the per-commit walk did not already write,
  keeping the dump loadable (`GraphParquetDumper.cpp:113-125`; binary `CommitDumper.cpp` /
  `GraphDumper.cpp`).

- **Memory-bounded row groups.** The edge and embedding dumpers no longer materialise a whole
  column before touching the writer; they flatten one bounded row group at a time against a
  512 MiB scratch budget (`EdgeContainerParquetDumper.cpp:32-86`,
  `PropertyContainerParquetDumper.cpp:40-126`). On load, every value type now streams straight
  into its container as the reader delivers each row-group batch — embeddings, edges, and (since
  the per-column accumulation vectors were removed) the scalar and string property columns — so
  load no longer transiently doubles the largest column (`PropertyContainerParquetLoader.cpp`,
  `EdgeContainerParquetLoader.cpp`). The embedding container's `sort()` skips the reorder when the
  ids are already ascending (the bulk-add case), which otherwise copied every embedding into a
  fresh container at commit time (`PropertyContainer.cpp:42-58`).

- **ZSTD compression.** All columns are written with `ZSTD`, set once on the writer's
  `WriterProperties` (`ParquetWriter.cpp:91-93`).

- **Shared layout and read helpers.** Column names and metadata keys live in per-area
  `*ParquetLayout.h` headers so the dumper and loader sides cannot drift; `ParquetFileReading`
  wraps the open-validate-drain sequence every loader runs (`ParquetFileReading.{h,cpp}`).

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

   > The one deliberate exception, matching the binary format, is
   > `EdgeIndexer::_patchNodeOffsets`: it is not written and is rebuilt on load by
   > recovering each patch node's id from its first edge ([§5.4](#54-edgeindexer)).
   > It is a pure `NodeID → index` lookup over the already-loaded `_patchNodes` and
   > the `EdgeContainer`, with no sort or tree build, so reconstructing it is O(patch
   > nodes) and cannot diverge from the loaded state — the cost principle 1 guards
   > against does not apply. Keeping the Parquet path identical to the binary
   > `EdgeIndexerLoader` here is worth more than the marginal self-containment.

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
| `UInt64` / IDs (`EntityID`/`NodeID`/`EdgeID`/`LabelSetID`/`EdgeTypeID`/`PropertyTypeID`/`size_t`) | `INT64`, logical `UINT_64` | bits round-trip via a two's-complement reinterpret at the `Int64Writer`/`Int64Reader` boundary; the `UINT_64` annotation keeps Parquet min/max stats unsigned-correct (verified by the `UInt64BoundaryRoundTrip` test seeding `2⁶³` and `uint64::max`) |
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
  info.parquet                        # 1 row: data_part_id, first_node_id, first_edge_id
  node-records.parquet                # labelset_id                      (row i = node firstNodeID+i)
  node-ranges.parquet                 # labelset_id, first_node_id, count
  edges-out.parquet                   # edge_id, node_id, other_id, edge_type_id
  edges-in.parquet                    # edge_id, node_id, other_id, edge_type_id
  edge-indexer-nodedata.parquet       # out_first, out_count, in_first, in_count   (row i = node i)
  edge-indexer-out-spans.parquet      # labelset_id, offset, count
  edge-indexer-in-spans.parquet       # labelset_id, offset, count
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

The commit-level structures (`labels`, `edge-types`, `property-types`, `labelsets`,
`journal`, `tombstones`, `metadata`; `storage/dump/CommitDumper.cpp:23-178`) are small.

**GraphMetadata schema maps** — `GraphMetadataParquet{Dumper,Loader}` (✅ committed), one
file each in the commit directory (paths owned by `CommitParquetLayout.h`, the per-commit
analogue of `DataPartParquetLayout.h`). Each map's ids are assigned sequentially by
`getOrCreate` (`LabelMap`/`EdgeTypeMap`/`PropertyTypeMap`/`LabelSetMap` have no explicit
`add(id, ...)`), so entries are dumped in id order and reloaded via `getOrCreate`,
reproducing the ids — the loader asserts the reassigned id matches the dumped one:

- `labels.parquet` — `label_id`, `name`
- `edge-types.parquet` — `edge_type_id`, `name`
- `property-types.parquet` — `property_type_id`, `value_type`, `name`
- `labelsets.parquet` — `labelset_id`, `integer_0..integer_3` (the four `uint64` of the
  `LabelSet`, with `static_assert(LabelSet::IntegerCount == 4)`; bitmasks with the high bit
  set round-trip through the `INT64` columns, exercised by the test's label-id-63 set)

`GraphMetadataParquetLoader` is a `friend` of `GraphMetadata` (to reach the private maps);
the maps fill through their public `getOrCreate`. Round-trip-tested (incl. an empty-metadata
case) and gated by `GraphMetadataComparator::same`.

**Commit journal** — `CommitJournalParquet{Dumper,Loader}` (✅ committed). The persisted
journal state is the node and edge write sets; the property write sets are transient
change-application state (untouched by `clear`/`empty`/`finalise`, not persisted by the
binary format, not checked by `CommitComparator`) and are not written. The two write sets
have independent row counts, so they split into two single-column files:

- `journal-nodes.parquet` — `node_id`, one row per written node in write-set order
- `journal-edges.parquet` — `edge_id`, one row per written edge in write-set order

`CommitJournalParquetLoader` is a `friend` of `CommitJournal` (to reach the raw write-set
vectors); ids are read straight back in dumped (sorted/unique-from-`finalise`) order,
nothing re-sorted. Round-trip-tested (incl. an empty-journal case) and gated by
`WriteSetComparator`.

**Tombstones** — `TombstonesParquet{Dumper,Loader}` (✅ committed). Same shape as the journal:
the two deleted-id sets split into `tombstone-nodes.parquet` (`node_id`) and
`tombstone-edges.parquet` (`edge_id`), one row per tombstoned id. The sets are unordered
(`std::unordered_set`), so row order is arbitrary and the loader inserts ids back into the
sets (no order to preserve). `TombstonesParquetLoader` is a `friend` of `Tombstones` (to
reach the private mutable sets). Round-trip-tested over tombstones produced by a real
two-commit `GraphWriter` delete (incl. an empty case), gated by `TombstoneSetComparator`.

**Commit metadata** — `CommitMetaDataParquet{Dumper,Loader}` (✅ committed). The binary
`metadata` file holds `numNodes`, `numEdges`, the commit-datapart count, and the id list of
*all* dataparts in the commit's history (not author/message/timestamp — that draft
description predated reading the code). The Parquet counterpart writes one
`commit-metadata.parquet`: the all-datapart ids as a `data_part_id` column (row count =
`num_all_dataparts`), with `num_nodes` / `num_edges` / `num_commit_dataparts` in the file's
key/value metadata (so they survive even a zero-datapart commit, whose file has no row
group). The dumper takes a `const Commit&` (like the binary dumper); the loader fills a
source-independent `CommitParquetMetaData` struct (num scalars + `std::vector<DataPartID>`),
which the wiring step will map onto the `Commit` / `CommitHistoryBuilder`. Round-trip-tested
over a real two-commit `GraphWriter` graph.

**Graph / commit orchestration** — `GraphParquet{Dumper,Loader}` + `CommitParquet{Dumper,Loader}`
(✅ committed), the Parquet analogues of the binary `GraphDumper`/`CommitDumper` /
`GraphLoader`/`CommitLoader`. They live in `storage/dump/parquet/` (so Arrow stays out of core
`storage`), throw rather than return `DumpResult`, and own a `GraphParquetLayout.h` for the
graph-directory paths:

```
graphDir/
  graph-info.parquet   # 1 row: graph_id, name
  commit-log.parquet   # commit_hash, one row per commit, oldest first
  commits/<hash>/      # one Parquet commit directory per commit (CommitParquetDumper)
  dataparts/<id>/      # shared per-DataPart Parquet directories (DataPartParquetDumper)
```

`GraphParquetDumper` walks the commit chain from head using only the graph's public
accessors (no friend access); `CommitParquetDumper` takes a `const Commit&` and drives the
four commit-level adapters plus the `DataPart` adapter for each of the commit's own
dataparts. On load, `GraphParquetLoader` (friend of `Graph`/`VersionController`) recreates
the `VersionController`, builds every commit shell via `CommitParquetLoader::load` (counts
from `commit-metadata.parquet`), then materializes **only the head commit's** full data via
`CommitParquetLoader::loadData` — matching the binary `GraphLoader`. `CommitParquetLoader`
(friend of `Commit`/`CommitData`/`CommitHistory`) fills the metadata maps, journal,
tombstones and each datapart (created through `VersionController::createDataPart`, so the
parts are Arc-owned and registered in the part map). `DataPartParquetLoader` gained a
`load(DataPart&, …)` overload that fills a controller-created part in place, sharing its
body with the standalone `unique_ptr` loader.

The full dump→load round-trip is gated end-to-end by `GraphComparator::same` on the
miniature **Reactome** sample graph (`examples/ReactomeSampleGraph`, also now the shared
fixture for `ReactomeTest`), in `test/storage/dump/parquet/GraphParquetLoaderTest.cpp`.

### 4.4 Row groups and compression

All columns are compressed with `ZSTD`, set once on the writer's `WriterProperties`
(`ParquetWriter.cpp:91-93`); no per-column dictionary/encoding tuning yet. Row groups are sized
by a **memory budget, not a fixed row count**: the edge and embedding dumpers flatten one row
group at a time against a 512 MiB scratch buffer so the dumper's transient footprint stays flat
regardless of edge/embedding count (`EdgeContainerParquetDumper.cpp:32-86`,
`PropertyContainerParquetDumper.cpp:40-126`); the smaller structures write a single row group.
Each loader streams every row-group batch straight into the target container as it arrives, so
neither dump nor load transiently doubles the largest column.

Measured on `ogbn_papers100m` + 256-dim embeddings, the row-group streaming dropped the ZSTD
dump's peak memory from 273.7 GiB to 228.9 GiB, fitting in RAM without swap. Forward: align
row-group boundaries to node-ID / partition ranges for partial on-disk scans. `BYTE_STREAM_SPLIT`
is not applicable to `FIXED_LEN_BYTE_ARRAY` embeddings (revisit with the embedding effort).

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
  buffer before `WriteBatch`; the transpose runs one bounded row group at a time (512 MiB),
  not over the whole column ([§4.4](#44-row-groups-and-compression)).
- Scalars `firstEdgeID`, `firstNodeID` in metadata. The loader cross-checks that the out- and
  in-edge files carry the same first ids and refuses a file missing them
  (`EdgeContainerParquetLoader.cpp:148-156`).

### 5.3 PropertyManager containers (`storage/properties/PropertyContainer.h:117,221,300`)

Per property type, the container exposes contiguous `ids()` and `all()`:

- **`node-props-<ptID>.parquet`** / **`edge-props-<ptID>.parquet`** — `entity_id`
  (`INT64`) + `value` (type per [§4.1](#41-type-mapping)). Trivial types
  (`Int64`/`UInt64`/`Double`/`Bool`) write `all()` near-zero-copy. Strings build
  `ByteArray` descriptors over the existing `string_view` bytes. Embeddings write
  each view's flat floats as `FIXED_LEN_BYTE_ARRAY(byteWidth = dim×4)`; dimension in
  metadata. On load every type streams straight into its `TypedPropertyContainer<T>` as each
  row-group batch arrives (the entity-id column is read ahead of the value column within each
  chunk), so no per-column accumulation vector is held ([§4.4](#44-row-groups-and-compression)).

### 5.4 EdgeIndexer (`storage/indexers/EdgeIndexer.h:60-84`)

Members: `_firstNodeID`, `_firstEdgeID`, `std::vector<NodeEdgeData> _nodes`,
`std::span<NodeEdgeData> _coreNodes`/`_patchNodes`,
`std::unordered_map<NodeID,size_t> _patchNodeOffsets`,
`LabelSetIndexer<EdgeSpans> _outLabelSetSpans`/`_inLabelSetSpans`.
`NodeEdgeData = {OutEdgeRange{first,count}, InEdgeRange{first,count}}`.

- **`edge-indexer-nodedata.parquet`** — one row per `_nodes` entry, in order:
  `out_first`, `out_count`, `in_first`, `in_count`. `_nodes` is laid out
  **patch-prefix, core-suffix** (`EdgeIndexer.cpp:34-35`), so on load
  `_patchNodes = [0, patchNodeCount)` and
  `_coreNodes = [patchNodeCount, patchNodeCount + coreNodeCount)`. Both counts are
  stored; the spans are sliced, not recomputed. (The original draft said the boundary
  is `coreNodeCount` — wrong; the layout is patch-first, so the slice keys off
  `patchNodeCount`. The review caught this; the implementation slices correctly.)
- **`edge-indexer-out-spans.parquet`** / **`edge-indexer-in-spans.parquet`** — one
  row per span, grouped by labelset in map order: `labelset_id`, `offset`, `count`.
- `_patchNodeOffsets` is **not written**. The loader rebuilds it exactly as the binary
  `EdgeIndexerLoader` does: for each of the first `patchNodeCount` entries of `_nodes`
  (the patch prefix), it recovers the node id from that node's first out-edge
  (`outs[outRange.first]._nodeID`), or its first in-edge if it has no out-edges, and maps
  that id to the patch index. This is the one place the Parquet path reconstructs rather
  than reads back — a cheap O(patch nodes) lookup with no sort or tree build (see the
  exception under principle 1).
- Scalars `firstNodeID`, `firstEdgeID`, `coreNodeCount`, `patchNodeCount` in metadata.

### 5.5 PropertyIndexer (`storage/indexers/PropertyIndexer.h`)

`using PropertyIndexer = std::unordered_map<PropertyTypeID, LabelSetIndexer<std::vector<PropertyRange>>>`.
`PropertyRange = {size_t _offset, size_t _count}`.

- **`node-prop-indexer.parquet`** / **`edge-prop-indexer.parquet`** — one row per
  `PropertyRange`, grouped by `(property_type_id, labelset_id)` in map order:
  `property_type_id`, `labelset_id`, `offset`, `count`. Range index is positional
  (row order within a group); group counts are read from the grouping, not stored.
  `PropertyIndexer` is a public map (`unordered_map<...>`), so this needs no friend.
  A flat row-per-range layout cannot represent an empty group (a property type with no
  label sets, or a label set with no ranges) — those do not occur for a built index, so
  the dumper **throws** if it hits one rather than silently dropping it (closing the
  reviewer's empty-group concern).

### 5.6 StringPropertyIndexer / StringIndex (`storage/indexes/StringIndex.h:26-145`)

`StringIndex` members: `_nextFreeID`, `std::vector<std::unique_ptr<PrefixTreeNode>>
_nodeManager`, `PrefixTreeNode* _root`. `PrefixTreeNode = {std::vector<EntityID>
_owners, std::vector<PrefixTreeNode*> _children (size ALPHABET_SIZE), size_t _id}`.
`StringPropertyIndexer` holds `std::map<PropertyTypeID, std::unique_ptr<StringIndex>>`.

The tree is already linearized to a node array on disk: `node._id` == its index in
`_nodeManager`; children are written as `(alphabet_index, child_id)` pairs. Faithful
columnization:

- **`*-string-index-indexes.parquet`** — `property_type_id`, `node_count`: one row per
  indexed property type. Node ids are dense (`0..node_count-1`) by construction, so the
  per-node array is *not* stored separately.
- **`*-string-index-children.parquet`** — `property_type_id`, `parent_node_id`,
  `child_index` (0..ALPHABET_SIZE-1), `child_node_id`: one row per non-null child link.
- **`*-string-index-owners.parquet`** — `property_type_id`, `node_id`, `entity_id`:
  one row per owner (replaces the binary `-owners` aux file).

On load, `StringIndex(node_count)` pre-allocates the dense node array (root = node 0,
`_nextFreeID = node_count` — both set by the ctor, so neither is stored), then child
links (`setChild`) and owners (`addOwner`) are applied through **public** APIs — no
friend access, and the tree is *not* rebuilt by re-inserting strings. The
`BYTE_ARRAY`-blob fallback the draft reserved was not needed; the flattened tables are
clean and the round-trip is gated by `StringIndexerComparator::same` (node count, node
ids, owners in order, child links).

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
- All columns are written with `ZSTD` (`WriterProperties`, `ParquetWriter.cpp:91-93`); a string
  value longer than the `parquet::ByteArray` 32-bit length is refused, not truncated
  (`.cpp:208-213`).
- The reader gained `setExpectedSchema`: a loader declares the schema the file must carry
  (column count, names, physical types, fixed-len widths), checked when the file opens before any
  callback (`ParquetReader.h:137-142`). Every `storage/dump/parquet` loader uses it, via the
  `ParquetFileReading` open-validate-drain helper.

### 6.2 `storage/dump/parquet/` — per-structure encoders/decoders

A sibling lib `turing_db_storage_dump_parquet_s` (links `turing_db_storage_s` +
`turing_db_io_parquet_s`), so Arrow stays out of core `storage`. One pair per structure:
`PropertyContainerParquet{Dumper,Loader}`, `NodeContainerParquet*`, `EdgeContainerParquet*`,
`EdgeIndexerParquet*`, `PropertyIndexerParquet*`, `StringPropertyIndexerParquet*`, plus the
`DataPartParquet{Dumper,Loader}` orchestrator and a shared `DataPartParquetLayout.h` that
owns the per-part filenames.

- **Exceptions, not `DumpResult`.** `dump` returns `void` and throws on failure; `load`
  returns the built object (`std::unique_ptr<...>`) or fills an output reference, and
  throws. `FatalException` for storage-level errors; the `io/parquet` layer's
  `TuringException` propagates.
- **Loaders read columns straight into the in-memory members — no build algorithms
  re-run.** IDs are reconstructed from raw `int64`; span members (`_coreNodes`,
  `_patchNodes`, the edge-indexer label-set spans) are sliced / rebound from the loaded
  vectors and the supplied `EdgeContainer`; prefix-tree child pointers are resolved by id
  lookup; label-set handles are bound through the supplied `LabelSetMap`.
- **Friend access where needed.** `NodeContainer`, `EdgeContainer`, `EdgeIndexer`, and
  `DataPart` + `PropertyManager` expose no public "install a whole structure" API, so each
  `*ParquetLoader` is added as a `friend` beside the binary `*Loader` and fills private
  members (or calls the private ctor) directly. `PropertyIndexer` and
  `StringPropertyIndexer` are buildable through public APIs and need no friend.

The `DataPart` orchestrator walks the part's public accessors and writes ~18 files into a
part directory — including a 1-row `info.parquet` of `data_part_id` / `first_node_id` /
`first_edge_id`. The loader constructs the `DataPart` via its public ctor, then assembles
every piece via friend access, binding handles/spans to the supplied `LabelSetMap` and the
loaded `EdgeContainer`. Per-property files are enumerated from the directory
(`node-props-<ptID>.parquet`, parsed with `std::from_chars`).

### 6.3 Orchestration and format dispatch

- `GraphFileType` (`storage/dump/GraphFileType.h:9-13`) gains `PARQUET`.
- `GraphDumper`/`CommitDumper` select the encoder family by configured format
  (default `BINARY` until Phase 3); the `type` marker records the choice.
- Loaders (`GraphSerializer`/`CommitLoader`/`DataPartLoader`) read the `type` marker
  and dispatch. Old dumps keep loading via the unchanged binary path.

None of this is built yet — `GraphFileType` still carries only `GML` and `BINARY`, and the
Parquet path is invoked directly. Independent of this marker, a Parquet dump already self-versions
through `graphParquetLayout::FORMAT_VERSION` in `graph-info.parquet`'s metadata, so a
forward-incompatible layout change is rejected on load regardless of how dispatch lands
([§0.1](#01-since-phase-2--hardening-memory-bounding-compression-committed)).

---

## 7. Phasing

**Phase 0 — writer primitive. ✅ Done.** `ParquetWriter` + `ParquetWriteSchema` +
round-trip unit tests (every column type, incl. `FIXED_LEN_BYTE_ARRAY` and the `UINT_64`
boundary).

**Phase 1 — DataPart, fully faithful. ✅ Done.** All six structure adapters
([§5](#5-faithful-column-schemas)) + the `DataPart` orchestrator, each round-trip-tested;
the capstone builds `SimpleGraph` and asserts `DataPartComparator::same`, plus a
two-commit test that forces and verifies an EdgeIndexer patch node (the
`_patchNodeOffsets` round-trip).

**Phase 2 — commit-level metadata. ✅ Done.** `GraphMetadata` schema maps, commit journal,
tombstones, and commit metadata adapters (all round-trip-tested), plus the
`GraphParquet{Dumper,Loader}` / `CommitParquet{Dumper,Loader}` orchestrators that emit and
read a full Parquet graph directory. Full dump→load regression via `GraphComparator::same`
on the Reactome sample (`GraphParquetLoaderTest`).

**Phase 3 — format dispatch + back-compat.** `GraphFileType::PARQUET` marker; loaders
([§6.3](#63-orchestration-and-format-dispatch)) dispatch on it; parametrize the regression
suite over both formats; flip the default to `PARQUET`. Old binary dumps keep loading.

**Phase 4 — cleanup & tuning. 🔶 In progress.** ZSTD compression and memory-bounded row-group
streaming are committed ([§4.4](#44-row-groups-and-compression)); the `parquet-roundtrip` sample
([§9](#9-testing)) benchmarks dump size, dump/load time and peak memory against the binary
serializer. Remaining: row-group alignment to node ranges for partial on-disk scans, per-column
encoding tuning, and — once existing dumps are migrated — sunsetting the binary dumpers and the
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
- **Negative-path loader tests:** corrupt/short/retyped dumps, out-of-bounds ranges,
  mismatched first-id metadata, oversized commit datapart counts and a non-empty target graph
  each raise `FatalException` rather than reading out of bounds (`test/storage/dump/parquet/`).
- **Benchmark sample:** `samples/parquet-roundtrip` loads a binary dump, round-trips it through
  the Parquet path and asserts `GraphComparator::same`; `-dump-only` / `-load-parquet` /
  `-dump-binary` / `-load-only` / `-add-random-embeddings` isolate dump or load timing, exercise
  the binary path, and attach deterministic random embeddings for size/time/peak-memory
  comparison.
- **Regression (Phase 2–3):** `regress/` dump/load suites parametrized over `BINARY`
  and `PARQUET`.
- **Back-compat:** a checked-in binary dump that must keep loading after the default flips.

---

## 10. Risks and open questions

- **String prefix-tree columnization** ([§5.6](#56-stringpropertyindexer--stringindex-storageindexesstringindexh26-145))
  — *resolved.* The flattened node/children/owners tables round-trip cleanly under
  `StringIndexerComparator::same`; the opaque-`BYTE_ARRAY`-blob fallback the draft reserved was
  not needed.
- **`UInt64`/`size_t` round-trip** — *resolved.* Full-range lossless round-trip through the
  `INT64`/`UINT_64` encoding is covered by the `UInt64BoundaryRoundTrip` test
  ([§4.1](#41-type-mapping)).
- **`coreNodes`/`patchNodes` span restoration** — *resolved.* The slice keys off
  `patchNodeCount` (the layout is patch-first, not `coreNodeCount`); verified by the two-commit
  patch-node equality test ([§5.4](#54-edgeindexer)).
- **Load-time CPU** — Parquet decode is more work than a near-memory-image binary load (though we
  avoid index *rebuild*, the bigger cost). The `parquet-roundtrip` sample now times dump and load
  in isolation ([§9](#9-testing)); parallelize decode via `JobSystem` if a 100 M-node graph
  proves it necessary.
- **Small-file overhead** — a part with many property/index tables yields many small
  Parquet files (footer overhead each). Phase 4 may consolidate; faithful
  decomposition first.

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
| Reader expected-schema validation | `io/parquet/ParquetReader.h:137-142` |
| Low-level Parquet write APIs in use | `test/io/ParquetReaderTest.cpp:124-161` |
| ParquetWriter ZSTD; oversized-string refusal | `io/parquet/ParquetWriter.cpp:91-93,208-213` |
| Dump format version; temp-dir + rename publish | `storage/dump/parquet/GraphParquetLayout.h:20-21`; `GraphParquetDumper.cpp:72-133` |
| Strict metadata scalar parsing | `storage/dump/parquet/ParquetMetadataParsing.cpp:11-23` |
| Open-validate-drain read helper | `storage/dump/parquet/ParquetFileReading.h` |
| Loader bounds checks (edge ranges, property ranges, first-id cross-check) | `storage/dump/parquet/EdgeIndexerParquetLoader.cpp:151-292`; `DataPartParquetLoader.cpp:170-187`; `EdgeContainerParquetLoader.cpp:148-156` |
| Memory-bounded row-group streaming; ascending-id sort skip | `storage/dump/parquet/EdgeContainerParquetDumper.cpp:32-86`; `PropertyContainerParquetDumper.cpp:40-126`; `storage/properties/PropertyContainer.cpp:42-58` |
| Shell-commit / orphaned-datapart dump | `storage/dump/parquet/GraphParquetDumper.cpp:113-125`; `storage/dump/CommitDumper.cpp`; `GraphDumper.cpp` |
| Benchmark / round-trip sample | `samples/parquet-roundtrip/main.cpp` |
| Parquet CMake wiring | `CMakeLists.txt:222`; `io/parquet/CMakeLists.txt` |
