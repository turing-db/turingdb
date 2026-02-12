# Lazy Commits

## Problem Statement

When TuringDB loads a graph from disk, `GraphLoader` eagerly loads every commit
and every DataPart into memory. Each commit inherits all previous DataParts via
`CommitHistory::newCommitHistoryFromPrevious`, so by the time HEAD is reached,
every DataPart that has ever existed is resident in memory.

This is wasteful in at least two scenarios:

1. **Historical queries.** To open a transaction at commit N, all commits after N
   (and their DataParts) are loaded unnecessarily.
2. **Post-merge commits.** A merge commit consolidates data into new DataParts,
   making all pre-merge DataParts redundant for HEAD queries. Yet they are all
   loaded at startup.

## Goals

- Load only the data required for the commits that are actually accessed.
- Eagerly load HEAD at startup (since most operations target it).
- Keep all other commits as lightweight stubs that can be hydrated on demand.
- Introduce a CommitLog that provides fast commit history iteration without
  requiring loaded DataParts.
- Restructure the on-disk layout so DataParts are stored independently of
  commits and referenced by a stable persistent ID.

## Non-Goals / Out of Scope

- **Eviction.** Once a commit or DataPart is loaded, it stays in memory forever.
  LRU eviction or unloading is a future concern.
- **Lazy dumping.** The dump path is not changed by this spec (beyond the new
  layout). Lazy/incremental dump optimizations are future work.
- **Migration tooling.** Graphs written with the old format must be re-dumped.
  No automatic migration is provided.

---

## Current Architecture

### On-Disk Layout (Before)

```
<graph-path>/
  info
  type
  commit-0-<hash>/
    metadata/
    journal
    tombstones
    merge                   # empty marker file (merge commits only)
    datapart-0/
    datapart-1/
    ...
  commit-1-<hash>/
    ...
```

DataParts are nested under the commit that created them. The commit index
prefix (`commit-0-`, `commit-1-`) encodes ordering.

### Loading (Before)

`GraphLoader::load` iterates every `commit-*` directory in order. For each one,
`CommitLoader::load`:

1. Creates a `Commit` with a new `CommitData` via `VersionController::createCommitData`.
2. Calls `newCommitHistoryFromPrevious` to copy the parent's `_allDataparts`.
3. Loads metadata, journal, and tombstones from files.
4. Loads every `datapart-*` directory via `DataPartLoader::load` (full
   deserialization of nodes, edges, properties, indexers).
5. Adds DataParts to `CommitHistory` via `CommitHistoryBuilder`.

Result: all commits and all DataParts are fully materialized at startup.

### CommitHistory (Before)

`CommitHistory` stores:
- `_allDataparts` -- `vector<WeakArc<DataPart>>`, all DataParts visible to this
  commit (inherited + own).
- `_commitDataparts` -- `span<WeakArc<DataPart>>` into the tail of
  `_allDataparts`, covering only this commit's own DataParts.
- `_commits` -- `vector<CommitView>`, the full commit chain up to this commit.
- `_journal` -- `unique_ptr<CommitJournal>`.

The `_commits` vector is used by `HistoryProcedure`, `DescribeCommitProcedure`,
`DBServerProcessor::commit_log`, and `GraphReader::isNodeDeleted`. In most
cases, it is either iterated for display or only the last entry is accessed.

---

## Proposed Changes

### 1. DataPart Persistent Identity

DataParts receive a new persistent identifier: a random `uint64_t`, analogous to
`CommitHash`. This ID is assigned at creation time and persisted with the
DataPart.

```cpp
// New type, same pattern as CommitHash
class DataPartID {
    uint64_t _id;
public:
    static DataPartID create();  // random
    uint64_t get() const;
    // hash, equality, etc.
};
```

`DataPart` gains a `_dataPartID` field. `VersionController::createDataPart`
assigns it. The ID is written to the DataPart's `info` file on dump and read
back on load.

### 2. New On-Disk Layout

```
<graph-path>/
  info
  type
  commitlog                     # new: graph-level commit log file
  dataparts/                    # new: shared DataPart storage
    datapart-<id>/
      info
      nodes
      edges
      edge-indexer
      node-props-*
      edge-props-*
      node-prop-indexer
      edge-prop-indexer
      node-string-prop-indexer
      node-string-prop-indexer-owners
      edge-string-prop-indexer
      edge-string-prop-indexer-owners
  commits/                      # new: commit directories (no index prefix)
    commit-<hash>/
      metadata/
      journal
      tombstones
      manifest                  # new: references DataParts by ID
```

Key changes from the old layout:
- DataParts move to a top-level `dataparts/` directory, named by their
  persistent `DataPartID`.
- Commit directories move under `commits/` and drop the index prefix (ordering
  comes from the commitlog file).
- The `merge` marker file is removed. Merge status is stored in the manifest.
- A graph-level `commitlog` file stores the commit ordering and summary stats.

### 3. Commit Manifest File

Each commit directory contains a `manifest` file that describes which DataParts
the commit references, without requiring ancestor commits to be loaded.

**Binary format:**

| Field                    | Type         | Description                                         |
|--------------------------|--------------|-----------------------------------------------------|
| `allDataPartsCount`      | `uint64_t`   | Number of entries in allDataParts                   |
| `allDataPartIDs`         | `uint64_t[]` | Array of DataPartIDs (length = allDataPartsCount)   |
| `commitDataPartsStart`   | `uint64_t`   | Start index of commitDataParts span in allDataPartIDs |
| `isMergeCommit`          | `uint8_t`    | 1 if merge commit, 0 otherwise                     |

The `commitDataParts` span is reconstructed as
`allDataPartIDs[commitDataPartsStart .. allDataPartsCount]` (it always extends
to the end of the array).

A commit's manifest makes it fully self-describing: given the manifest and the
shared `dataparts/` folder, a commit can be hydrated independently without
loading any ancestor commits.

### 4. CommitLog

#### CommitLog Struct

A lightweight struct that summarizes a commit without requiring its data to be
loaded:

```cpp
struct CommitLog {
    CommitHash _hash;
    uint64_t _numDataParts;
    uint64_t _numNodes;
    uint64_t _numEdges;
};
```

#### Ownership

`VersionController` owns the commit log as `std::vector<CommitLog>`. This
replaces the role that `CommitHistory::_commits` (the `vector<CommitView>`)
previously served for history iteration.

Each `Change` owns a commit log as well.

#### Graph-Level Commitlog File

**Binary format:**

| Field            | Type             | Description                          |
|------------------|------------------|--------------------------------------|
| File header      | fixed            | Magic bytes + version                |
| `numCommits`     | `uint64_t`       | Number of commit entries             |
| Commit entries   | `CommitLog[]`    | Array of CommitLog structs           |

The file is appendable: when new commits are dumped, seek to the `numCommits`
offset, overwrite the count, then append the new entries at the end of the file.

If the file is ever found to be inconsistent (e.g., crash during append), it can
be reconstructed from the commit directories on disk.

#### CommitLog Population

- **At load time:** Read from the commitlog file.
- **At runtime:** When a new commit is created via `Change::submit` or `Change::commit`, a
  `CommitLog` entry is appended to the vector.
- **At dump time:** New entries are appended to the commitlog file.

### 5. CommitHistory Refactoring

Remove `std::vector<CommitView> _commits` from `CommitHistory`. This vector
encoded the commit chain inside each commit's data, which couples per-commit
state with global history.

**Consumers and their migration:**

| Consumer                     | Current usage                            | New approach                                    |
|------------------------------|------------------------------------------|-------------------------------------------------|
| `HistoryProcedure`           | Iterates `_view.commits()` for stats     | Read from `VersionController::commitLog()`      |
| `DescribeCommitProcedure`    | Finds commit by hash, reads stats        | Look up in CommitLog by hash                    |
| `DBServerProcessor`          | Iterates commits for log display         | Read from CommitLog                             |
| `GraphReader::isNodeDeleted` | Accesses `commits().back().tombstones()` | Access tombstones directly from CommitData      |
| `CommitBuilder`              | Accesses `commits().back()` for prev     | Receives previous commit reference explicitly   |

After this refactoring, `CommitHistory` retains only:
- `_allDataparts` -- `vector<WeakArc<DataPart>>`
- `_commitDataparts` -- `span<WeakArc<DataPart>>`
- `_journal` -- `unique_ptr<CommitJournal>`

### 6. Lazy Commit Loading

#### Commit Stub

At startup, `GraphLoader` reads the commitlog file and creates a lightweight
`Commit` object for each entry -- containing only the `CommitHash` and a null
`_data` pointer. The existing `Commit::hasData()` method distinguishes stubs
from hydrated commits.

`VersionController` is populated with these stubs so `_commits` and `_offsets`
are fully available for commit lookup.

HEAD is then eagerly hydrated (see below).

#### Hydration

When `openTransaction(hash)` targets an unhydrated commit, hydration is
triggered:

1. Resolve the commit's directory path from its hash: `commits/commit-<hash>/`.
2. Load metadata, journal, and tombstones from the commit directory.
3. Read the manifest to get the list of `DataPartID`s.
4. For each DataPartID:
   - Check if the DataPart is already loaded (lookup in a
     `DataPartID -> WeakArc<DataPart>` map on `VersionController`).
   - If not loaded, load it from `dataparts/datapart-<id>/` via
     `DataPartLoader::load` and register it in the map.
   - Obtain the `WeakArc<DataPart>` reference.
5. Reconstruct `_allDataparts` and `_commitDataparts` from the manifest.
6. Set `Commit::_data` to the newly created `CommitData`.

Since DataParts are shared via `ArcManager` and looked up by `DataPartID`, a
DataPart loaded for one commit is reused by any other commit that references it.

#### HEAD Eager Loading

At startup, after creating all commit stubs, HEAD is immediately hydrated.
This ensures the common case (queries against the latest state) requires no
on-demand loading.

#### DataPart Lookup Map

`VersionController` gains a new map for tracking loaded DataParts by their
persistent ID:

```cpp
std::unordered_map<DataPartID, WeakArc<DataPart>> _loadedDataParts;
```

This is consulted during hydration to avoid loading the same DataPart twice.

### 7. API Changes

#### `openTransaction` Return Type

Today `Graph::openTransaction` and `VersionController::openTransaction` return
`FrozenCommitTx` directly. Since hydration can fail (I/O errors, corrupt data),
the return type changes to:

```cpp
// Before
FrozenCommitTx openTransaction(CommitHash hash = CommitHash::head()) const;

// After
DumpResult<FrozenCommitTx> openTransaction(CommitHash hash = CommitHash::head());
```

Note: the method also loses `const` since hydration mutates the commit.

Callers must handle the error case. This affects:
- `Graph::openTransaction`
- `VersionController::openTransaction`
- `CommitView::openTransaction`
- `Change` constructor (line 20 in `Change.cpp`)
- All call sites in query pipeline, server, and tests

#### CommitLog Access

```cpp
// On VersionController
std::span<const CommitLog> commitLog() const;
```

Procedures and the server use this instead of `GraphView::commits()`.

### 8. Write Path (Unchanged)

The runtime write path is unaffected:

- `Commit::createNextCommit` continues to call
  `newCommitHistoryFromPrevious` to copy the parent's `_allDataparts`. This
  works because the parent commit (HEAD) is always hydrated.
- `CommitBuilder`, `CommitWriteBuffer`, and `Change` operate as before.
- `Change::submit` appends a `CommitLog` entry to the VersionController's
  vector.

### 9. Dump Path Changes

The dump path is updated for the new on-disk layout:

- **DataParts** are written to `dataparts/datapart-<id>/` instead of nested
  under commit directories. Only `commitDataparts` are dumped per commit (same
  as today).
- **Commits** are written to `commits/commit-<hash>/` with a manifest file
  instead of the merge marker.
- **Commitlog file** is written/appended at the graph level.
- **Incremental dump** checks for existing DataPart directories by ID (instead
  of checking commit directories) and only writes new ones.

---

## Loading Flow (After)

```
GraphLoader::load(graph, path)
  1. Load graph info from <path>/info
  2. Read commitlog file -> vector<CommitLog>
  3. For each CommitLog entry:
     - Create stub Commit (hash only, _data = null)
     - Register in VersionController (_commits, _offsets)
  4. Populate VersionController::_commitLog
  5. Hydrate HEAD commit
     - Load commit directory (metadata, journal, tombstones)
     - Read manifest -> list of DataPartIDs
     - Load each DataPart from dataparts/ directory
     - Register DataParts in _loadedDataParts map
     - Construct CommitData and attach to Commit
```

## Hydration Flow (On Demand)

```
VersionController::openTransaction(hash)
  1. Look up commit in _offsets
  2. If commit.hasData() -> return FrozenCommitTx (fast path)
  3. Else hydrate:
     a. Resolve path: commits/commit-<hash>/
     b. Load metadata, journal, tombstones
     c. Read manifest
     d. For each DataPartID in manifest.allDataPartIDs:
        - If in _loadedDataParts -> use existing WeakArc
        - Else -> DataPartLoader::load, register in map
     e. Build _allDataparts vector and _commitDataparts span
     f. Attach CommitData to Commit
  4. Return FrozenCommitTx
```

---

## Migration

No automatic migration. Graphs written with the old on-disk layout (DataParts
nested under commits, no commitlog file) must be re-dumped with the new format.
The loader does not support the old layout.

## Future Work

- **Eviction.** Unload DataParts and CommitData when no transaction holds a
  reference, using LRU or reference-count-based eviction.
- **Lazy dump optimizations.** Defer writing DataParts to disk, batch writes.
- **Commitlog crash recovery.** Detect and rebuild a corrupt commitlog file from
  commit directories on disk.
