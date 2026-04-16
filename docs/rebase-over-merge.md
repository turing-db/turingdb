# Rebasing a Change Across a Datapart Merge Commit — Specification

This document describes the **design** of the machinery required to allow
`MERGE_DATA_PARTS` to proceed while open `Change` objects exist, by teaching the rebaser
to translate pre-merge entity IDs across the merge commit.

---

## Primer: Change, Rebase, and the Current Limitation

A `Change` is a pending branch off some `baseHash`, holding local commits and a write
buffer. On submit, `VersionController::submitChange` installs those commits onto the
current head. If main has moved since branch time, `ChangeRebaser` first translates the
change's writes from **branch-time ID space** to **current-head ID space** — renumbering
change-local entities, rewriting edge endpoints, and checking for conflicts against
commits that landed on main in the meantime.

The current rebaser (`EntityIDRebaser`, `storage/versioning/EntityIDRebaser.cpp:36-46`)
assumes main is **append-only**: pre-branch IDs are stable, and change-local IDs (`>=
branchTimeNextNodeID`) shift upward by `newNextNodeID - branchTimeNextNodeID`.

A datapart merge violates that assumption on both axes. Pre-branch IDs move (or disappear),
and `newNextNodeID < branchTimeNextNodeID` is possible because the merge compacts main's
ID range. Today, `ChangeManager::mergeDataParts` (`system/ChangeManager.cpp:26-34`)
side-steps the problem by rejecting the merge whenever any `Change` is open, returning
`DataPartMergeErrorType::CHANGES_ON_MAIN`. This spec replaces that guard with a correct
rebase path.

---

## The Three ID Spaces

Throughout this document:

| Space | Definition | Held by |
|---|---|---|
| **Branch-time** | IDs as the change's local commits see them. Pre-branch IDs occupy `[0, B)`; change-local creations occupy `[B, ...)`, where `B` is the allocated counter at `baseHash`. | The `Change`'s dataparts and write buffer. |
| **Pre-merge** (per merge) | The ID space main occupied immediately before that merge commit. A sequence of append-only commits between branch and the merge may have grown `[0, B)` to `[0, B_pre)`. | The merge commit's inputs. |
| **Post-merge** (per merge) | The ID space main occupies immediately after that merge commit. Survivors of `[0, B_pre)` are renumbered compactly into `[0, B_post)`, where `B_post <= B_pre`. | The merge commit's output. |

A rebase may span multiple merges, each with its own `(B_pre, B_post)` pair. The new
head's `getTotalNodesAllocated()` equals the post-merge total of the last merge plus any
append-only commits after it.

---

## Context: Why a Merge Commit Is Different

A datapart merge (`DataPartMerger::merge`, invoked via `VersionController::mergeDataParts`)
rewrites the graph's storage in ways the current rebase pipeline does not handle:

- It produces a single datapart with `_firstNodeID = 0, _firstEdgeID = 0`.
- `DataPart::load` renumbers surviving entities to compact IDs `[0, survivingCount)` via the
  `tmpNodeIDs` mapping (see `storage/DataPart.cpp:95-98`).
- `GraphReader::getTotalNodesAllocated()` is derived from `lastPart.firstNodeID +
  lastPart.containerSize` (`storage/reader/GraphReader.cpp:17-24`), so post-merge it equals
  `survivingCount`, not the pre-merge max. The `EntityIDRebaser` invariant
  `_branchTimeNextNodeID <= _newNextNodeID` no longer holds.
- Tombstones absorbed by the merge disappear from the resulting commit's tombstone set.
- Metadata is copied verbatim (`Commit::createMergeCommit`), so the merge commit itself
  contributes no label, edge-type, or property-type remappings. Append-only commits between
  branch and merge can still evolve metadata, so `MetadataRebaser` is not blanket-identity
  — the merge commit just adds nothing on top of whatever intermediate commits did.
- `Commit::_mergeCommit` (declared at `storage/versioning/Commit.h:73`) is currently never
  set. Flip it to `true` inside `Commit::createMergeCommit`; this is the canonical
  discriminator for "is this a merge commit" across the codebase. Attached `IDRemap`s
  must always accompany a set flag, so also `bioassert(_mergeCommit == (nodeRemap !=
  nullptr) && _mergeCommit == (edgeRemap != nullptr))` at commit load / publication time
  to catch drift.
- `DataPartRebaser::_nodeOffset` and `_edgeOffset` (assigned at `DataPartRebaser.cpp:36-37`)
  are written but never read. They survive only as dead storage and will compute an
  underflowed value when `_newNextNodeID < _branchTimeNextNodeID`. Delete them as part of
  this work; their formula is not load-bearing, only `newFirstNodeID` is.

---

## Approach Summary

1. Each merge commit emits a sparse **`IDRemap`** for nodes and for edges, encoding which
   pre-merge IDs survived and where they landed post-merge. Tombstoned IDs are gaps. The
   tables are attached to the merge commit's `CommitData` and live for its lifetime.
2. The rebaser is generalised from `EntityIDRebaser` to `EntityIDRemapper`, which composes
   a sequence of `IDRemap` tables (one per merge commit in the rebase span) with the
   existing affine shift for change-local IDs. A new `tryRebase*` variant returns
   `std::optional`; the existing asserting `rebase*` is retained for paths proved safe by
   the conflict check.
3. `ChangeConflictChecker` gains (a) a new conflict class for pre-branch IDs the change
   references that a merge dropped — reusing the existing iteration over each
   `CommitBuilder`'s writeBuffer, now routed through `tryRebase*`, (b) per-commit
   projection of main's journal write sets forward through the merges that follow each
   commit, before unioning into `writes`, and (c) an edge-ID-based replacement for the
   DP-index discriminator in `checkNewEdgesIncidentToDeleted`, which breaks under
   merges.
4. Deletion paths (`rebaseTombstones`, journal write sets, write-buffer deletion sets) use
   `tryRebase*` and silently drop `nullopt` entries — both sides agreed to delete.
5. The `CHANGES_ON_MAIN` guard is removed last, once the above machinery is in place.

---

## `IDRemap` — Sparse Run-Encoded Remap

### Structure

```cpp
struct RemapInstruction {
    uint64_t _prePos;    // start of surviving run in pre-merge space (inclusive)
    uint64_t _postPos;   // start of the same run in post-merge space
    uint64_t _length;    // run length
};

class IDRemap {
public:
    // Pre-IDs >= _preTotal are out of this merge's scope (asserted by caller).
    // Returns nullopt when preID falls in a tombstoned gap.
    std::optional<uint64_t> apply(uint64_t preID) const;

    uint64_t preTotal() const { return _preTotal; }
    uint64_t postTotal() const { return _postTotal; }

private:
    std::vector<RemapInstruction> _runs;   // sorted by _prePos; gaps = tombstones
    uint64_t _preTotal {0};
    uint64_t _postTotal {0};
};
```

`_preTotal` is this merge's pre-merge allocated counter; `_postTotal = sum of _length`.

### Worked example

Suppose a merge runs on a graph with 12 allocated nodes (pre-IDs `[0, 12)`) and
tombstones `{2, 5, 6, 7, 10}`. Walking pre-IDs in order and skipping tombstoned entries:

```
pre-ID:   0  1  2  3  4  5  6  7  8  9  10 11
status:   ✓  ✓  ✗  ✓  ✓  ✗  ✗  ✗  ✓  ✓  ✗  ✓
post-ID:  0  1  -  2  3  -  -  -  4  5  -  6
          └──┘     └──┘           └──┘     └┘
          run 1    run 2          run 3    run 4
```

The resulting `IDRemap` has `_preTotal = 12`, `_postTotal = 7`, and four
`RemapInstruction`s:

| `_prePos` | `_postPos` | `_length` | covers pre-IDs | covers post-IDs |
|---:|---:|---:|---|---|
| 0  | 0 | 2 | `[0, 2)`   | `[0, 2)` |
| 3  | 2 | 2 | `[3, 5)`   | `[2, 4)` |
| 8  | 4 | 2 | `[8, 10)`  | `[4, 6)` |
| 11 | 6 | 1 | `[11, 12)` | `[6, 7)` |

Lookups:

- `apply(4)` — binary search finds the second instruction (`_prePos = 3`); offset is
  `4 - 3 = 1`, so the result is `2 + 1 = 3`.
- `apply(6)` — binary search lands on the second instruction, but offset `6 - 3 = 3`
  exceeds `_length = 2`, so `6` falls in the gap `[5, 8)`. Returns `nullopt`
  → conflict if the change referenced node `6`.
- `apply(11)` — binary search finds the fourth instruction; offset `0`, result `6`.

### Composition example

Now suppose after the merge above (call it `M1`), main accrues a few append-only commits
that bring the allocated counter to `9`, then a second merge `M2` runs with tombstones
`{1, 7}` in that post-`M1` space. `M2`'s instructions are:

| `_prePos` | `_postPos` | `_length` |
|---:|---:|---:|
| 0 | 0 | 1 |
| 2 | 1 | 5 |
| 8 | 6 | 1 |

Composition `M1 ∘ M2` (built via merge-join on `M1`'s post-IDs against `M2`'s pre-IDs)
yields an `IDRemap` over the original pre-`M1` space `[0, 12)` with `_postTotal = 6`:

| `_prePos` | `_postPos` | `_length` | rationale |
|---:|---:|---:|---|
| 0  | 0 | 1 | pre-ID `0` → post-`M1` `0` → post-`M2` `0`; pre-ID `1` dropped by `M2` |
| 3  | 1 | 2 | pre-IDs `3, 4` → post-`M1` `2, 3` → post-`M2` `1, 2` |
| 8  | 3 | 2 | pre-IDs `8, 9` → post-`M1` `4, 5` → post-`M2` `3, 4` |
| 11 | 5 | 1 | pre-ID `11` → post-`M1` `6` → post-`M2` `5`                            |

A change that branched before `M1` and referenced pre-ID `4` resolves to post-`M2` ID
`2`; a reference to pre-ID `7` hits the gap in `M1` and the remapper short-circuits to
`nullopt` without consulting `M2`.

### Why sparse runs, not a dense table

The obvious alternative is a dense `std::vector<NodeID>` of size `_preTotal`, indexed by
pre-ID. The sparse run encoding wins on every axis that matters here:

- **Memory**: one 24-byte instruction per **surviving run**, not per ID. Clustered deletes
  (a single `DELETE` query over a label) yield one gap and thus one instruction,
  regardless of how many entities were removed. A graph with tens of millions of entities
  and a handful of bulk deletes pays kilobytes. The dense alternative pays 8 bytes × total
  allocated, always.
- **Lookup**: `O(log R)` binary search by `_prePos`, where `R` is the run count. In a rebase
  pass we walk the change's dataparts in entity order, so lookups are largely monotonic
  — a cached "current run" pointer gives amortised `O(1)` with the binary search as a
  fallback only when crossing datapart boundaries or following an edge endpoint.
- **Explicit "deleted" signal**: a gap lookup returns `nullopt`, consumed directly by the
  conflict checker. A dense table would need a sentinel value and an extra comparison.

The pathological case — every other pre-ID tombstoned — still costs only `~log(N/2)`
comparisons (≈24 for a billion-node graph) and has a smaller cache footprint than the
dense version.

### Population

The `DataPartMerger::merge` loop at `storage/mergers/DataPartMerger.cpp:113-125` already
walks every pre-ID and checks `tombstones.containsNode(nodeIDVal)`. Emit one
`RemapInstruction` at each run boundary:

```cpp
// Pseudocode interleaved into the existing walk.
if (!tombstones.containsNode(nodeIDVal)) {
    if (!inRun) { runStart = nodeIDVal; postStart = postIDVal; inRun = true; }
    ++postIDVal;
} else if (inRun) {
    runs.push_back({runStart, postStart, nodeIDVal - runStart});
    inRun = false;
}
++nodeIDVal;

// After the walk completes, close a run that ran to the end of the allocated space:
if (inRun) {
    runs.push_back({runStart, postStart, nodeIDVal - runStart});
}
```

For edges, the merger already builds a run encoding via `TombstoneRanges::populateRanges`
on each datapart's `_outEdges` (`storage/mergers/DataPartMerger.cpp:129-134`). Because
`_outEdges[i]._edgeID == firstEdgeID + i` (see `EdgeContainer::get`,
`storage/EdgeContainer.h:54-56`), those `NonDeletedRange{_start, _size}` entries already
denote surviving runs by edge-ID position — convert each into a `RemapInstruction` by
offsetting `_start` with the datapart's `firstEdgeID` and stamping `_postPos` from a
running counter. No second walk required.

Cost for nodes is a handful of additional instructions per iteration in a loop that is
already memory-bound.

### Composition across multiple merges

Two `IDRemap`s `M1` (A→B) and `M2` (B→C) compose in `O(R1 + R2)` via a merge-join on the
`B` axis: walk both run lists, intersect each pair of overlapping runs, emit the resulting
C-space runs. Two strategies for the rebaser:

- **Chained lookup** (default): pass `std::span<const IDRemap*>` to the remapper and apply
  sequentially. One `O(log R)` step per merge per lookup.
- **Eager compose**: fold the full span into a single `IDRemap` at the start of rebase.
  Add only if profiling shows it matters. Most changes will span zero or one merge.

### Storage location and lifetime

Add two members to `CommitData` (`storage/versioning/CommitData.h`):

```cpp
std::unique_ptr<IDRemap> _nodeRemap;   // populated only for merge commits
std::unique_ptr<IDRemap> _edgeRemap;   // populated only for merge commits
```

`std::unique_ptr` gives **pointer stability** (the spec threads raw `const IDRemap*`
through spans, so the address cannot move once attached) and costs nothing for non-merge
commits (two null pointers). Do **not** store the remaps by value — even a single
by-value member would force every `CommitData` to carry the full `std::vector<RemapInstruction>`
machinery, and a future refactor holding them in a `std::vector<IDRemap>` would
invalidate pointers on growth, breaking the span model.

`DataPartMerger::merge` fills the two `IDRemap`s via the fill/output pattern
(`buildNodeRemap(IDRemap& out, ...)`, `buildEdgeRemap(IDRemap& out, ...)`) into freshly
`std::make_unique`'d members before the commit is published via
`VersionController::addCommit`. Once published, the pointers are stable for the commit's
lifetime.

CommitData lifetime is governed by `ArcManager<CommitData>`, and commits are retained
by `VersionController::_commits` for the graph's lifetime (no eviction today). At ~24
bytes per surviving run, and merges being rare, the cumulative memory is negligible for
typical graphs — revisit only if profiling shows the tables dominate some workload. The
earlier-proposed hooks in `submitChange`/`deleteChange` to reference-count open changes
against merge ancestry were not worth the coordination cost for kilobytes per merge.

---

## `EntityIDRemapper`

Replace `EntityIDRebaser` (`storage/versioning/EntityIDRebaser.{h,cpp}`) with a type that
composes per-merge tables with the affine shift for change-local IDs:

```cpp
class EntityIDRemapper {
public:
    std::optional<NodeID> tryRebaseNodeID(NodeID old) const;
    NodeID rebaseNodeID(NodeID old) const;  // asserts survives; used on known-valid paths

    std::optional<EdgeID> tryRebaseEdgeID(EdgeID old) const;
    EdgeID rebaseEdgeID(EdgeID old) const;

private:
    std::span<const IDRemap*> _nodeMerges;   // in commit order, oldest → newest
    std::span<const IDRemap*> _edgeMerges;
    NodeID _branchTimeNextNodeID, _newNextNodeID;
    EdgeID _branchTimeNextEdgeID, _newNextEdgeID;
};
```

### Two independent cutoffs

The remapper uses two distinct cutoffs, one per code path:

- `_branchTimeNextNodeID` — the **change's** branch point. IDs `>= _branchTimeNextNodeID`
  are local to the change and take the affine-shift path, bypassing the merge walk
  entirely. Per-merge `_preTotal` values are `>= _branchTimeNextNodeID` (intervening
  commits can only grow the allocated counter before a merge runs), so a local ID never
  collides with any merge's input range.
- `IDRemap::_preTotal` — **that merge's** pre-merge allocated counter. Only used to bound
  the input range of a single `IDRemap::apply` call. Different merges have different
  `_preTotal`s.

These two quantities are independent.

### Lookup logic

```cpp
std::optional<NodeID> tryRebaseNodeID(NodeID old) const {
    if (old >= _branchTimeNextNodeID) {
        // Local-to-change: unchanged affine shift.
        return old + _newNextNodeID - _branchTimeNextNodeID;
    }
    uint64_t current = old.getValue();
    for (const IDRemap* m : _nodeMerges) {
        auto next = m->apply(current);
        if (!next) {
            return std::nullopt;
        }
        current = *next;
    }
    return NodeID {current};
}
```

With an empty `_nodeMerges` span, the branch-time half of this function returns the same
value as the old `EntityIDRebaser::rebaseNodeID`. **Not** byte-for-byte, though: the old
class bioasserts `_branchTimeNextNodeID <= _newNextNodeID` (`EntityIDRebaser.cpp:37`),
and the new one drops it because merges invalidate it. Preserve the safety net by
re-asserting it inside `tryRebaseNodeID`/`rebaseNodeID` on the code path that takes the
empty-merge-span branch — i.e. gate the assertion on `_nodeMerges.empty()`. The post-
merge path has no such invariant.

### Invariants

- `_branchTimeNextNodeID` is measured against the branch-time view (pre-any-merges).
- `_newNextNodeID` is measured against the new head (post-all-merges plus any subsequent
  append-only commits).
- The old assertion `_branchTimeNextNodeID <= _newNextNodeID` no longer holds and is
  removed.

### `nullopt` semantics by call site

`tryRebaseNodeID(old)` returns `nullopt` when `old` is a pre-branch ID whose target was
deleted on main and then absorbed by a merge. The correct response depends on the
caller's intent:

| Call site | Intent | On `nullopt` |
|---|---|---|
| `ChangeConflictChecker` pending edge endpoints | Change wants entity valid | Throw — new conflict class |
| `ChangeConflictChecker` updated nodes/edges | Change wants entity valid | Throw — new conflict class |
| `ChangeConflictChecker::checkDeletedNodeConflicts/EdgeConflicts` | Change also deletes the entity | `continue` — both sides agreed |
| `ChangeRebaser::rebaseTombstones` | Change's own tombstone | Drop silently |
| `CommitHistoryRebaser` journal write sets | Past change-local write (often a deletion) | Drop entry |
| `CommitWriteBufferRebaser` deletion sets | Same as `rebaseTombstones` | Drop entry |
| `CommitWriteBufferRebaser` updates / pending edge endpoints | Write intent | Unreachable — conflict check already threw |
| `DataPartRebaser` edge endpoints, property IDs, patch node offsets, labelset ranges | Committed reference | Unreachable — conflict check already threw |

Rows tagged "unreachable" call the asserting `rebaseNodeID`; the assertion is a
belt-and-braces check on the conflict-check coverage. All other rows call `tryRebaseNodeID`
and act on the returned `optional`.

---

## Downstream Component Changes

### `DataPartRebaser` — correctness note first, no structural change

`DataPartRebaser::rebase` at `storage/versioning/DataPartRebaser.cpp:34` computes
`newFirstNodeID = prevPart._firstNodeID + prevPart.getNodeContainerSize()` to slot each
change-local datapart behind its predecessor on main. This formula is load-bearing and —
crucially — **still correct across a merge**: for the first change-local datapart,
`prevPart` is the merged datapart, which has `firstNodeID = 0` and
`containerSize = survivingCount`, so the change's first datapart lands at `survivingCount`
in the new head's space. Each subsequent change-local datapart chains off the preceding
one, same as before.

The `_idRebaser->rebaseNodeID(...)` / `rebaseEdgeID(...)` calls — for edge
`_nodeID`/`_otherID`/`_edgeID`, patch node offsets, property container IDs, and labelset
range `_first` — keep calling the **asserting** `rebaseNodeID`/`rebaseEdgeID`. These paths
operate on IDs the conflict checker has already proved survive (see the `nullopt`
semantics table); the assertion catches a gap in conflict-check coverage rather than
swallowing it. Also remove the dead `_nodeOffset` and `_edgeOffset` fields and their
computation at `DataPartRebaser.cpp:36-37` as part of this step.

### `CommitHistoryRebaser` (`storage/versioning/CommitHistoryRebaser.cpp`)

`rebase()` still splices change-local dataparts after the prev history's dataparts and
still routes the per-datapart rebase through `DataPartRebaser` unchanged. After a merge,
`prevHistory._allDataparts` contains a single (merged) datapart — the loop at lines 46-50
works because it walks whatever dataparts the prev history exposes.

The journal write-set rebase changes: the write set can contain IDs the change deleted
whose targets main also deleted and a merge then absorbed. Switch the loops at lines
55-61 from the asserting `rebaseNodeID`/`rebaseEdgeID` to `tryRebase*`, and drop any
entry that returns `nullopt`. Rationale: a gap in the journal write set is always from
the deletion path (non-delete writes would have thrown in conflict checking), and after
the merge there is no valid post-merge ID to record — the entry is moot.

Implementation-wise, iterate into a temporary and swap back, mirroring the pattern in
`ChangeRebaser::rebaseTombstones`.

### `CommitWriteBufferRebaser` (`storage/versioning/CommitWriteBuffer.{h,cpp}`)

The deletion-set loops (`CommitWriteBuffer.cpp:408-424`) switch to `tryRebase*` and drop
`nullopt` entries, mirroring `rebaseTombstones`. The pending-edge endpoint and update
loops keep the asserting `rebase*` — the conflict checker has already rejected any change
that would land a gap here.

### `ChangeRebaser::rebaseTombstones`

The change's tombstones are keyed in branch-time space. Route each tombstone through
`tryRebaseNodeID`:

- If `has_value()`, insert the post-merge ID into the new tombstone set.
- If `nullopt`, the entity was already removed by the merge — drop the tombstone silently.
  Not a conflict: the change wanted it deleted and so did main.

Then union with main's post-merge tombstones as today.

---

## Conflict Detection

Extend `ChangeConflictChecker` (`storage/versioning/ChangeConflictChecker.{h,cpp}`) in
three places.

### New conflict class: merge-deleted reference

For every pre-branch entity ID the change references in a **non-delete** context —
pending edge endpoints (`checkPendingEdgeConflicts`) and node/edge updates
(`checkUpdatedNodeConflicts` / `checkUpdatedEdgeConflicts`) — route through
`tryRebase*`. On `nullopt`, the change references a merge-absorbed entity. **Before
throwing**, consult the change's own intent: if the entity is also in
`writeBuffer.deletedNodes` (for pending-edge src/tgt, or for node updates) or
`deletedEdges` (for edge updates), `continue` — the change would drop the edge / skip
the update anyway at flush time (`buildPendingEdge` elides when
`deletedNodes.contains(src|tgt)` at `CommitWriteBuffer.cpp:165-174`; applyUpdates on a
deleted entity is a no-op). Only throw when the change still "wants" the entity.

Concretely for pending edges:

```cpp
if (const NodeID* oldSrcID = std::get_if<NodeID>(&edge.src)) {
    auto newSrc = _entityIDRemapper.tryRebaseNodeID(*oldSrcID);
    if (!newSrc) {
        if (writeBuffer.deletedNodes().contains(*oldSrcID)) continue;
        throw ...;
    }
    if (writes.writtenNodes.contains(*newSrc)) { throw ... }
}
// same for tgt
```

The symmetric treatment for `checkUpdatedNodeConflicts`: check
`writeBuffer.deletedNodes().contains(updatedNode)` before throwing. For
`checkUpdatedEdgeConflicts`: check `writeBuffer.deletedEdges().contains(updatedEdge)`.

No separate walk over the change's committed dataparts is needed.
`CommitBuilder::flushWriteBuffer` (`CommitBuilder.cpp:146`) retains `_pendingEdges`,
`_deletedNodes`, `_updatedNodes` in the writeBuffer after flushing (only a `_flushed`
flag flips). The existing loop in `ChangeConflictChecker::checkConflicts` iterates all
`CommitBuilder`s in `_change._commits` and inspects each one's writeBuffer — so every
pre-branch ID that would land in a flushed datapart (edge endpoints, patch offsets,
property-container IDs, labelset ranges) is already reachable via `pendingEdges` /
`updatedNodes` / `updatedEdges` / `deletedNodes` / `deletedEdges`. Routing those five
checks through `tryRebase*` covers the committed dataparts transitively.

For delete contexts (`checkDeletedNodeConflicts` / `checkDeletedEdgeConflicts`),
`nullopt` is *not* a conflict — both sides agreed to delete; skip the entry. See the
`nullopt` semantics table. While touching these two functions, also replace the
`deletedNode == newID` shortcut (`ChangeConflictChecker.cpp:175`) — it was a proxy for
"this is a pre-branch ID", but now an ID can move across a merge. Use
`old < _branchTimeNextNodeID` instead for populating `_deletedExistingNodes`. This
requires exposing `_branchTimeNextNodeID` from `EntityIDRemapper` via a getter
(`branchTimeNextNodeID()`).

### Rebase main's write sets before unioning

`getWritesSinceCommit` (`ChangeConflictChecker.cpp:29-36`) unions each main commit's
`journal.nodeWriteSet()` / `edgeWriteSet()` into `writes`. Each journal records IDs in
the ID space of its own commit, so a write set from commit C_i that precedes merge M_j
is in pre-M_j space. A later `writes.writtenNodes.contains(newSrc)` compares a
post-merge `newSrc` against a mix of eras — **false negatives** on any entity that main
touched and a merge then renumbered.

Concrete repro: branch-time counter is 200; main commit C_1 updates node `X` (pre-merge
ID 100), journal records 100. Main merge M_1 renumbers `X` to post-merge ID 50. Change
has also updated `X` (branch-time ID 100). Rebase: `tryRebaseNodeID(100) = 50`. Check:
`writes.writtenNodes.contains(50)` — returns false, because `writes` still has 100.
Conflict missed.

Fix: walk `_commitsSinceBranch` once, maintaining a "merges-remaining" sub-span that
starts as the full merge span and shrinks each time the walk crosses a merge commit.
For each non-merge commit `C_i`, project its journal IDs through `tryRebase*` against
the current merges-remaining; drop `nullopt` entries (entity was main-deleted and
merge-absorbed — no post-merge ID exists, so no rebased reference can hit it). For each
merge commit, drop the first entry from merges-remaining; merge journals are empty so
nothing to union.

Expose overloads on `EntityIDRemapper` that take the subspan explicitly:

```cpp
std::optional<NodeID> tryRebaseNodeID(NodeID old, std::span<const IDRemap*> subspan) const;
std::optional<EdgeID> tryRebaseEdgeID(EdgeID old, std::span<const IDRemap*> subspan) const;
```

These skip the `_branchTimeNext*` cutoff check (main's IDs aren't in change-local space)
and just chain the provided subspan's `IDRemap`s. Overloading the same name (rather than
introducing `*ThroughSubspan`) keeps the API surface small and matches project naming
conventions — the two base methods and two subspan overloads cover every call site.

**Ownership of the projection: `ChangeRebaser`.** Build `ConflictCheckSets writes` in
`ChangeRebaser` using these overloads, *before* constructing `ChangeConflictChecker`.
Pass the pre-built writes into the checker's new constructor (see the next section).
`ChangeConflictChecker` then loses its `getWritesSinceCommit` method, loses its
`_commitsSinceBranch` member (it no longer walks the span itself), and its `writes`
parameter is an already-projected set in current-head space — `contains()` works as
today.

### `checkNewEdgesIncidentToDeleted` — redo the discriminator

The current implementation (`ChangeConflictChecker.cpp:68-87`) computes

```
startingIndex = totalDPsOnMain - sum(commitDataparts over commits since branch)
```

to skip dataparts that existed at branch time. A merge collapses the pre-merge DPs into
one, so the sum can exceed `totalDPsOnMain`, the `bioassert` fires, and even if you
removed the assertion the DP-index would be meaningless — the merged DP contains
pre-branch edges alongside post-branch ones.

Replace the DP-index discriminator with an edge-ID one.

**Where it lives.** `branchCutoffEdgeID` is a scalar `EdgeID` member on `ChangeRebaser`,
computed once in `ChangeRebaser::init` from `_branchTimeNextEdgeID` and the edge-merge
span, and passed to `ChangeConflictChecker`'s constructor. It is **not** on
`EntityIDRemapper` — the remapper is per-ID, stateless about branch position. Expose it
via `ChangeRebaser::branchCutoffEdgeID()` so tests and the checker can read it.

**Algorithm.** Push `B_e = _branchTimeNextEdgeID` through each `IDRemap` in the edge
merge span via a ceiling-style lookup:

- If a run `R` contains `B_e` (`R._prePos <= B_e < R._prePos + R._length`):
  next value = `R._postPos + (B_e - R._prePos)`.
- Else if a run `R` is the first with `R._prePos >= B_e`: next value = `R._postPos`.
- Else (`B_e` exceeds every run): next value = `_postTotal`.

After the last merge, add the allocated growth from any post-merge append-only commits
(those IDs are trivially `>= branchCutoffEdgeID` by construction).

Worked example: preTotal = 10, tombstones {2, 7}, so runs are
`(0→0, len 2)`, `(3→2, len 4)`, `(8→6, len 2)`; postTotal = 8. `B_e = 5` falls inside
the second run (`3 <= 5 < 7`), so next value = `2 + (5-3) = 4` — the four surviving
pre-branch IDs (`{0, 1, 3, 4}`) occupy post-merge positions `[0, 4)`, and everything
from post-merge ID 4 onward is "new since branch."

**Use in the checker.** In `checkNewEdgesIncidentToDeleted`, drop the `startingIndex`
machinery. Iterate `GetOut/InEdges` over `_deletedExistingNodes` on the post-merge view
and filter `record._edgeID >= _branchCutoffEdgeID` (stored member). An edge meeting
that predicate was created since branch time; if it isn't tombstoned on main, throw as
today.

---

## `ChangeRebaser::init` Signature

`init` currently takes `branchTimeReader` and `mainReader`. Expand it:

```cpp
void init(const GraphReader& mainReader,
          const GraphReader& branchTimeReader,
          Commit::CommitSpan commitsSinceBranch);
```

The span is already fetched in `Change::rebase` via
`_versionController->getCommitsSinceCommitHash(baseHash())` (`Change.cpp:111-112`) but
**after** calling `init`. Reorder: fetch the span first and pass it into `init`.

Inside `init`, `ChangeRebaser`:
1. Filters `commitsSinceBranch` through `Commit::isMergeCommit()` to build two
   `std::vector<const IDRemap*>` (node and edge); constructs the `EntityIDRemapper` with
   them.
2. Computes `_branchCutoffEdgeID` from `_branchTimeNextEdgeID` and the edge merge span
   via the ceiling walk (see the previous section).
3. Walks `commitsSinceBranch` once to build `ConflictCheckSets _writes`, projecting each
   non-merge commit's journal through `tryRebase*(id, merges-remaining-subspan)`,
   dropping `nullopt`, shrinking the subspan at each merge commit.
4. Stores the readers + remapper + cutoff + writes as members.

`checkConflicts` no longer needs the commit span as an argument — the writes it needs
are already pre-projected on the rebaser. Its new signature is:

```cpp
void checkConflicts();
```

and internally it constructs the checker as:

```cpp
ChangeConflictChecker checker(*_change,
                              _entityIDRemapper,
                              _writes,
                              _branchCutoffEdgeID,
                              _newMainReader);
checker.checkConflicts();
```

`ChangeConflictChecker`'s old members `_entityIDRebaser`, `_commitsSinceBranch` are
replaced by `_entityIDRemapper`, `_writes`, `_branchCutoffEdgeID`; the
`getWritesSinceCommit` method is removed.

---

## Persistence

`CommitMetaDataDumper::dump` (`storage/dump/CommitMetaDataDumper.cpp:13-34`) today
writes `numNodes`, `numEdges`, `numCommitDataParts`, `numAllDataParts`, and the
datapart-ID list. It does **not** persist `_mergeCommit` or any `IDRemap` state.
Without fixing this, a dump + load of a graph containing a merge commit would produce
silent corruption: on re-load, the reconstructed `Commit` has `_mergeCommit == false`
and null `IDRemap` members, so any `Change` whose `baseHash` predates the merge would
rebase through an empty merge span, take the identity path, and hand pre-merge IDs to
`DataPartRebaser`'s asserting `rebaseNodeID` — which would happily accept wrong
numbers and return a corrupted post-merge graph.

### What to persist

Extend `CommitMetaDataDumper::dump` (and the matching `CommitLoader::loadData`) with:

1. **`_mergeCommit` flag.** One byte (or a header bit) per commit. Load must set
   `commit->_mergeCommit` after reading.

2. **Two `IDRemap`s**, each as `(preTotal : u64, postTotal : u64, runCount : u64,
   runs : array<{prePos, postPos, length}>)`. Present only when `_mergeCommit == true`.
   On load, materialize them into `commitData._nodeRemap` /
   `commitData._edgeRemap` via `std::make_unique`.

Put the `IDRemap` payloads in separate files next to `journal` and `tombstones`
(e.g. `commitDir/idremap_nodes`, `commitDir/idremap_edges`) so non-merge commits pay
nothing — the dumper simply omits them. The metadata file's header grows by one byte
for the flag.

### Lazy commit load path

`VersionController::loadCommit` (`storage/versioning/VersionController.cpp:211-232`)
invokes `CommitLoader::loadData` on-demand when a not-yet-materialized commit is
referenced — so once `loadData` is updated as above, the lazy path is covered
automatically. Also update `GraphLoader::load`: today it calls `loadData` only for
the head commit (`GraphLoader.cpp:95`), but a rebase spanning merge commits older
than head must trigger lazy load via `VersionController::loadCommit` before reading
`IDRemap`s. Concretely: `ChangeRebaser::init`, when filtering merge commits out of
the span, calls `_versionController->loadCommit(commit->hash(), …)` on any merge
commit whose `CommitData` reports missing `IDRemap`s — this is a one-time cost
proportional to the number of merges in the span.

### Versioning

Bump the graph dump format version (see `GraphDumpHelper`) so old dumps fail fast with
a clear error rather than loading missing data. Add a one-release window where the
loader accepts both the old format (no `IDRemap`, asserts the graph has no merge
commits) and the new one, then drop the old path.

### Regression tests

- **Dump-load roundtrip across a merge.** Create graph → merge → dump → re-open a
  fresh graph from the dump → verify `isMergeCommit()` is `true` on the merged commit
  and `IDRemap::apply(X)` matches pre-dump values for every surviving `X`.
- **Change submit after dump-load.** Branch → internal writes → dump the graph →
  on another process, load → submit the change → verify post-rebase IDs match those
  of an in-memory-only run of the same scenario.

---

## Ordering with the Accessor Refactor

This spec and `docs/accessor-refactoring-plan.md` touch the same lock domain. Land this
spec's work **first**, then the accessor refactor. Rationale:

- This spec's changes are all within `storage/versioning/` and
  `storage/mergers/DataPartMerger.cpp`; the guard's location (`ChangeManager` vs
  `VersionController`) is orthogonal to every other change here.
- Step 1 gates the guard on `_allowMergeWithOpenChanges` — a flag on
  `VersionController` — which survives the accessor refactor unchanged (the flag is a
  property of the graph, not of `ChangeManager`).
- Doing this work first means the accessor refactor's Step 1 (move change storage,
  move `hasOpenChanges()` into `VersionController`, move the guard with it) is a
  pure code move that preserves behaviour: the flag is already on `VersionController`,
  the guard already reads it, the lock direction already works.

If someone lands the accessor refactor first, the only change needed here is the
one-line move of the gate from `ChangeManager::mergeDataParts` to
`VersionController::mergeDataParts`.

**Lock order invariant.** Any code path that acquires both `_changesLock` (accessor
refactor's RWSpinLock on pending changes) and `_mutex` (VersionController's
`shared_mutex` on commit history) must acquire them in the order **`_changesLock`
first, then `_mutex`**. The merge path already does this implicitly today
(`ChangeManager::mergeDataParts` takes `_changesLock` and then calls into
`Graph::mergeDataParts` → `VersionController::mergeDataParts` which takes `_mutex`).
Keep this order after the accessor refactor. Reversing it can deadlock against
`submitChange` / `createChange`.

---

## Guard Removal

The `CHANGES_ON_MAIN` check in `ChangeManager::mergeDataParts`
(`system/ChangeManager.cpp:29-31`) — or its post-accessor-refactor home in
`VersionController::mergeDataParts` — is wrapped in a runtime check
`!_allowMergeWithOpenChanges` from step 1 onward. Deployment:

1. Default `_allowMergeWithOpenChanges = false`. Existing behaviour preserved.
2. After all steps 1-6 ship and soak in staging/tests, flip the default to `true` in
   one commit. This is the trivial revert point: if a latent bug surfaces in
   production, revert the flip commit alone — the machinery stays in place but the
   guard re-engages, and the system falls back to today's behaviour.
3. Once the default has soaked in production for one release with no incidents,
   remove the flag and the guard entirely. Keep the enum value
   `DataPartMergeErrorType::CHANGES_ON_MAIN` for one additional release, then delete.

The flag is read under the same `std::unique_lock<std::shared_mutex>` as
`mergeDataParts` (no separate synchronization needed), and can be flipped via a test
fixture, a `--dev` CLI option, or an admin endpoint — implementation is the caller's
choice; the spec only requires that the flag exists and is wired through step 1.

---

## Rollout

Ordered so that each step is independently shippable and leaves existing behaviour
correct.

1. **Emit `IDRemap` during merge; add the runtime feature flag.** Populate node and edge
   `IDRemap`s in `DataPartMerger::merge`, attach them to the merge commit's `CommitData`
   (two `std::unique_ptr<IDRemap>` members), set `Commit::_mergeCommit = true` in
   `createMergeCommit`, and persist both in `CommitMetaDataDumper`/`CommitLoader` and the
   lazy `VersionController::loadCommit` path (see the Persistence section). Also add the
   feature flag `VersionController::_allowMergeWithOpenChanges` (default `false`), and
   gate the `CHANGES_ON_MAIN` guard on its negation. Delete the dead
   `DataPartRebaser::_nodeOffset`/`_edgeOffset` fields. No readers consume the remaps
   yet. Unit-test directly: identity (no tombstones), single-gap, clustered deletes,
   fully-sparse deletes. Tests can flip the flag to exercise end-to-end paths from
   step 3 onward.

2. **`EntityIDRebaser` → `EntityIDRemapper`: rename + add `tryRebase*`.** This is a
   single atomic patch: rename the class, update all four downstream references in one
   go (`ChangeConflictChecker::_entityIDRebaser`, `DataPartRebaser::_idRebaser`,
   `CommitHistoryRebaser::rebase` parameter, `CommitWriteBufferRebaser::_idRebaser`),
   and add `tryRebaseNodeID` / `tryRebaseEdgeID` alongside the existing asserting
   `rebase*`. Also add the subspan overloads (see "Rebase main's write sets"). With an
   empty merge span, the asserting variant returns the same values as the old rebaser;
   the invariant bioassert is re-added inside the empty-span branch. All existing
   rebase tests must pass. The rename is mechanical, not "purely additive" — the 4
   signature updates cannot ship separately.

3. **Thread merge spans through `ChangeRebaser::init`.** Reorder `Change::rebase` to
   fetch the commit span before calling `init`; `init` filters `isMergeCommit()` to
   build the two `IDRemap*` vectors, hands them to `EntityIDRemapper`, computes
   `_branchCutoffEdgeID`, and builds the projected `_writes`. Until this step lands,
   the merge-walk branch of `tryRebase*` is unreachable from integration tests (unit
   tests still exercise it directly).

4. **Route deletion paths through `tryRebase*`.** Switch `ChangeRebaser::rebaseTombstones`,
   `CommitHistoryRebaser`'s journal write-set loops, and `CommitWriteBufferRebaser`'s
   deletion-set loops to the optional-returning variant, dropping `nullopt` entries.
   Append-only rebase behaviour is unchanged (no gaps in that case).

5. **Route conflict checks through `tryRebase*`; finalise the checker API.** Switch
   `checkPendingEdgeConflicts`, `checkUpdatedNodeConflicts`, `checkUpdatedEdgeConflicts`,
   `checkDeletedNodeConflicts`, `checkDeletedEdgeConflicts` to `tryRebase*`. Non-delete
   paths consult `writeBuffer.deletedNodes/Edges` before throwing on `nullopt` (see
   "New conflict class"). Delete paths `continue` on `nullopt`. Fix the
   `deletedNode == newID` shortcut to `old < _entityIDRemapper.branchTimeNextNodeID()`.
   Update `ChangeConflictChecker`'s ctor to accept pre-projected `ConflictCheckSets`
   and `_branchCutoffEdgeID`; remove `getWritesSinceCommit`. Update the single call
   site in `ChangeRebaser::checkConflicts`.

6. **Conflict checker — `checkNewEdgesIncidentToDeleted` redo.** Replace the DP-index
   discriminator with `_branchCutoffEdgeID` (computed in step 3, already passed in).
7. **Flip `_allowMergeWithOpenChanges` default to `true`.** A trivially revertible
   commit. After it soaks in production for one release, a follow-up removes the flag
   and the guard entirely (see "Guard Removal").

Unit tests exercise each step in isolation (the merge-span code paths are reachable via
direct tests of `IDRemap`, `EntityIDRemapper`, `ChangeRebaser`,
`ChangeConflictChecker`). End-to-end regression tests that actually merge with an open
change run from step 3 onward with the flag flipped on — no separate "test knob" is
needed, because the flag is the knob.

---

## Regression Tests

Each test should assert both that the operation succeeds/fails as expected **and** the
resulting ID values.

- **Survivor-only writes.** Change writes to entities that all survive a subsequent merge.
  Assert: submit succeeds; written IDs post-rebase equal the `IDRemap`-mapped post-merge
  IDs.
- **Write-against-merge-deleted conflict.** Main deletes entity `X`, merge absorbs the
  tombstone, then the change (which touched `X`) tries to submit. Assert: submit fails
  with the new conflict type; no state change on main.
- **Shrunk counters — local IDs land correctly.** Change creates `K` new nodes at
  branch-time IDs `[B, B+K)`. Main then merges, compacting allocated counter to
  `B_post < B`. Assert: after submit, the new nodes have IDs `[B_post, B_post + K)` and
  `getTotalNodesAllocated() == B_post + K`.
- **Multi-merge composition.** Change spans two merges M1 and M2. Assert: pre-branch ID
  `X` in the change ends up at `M2.apply(M1.apply(X))`. Cross-check against an eagerly
  composed `IDRemap`.
- **Tombstone-both-sides drop.** Change marks entity `Y` deleted; main also deletes `Y`
  and merges. Assert: submit succeeds (no conflict), final tombstone set does not contain
  the pre-merge `Y` ID, and no stale reference remains in the change's datapart.
- **Identity merge.** Merge with zero tombstones. Assert: each `IDRemap` contains exactly
  one `RemapInstruction` with `_prePos == _postPos == 0` and `_length == _preTotal`;
  `tryRebaseNodeID(X) == X` for all pre-branch `X`.
- **Internal `COMMIT` across a merge.** Change does `CREATE ... COMMIT CREATE ...` so
  the first commit's writes are flushed into a change-local datapart before submit. Main
  then merges. Assert: submit succeeds; every ID in the flushed datapart (edge endpoints,
  patch offsets, property IDs, labelset `_first`) is rebased through the remapper;
  `DataPartRebaser`'s asserting path never hits a gap.
- **Edge endpoint to merge-deleted node (after internal COMMIT).** Change creates edge
  `E` with endpoint the pre-branch node `X`, then internal-`COMMIT`s so `E` lands in a
  flushed datapart. Main deletes `X`, merge absorbs the tombstone. Assert: submit fails
  with the new conflict class, surfaced via `checkPendingEdgeConflicts` (which iterates
  the writeBuffer across *all* `CommitBuilder`s — flushed or not — because `_pendingEdges`
  persists past flush). The asserting `rebaseNodeID` in `DataPartRebaser` is never
  reached.
- **Delete-and-update conflict.** Change updates property on node `X`. Main deletes `X`,
  merge absorbs the tombstone. Assert: submit fails with the new conflict class (update
  intent on a merge-absorbed ID is a conflict even though a pure delete would not be).
- **Metadata-evolving intermediate commit before merge.** Main adds a new label in a
  commit, then merges. Change references the pre-existing labels. Assert: labelset IDs on
  the change's datapart are remapped by `MetadataRebaser`; entity IDs are remapped by the
  remapper; they compose correctly (no double-remap, no skipped remap).
- **Post-merge append-only commits.** Main merges, then accepts `M` more append-only
  commits bringing the counter to `B_post + M` before the change submits. Change creates
  `K` local nodes. Assert: local nodes land at `[B_post + M, B_post + M + K)`; pre-branch
  survivors go through `IDRemap` only.
- **`checkNewEdgesIncidentToDeleted` across a merge.** Change wants to delete pre-branch
  node `X`. Main adds edge `E` incident to `X` (new edge-ID), then merges. Assert: submit
  fails because `E` is not tombstoned on main — with the new `branchCutoffEdgeID`
  discriminator driving the check, not the broken DP-index one.
- **Pending edge to node the change also deletes.** Change creates edge `E` from
  pre-branch node `X` to a new local node, and *in the same change* deletes `X`. Main
  deletes `X` and merges. Assert: submit succeeds (all three parties agreed `X` is
  gone, and the edge is elided at flush); `checkPendingEdgeConflicts` does **not**
  throw on the nullopt lookup of `X` as src.
- **Update on a node the change also deletes.** Change does `SET x.p = v` then
  `DELETE x`. Main deletes `X` and merges. Assert: submit succeeds; the update on the
  merge-absorbed `X` is skipped because `writeBuffer.deletedNodes.contains(X)`.

---

## Tradeoffs

- **Memory**: proportional to surviving-run count, not total allocated. Attached to the
  merge `CommitData`; lives as long as the commit is retained by `VersionController`
  (i.e. the graph's lifetime, since commits are not evicted today). Typical graphs pay
  kilobytes; the dense alternative would pay megabytes. Per-change GC is intentionally
  not implemented — the coordination cost isn't justified by the memory savings at this
  scale.
- **Conflict rate**: changes that touched an entity main deleted and merged will now fail
  at rebase time with a precise error rather than being blocked at merge time with a
  coarse one. This is the correct trade.
- **Conflict-check cost**: adds a walk over each flushed change-local datapart's IDs —
  linear in the change's committed data. For typical changes this is the same order of
  work as `DataPartRebaser`, so no new scaling concern.
- **No move semantics / RVO**: `IDRemap` holds a `std::vector`. Per `CODING_STYLE.md`,
  construct in place via a member function (`buildNodeRemap(IDRemap& out, ...)`) rather
  than returning by value from a factory.
