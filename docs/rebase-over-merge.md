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
- Metadata is copied verbatim (`Commit::createMergeCommit`), so a pure merge does not remap
  label, edge-type, or property-type IDs. `MetadataRebaser` stays identity.

---

## Approach Summary

1. Each merge commit emits a sparse **`IDRemap`** for nodes and for edges, encoding which
   pre-merge IDs survived and where they landed post-merge. Tombstoned IDs are gaps.
2. The rebaser is generalised from `EntityIDRebaser` to `EntityIDRemapper`, which composes
   a sequence of `IDRemap` tables (one per merge commit in the rebase span) with the
   existing affine shift for change-local IDs.
3. `ChangeConflictChecker` gains a new conflict class: pre-branch IDs the change references
   that a merge dropped (because main deleted them first).
4. The `CHANGES_ON_MAIN` guard is removed.

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
```

Same pattern for edges. Cost is a handful of additional instructions per iteration in a
loop that is already memory-bound.

### Composition across multiple merges

Two `IDRemap`s `M1` (A→B) and `M2` (B→C) compose in `O(R1 + R2)` via a merge-join on the
`B` axis: walk both run lists, intersect each pair of overlapping runs, emit the resulting
C-space runs. Two strategies for the rebaser:

- **Chained lookup** (default): pass `std::span<const IDRemap*>` to the remapper and apply
  sequentially. One `O(log R)` step per merge per lookup.
- **Eager compose**: fold the full span into a single `IDRemap` at the start of rebase.
  Add only if profiling shows it matters. Most changes will span zero or one merge.

### Storage location and lifetime

Attach the `IDRemap`s to the merge `Commit` (or to its `CommitData`). Keep them alive
while at least one open `Change` has a `baseHash` older than the merge commit. When the
merge commit is installed, check whether any such change exists; if not, free the tables
immediately. Otherwise free them later via hooks in `VersionController::submitChange` and
`deleteChange` once the last older change drains. For the current operational pattern —
merges run when no changes are open — the tables are attached then freed in the same
critical section.

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

With an empty `_nodeMerges` span, the branch-time half of this function behaves
identically to the old `EntityIDRebaser::rebaseNodeID`. The signature change — `optional`
return — is a real API break; callers update at the step in the rollout that introduces
the new type.

### Invariants

- `_branchTimeNextNodeID` is measured against the branch-time view (pre-any-merges).
- `_newNextNodeID` is measured against the new head (post-all-merges plus any subsequent
  append-only commits).
- The old assertion `_branchTimeNextNodeID <= _newNextNodeID` no longer holds and is
  removed.

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

The remaining work in this file is mechanical: every `_idRebaser->rebaseNodeID(...)` and
`rebaseEdgeID(...)` call — for edge `_nodeID`/`_otherID`/`_edgeID`, patch node offsets,
property container IDs, and labelset range `_first` — routes through the remapper. Call
sites do not change; only the underlying type's lookup logic does.

### `CommitHistoryRebaser` (`storage/versioning/CommitHistoryRebaser.cpp`)

No structural change. `rebase()` already splices change-local dataparts after the prev
history's dataparts and rebases the journal's write sets through the remapper. After a
merge, `prevHistory._allDataparts` contains a single (merged) datapart — the loop at
lines 46-50 still works because it walks whatever dataparts the prev history exposes.

### `CommitWriteBufferRebaser` (`storage/versioning/CommitWriteBuffer.{h,cpp}`)

Routes all `_idRebaser->rebase*` calls through the remapper. No structural change.

### `ChangeRebaser::rebaseTombstones`

The change's tombstones are keyed in branch-time space. Route each tombstone through
`tryRebaseNodeID`:

- If `has_value()`, insert the post-merge ID into the new tombstone set.
- If `nullopt`, the entity was already removed by the merge — drop the tombstone silently.
  Not a conflict: the change wanted it deleted and so did main.

Then union with main's post-merge tombstones as today.

---

## Conflict Detection

Extend `ChangeConflictChecker` (`storage/versioning/ChangeConflictChecker.{h,cpp}`). For
every pre-branch entity ID in the change's write set, deleted set, updated set, or
pending write-buffer reference:

- Call `tryRebaseNodeID` / `tryRebaseEdgeID`.
- If the result is `nullopt`, emit a conflict: *main deleted an entity the change still
  references, and a merge then collected the tombstone*.

The existing checks (`checkNewEdgesIncidentToDeleted`, `checkDeletedNodeConflicts`,
`checkPendingEdgeConflicts`, `checkDeletedEdgeConflicts`, and the node/edge-update
variants) already operate on the post-rebase main reader; once that reader is the
post-merge head, their logic is unchanged. They just receive post-merge IDs from the
remapper.

---

## `ChangeRebaser::init` Signature

`init` currently takes `branchTimeReader` and `mainReader`. Add a third input: the
ordered span of `IDRemap` pointers collected from any merge commits between them:

```cpp
void init(const GraphReader& mainReader,
          const GraphReader& branchTimeReader,
          std::span<const IDRemap*> nodeMerges,
          std::span<const IDRemap*> edgeMerges);
```

`VersionController::submitChange` is the call site that already walks the commit span
from branch base to head; it collects the `IDRemap` pointers from each merge commit in
that span and hands them to `init`.

---

## Guard Removal

Remove the `CHANGES_ON_MAIN` check in `ChangeManager::mergeDataParts`
(`system/ChangeManager.cpp:29-31`) — or in `VersionController::mergeDataParts` post the
accessor refactor described in `docs/accessor-refactoring-plan.md`. Keep the enum value
`DataPartMergeErrorType::CHANGES_ON_MAIN` for one release, then delete.

---

## Rollout

Ordered so that each step is independently shippable and leaves existing behaviour
correct.

1. **Emit `IDRemap` during merge.** Populate node and edge `IDRemap`s in
   `DataPartMerger::merge` and attach them to the resulting merge commit. No callers yet.
   Unit-test the tables directly: identity case (no tombstones), single-gap case,
   clustered deletes, fully-sparse deletes.
2. **`EntityIDRebaser` → `EntityIDRemapper`.** Rename and generalise; add `tryRebase*`
   returning `std::optional`. With an empty merge span, the branch-time half of lookup
   matches the old rebaser byte-for-byte. Call sites update to handle the new return
   type. All existing rebase tests must pass.
3. **Thread merge spans through `ChangeRebaser::init`.** Collect `IDRemap*`s from the
   commit span in `VersionController::submitChange`. Until this step lands, the merge-walk
   branch of `tryRebase*` is unreachable — steps 4 and 5 are prerequisites on this one.
4. **`ChangeRebaser::rebaseTombstones` re-routing.** Switch to `tryRebase*` and drop
   tombstones that hit gaps.
5. **`ChangeConflictChecker` gains merge-deleted conflicts.** Add tests: change with
   writes to a node that main tombstones and then merges away.
6. **Remove the `CHANGES_ON_MAIN` guard.**
7. **Lifetime GC.** Drop `IDRemap`s from old merge commits once no change has a
   `baseHash` predating them. Hook into `submitChange` and `deleteChange`.

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

---

## Tradeoffs

- **Memory**: proportional to surviving-run count, not total allocated. Held only while
  an older change exists. Typical graphs pay kilobytes; the dense alternative would pay
  megabytes.
- **Conflict rate**: changes that touched an entity main deleted and merged will now fail
  at rebase time with a precise error rather than being blocked at merge time with a
  coarse one. This is the correct trade.
- **No move semantics / RVO**: `IDRemap` holds a `std::vector`. Per `CODING_STYLE.md`,
  construct in place via a member function (`buildNodeRemap(IDRemap& out, ...)`) rather
  than returning by value from a factory.
