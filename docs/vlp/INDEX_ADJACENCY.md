# Index adjacency: filter a node's candidates once

## Context

The branch implements PathEnum's pruning rule but not its index. `PathDistanceIndex` and
`PathTargetIndex` give `dist(v)` and the walk rejects a candidate when `dist(v) > remaining`,
which is IDX-DFS. What PathEnum also does, and this does not, is materialise the surviving
adjacency: it keeps each node's valid neighbours sorted by distance to the target, so the
neighbours reachable within a budget are a prefix of a list that was filtered once.

Here the walk filters on every visit. `generateCandidates` reads the node's raw adjacency and
tests each 32-byte `EdgeRecord` for backtracking, edge type, tombstones, trail membership and
the distance bound; `pushFrame` copies the survivors into the walker's stacks and runs the hop
predicate over them; a patched node repeats all of it per patch part. None of that is kept. A
node reached by 40,000 distinct prefixes pays for it 40,000 times, and under trail semantics
that revisit factor is the normal case, not the extreme one: a node is re-expanded once per
prefix that arrives at it.

This change stores the result. Per node: the candidates that survive everything that does not
depend on the path, sorted by distance, with the distance inline. The walk then scans a
prefix and tests the trail.

## What depends on the path

| Test | Depends on |
|---|---|
| edge type | the edge |
| tombstone | the view |
| patch parts | the node |
| hop predicate | source, edge, end |
| `dist(other)` | the end constraint |
| backtrack, trail membership | **the prefix** |

Five of the six are properties of the node being expanded. The hop predicate is the one that
needs checking, and it is safe: `NLHopFilter::filter` fills exactly three columns - source,
edge and end - and `db.explore_paths` says the region "reads nothing from outside but
constants", so its verdict for a node is the same however the walk got there. Only the trail
test is per-visit, and it is already the cheapest: a 64-bit signature test before any scan.

## Design

### 1. `PathCandidateStore` (storage/iterators/PathCandidateStore.{h,cpp})

An open-addressing table keyed by node, the shape of `PathReachTable`, mapping a node to a
range of one shared arena. The arena is three parallel arrays: `EdgeID`, `NodeID`, and one
distance byte. Seventeen bytes per surviving candidate against the 32 of an `EdgeRecord`, and
only for candidates that survived.

```cpp
struct PathCandidateRange {
    size_t _begin {0};
    size_t _end {0};
};
```

The store is capped in bytes. When the arena is full it stops admitting nodes and `find`
misses forever after, so the walk falls back to filtering per visit. Admission stops, entries
are never evicted: a range handed to a live frame must stay valid.

### 2. Filling it

On the first expansion of a node the walk does what it does today - read the owner part's
adjacency, merge the patch parts, drop the wrong type and the tombstoned, run the hop
predicate - and appends the survivors to the arena instead of to the walker's stacks. The
`RangeRequested` / `SpanRequested` staging stays on that path; it is the miss that has a
memory fetch to hide.

A hit skips all of it. There is no adjacency to fetch, so the walker pushes the frame in the
same turn and `prefetchNodeData` is not called.

### 3. The prefix cut

With an end constraint the entries are sorted ascending by the distance byte, so the
candidates that can still reach an end within `r` hops are a prefix and the scan stops at the
first entry that fails:

```cpp
for (size_t entry = range._begin; entry < range._end; entry++) {
    if (distances[entry] > remainingHops) {
        break;
    }

    const EdgeID edge = edges[entry];
    if ((signature & signatureBit(edge)) != 0 && isOnPath(edge)) {
        continue;
    }

    // descend
}
```

That is PathEnum's prefix retrieval without their offset table: they precompute one offset per
budget value, and stopping at the first failure costs nothing and touches only lines the scan
was going to read.

What leaves the inner loop matters as much as what stays. No type compare, no tombstone probe,
no predicate evaluation, and no random read into a distance structure - the distance is inline.
That last one is worth the most under a bound end, where the lookup today is a hash probe into
`PathTargetBatch`, priced at 3.0 candidate checks in `PathTargetIndex.cpp`.

### 4. The two distance sources

The survivors are stable for the whole loop. The distance byte is not.

- `end_labels`: `PathDistanceIndex` is built once per loop, so the byte and the order are
  loop-stable. Fill and sort once.
- `end_column`: each seed prunes against its own target and `PathTargetIndex` is rebuilt per
  chunk. Sort by the **minimum distance over the batch's 64 targets**. It is a lower bound for
  every bit, so the prefix cut stays sound - past the cut no target in the batch is reachable -
  and inside the prefix each row still runs `canReachWithin` for its own bit. The byte array
  and the order are refilled per chunk over the nodes already in the store; the adjacency read
  and the predicate, which are the expensive parts, are not paid again.
- No end constraint: unordered, no early-out, and the store still saves the type, tombstone,
  patch-part and predicate work on every revisit.

### 5. The walk

A `Frame` becomes a range into the arena rather than into the walker's stacks, plus the
cursor it already has. `pushFrame` writes nothing and `popFrame` truncates nothing. The
walker keeps its own stacks only for the fallback, so `Frame` carries the bit that says which
of the two it indexes.

The distinct mode shares the store. `collectReachCandidates` re-reads a node's adjacency once
per batch of 64 seeds, so a node in every ball is expanded as many times as there are batches;
those revisits are the same revisits, and the mode wants the unordered form.

### 6. The gate

The two existing gates ask whether the enumeration is bigger than one BFS. This one asks a
different question: will nodes be revisited often enough to pay for storing their survivors.
Both terms are already estimated. `estimatedEnumerationChecks` gives the candidate checks the
walk expects, and the ball `planBatch` derives from `estimatedSearchChecks` gives the distinct
nodes it will stand on; their ratio is the expected revisits per node. At a ratio near 1 the store is
pure overhead and must not be built. The deep, branchy walks where it runs to thousands are
the ones that pay.

### 7. Memory

The worst case is an untyped `both` walk over reactome: 11.5M edges appear in two adjacency
lists, so 23M entries, about 390 MB. A typed walk is proportional to that type's edge count.
The cap needs the treatment `PathTargetIndex` already gives itself - a byte limit, checked
against the estimate before the first fill, and admission that stops rather than grows.

## What this does not do

It cuts the constant per hop and the work per revisit. It does not cut the number of paths, so
the output-bound shapes do not move: the 125M-path query in `path_bench.md` spends its time
writing rows. The queries that move are the end-constrained, deeply-revisiting ones.

It is also not the join. IDX-JOIN and the hub-suspended search stay deferred for the reasons
in `PLAN.md`, and they need a forward index this change does not build.

## Files

- `storage/iterators/PathCandidateStore.{h,cpp}` - new: the table, the arena, the cap.
- `storage/iterators/PathExplorator.{h,cpp}` - `Frame` over either array, the fill path
  writing into the store, the prefix scan, the distinct mode's collect.
- `storage/iterators/PathDistanceIndex.{h,cpp}` - the revisit estimate the gate reads.
- `query/ir/interpreter/NLProgram.h` - `NLExplorePathsLoopData` owns the store, as it owns the
  distance index.
- `query/ir/interpreter/NLExecutor.cpp` - the gate and the wiring, beside `pruningIndexFor`.
- `test/storage/CMakeLists.txt`, `test/storage/iterators/PathCandidateStoreTest.cpp`.
- `samples/path_bench/main.cpp` - a `-section store` table.

## Implementation order

1. `PathCandidateStore` and its unit test: insert, find, the range, the cap refusing a node
   once the arena is full, clear.
2. The explorator fills and reads it with no ordering and no end constraint, behind a setter
   the tests drive. The cyclic sweep runs with the store forced on and forced off.
3. The distance byte and the prefix cut for `end_labels`.
4. The batch minimum and the per-chunk re-sort for `end_column`.
5. The distinct mode's collect.
6. The gate, calibrated on `path_bench` with a hit/miss counter beside
   `getCandidateCheckCount`.

## Verification

- `PathCandidateStoreTest` for the table itself.
- Every existing path test run with the store forced on: `PathExploratorCyclicTest`'s sweep
  over directions, bounds, filters, walkers, chunk sizes, end labels, bound ends, the distinct
  mode and tombstones already compares against `ReferenceEnumerator`, so identical sorted rows
  with the store on and off is the correctness argument. The sweep must include a node whose
  candidates are cut by the budget at one depth and not at another, or the prefix cut passes
  untested.
- A fixture that fills the arena mid-walk, so the fallback path runs beside the store in one
  query.
- `path_bench`: candidate checks and wall time with the store on and off, on the shapes where
  the revisit factor is 1 (it must not lose) and where it is large.

## Risks

- **Ordering.** Sorting by distance changes the order rows come out. Unspecified already, and
  the v3 tests compare sorted rows, but a query with `LIMIT` returns a different set of rows
  than it does today. Say so before someone finds it.
- **Lifetime and threads.** The store in `NLExplorePathsLoopData` inherits whatever the
  distance index assumes: built once, read by every chunk, no lock. Confirm that assumption is
  actually held by the loop driver before storing anything bigger.
- **The predicate contract.** Memoising the hop predicate is sound only while the region reads
  nothing but its three arguments and constants. That is the op's documented contract; the
  verifier should be the thing that keeps it true, not this document.
- **Cap accounting.** A range handed out must stay valid for the whole query, so the arena
  cannot grow by reallocation under live frames unless the ranges are offsets, not pointers.
  Keep them offsets.
- **The fallback must stay exercised.** If the gate almost always builds the store, the
  per-visit path rots. The sweep runs both.

## Sources

- PathEnum (Sun, Chen, He, Hooi, SIGMOD 2021): https://arxiv.org/abs/2103.11137 - the index
  whose adjacency this borrows, and the prefix retrieval by sorted distance.
- `docs/vlp/PLAN.md` - the tiers, the gates and what stays deferred.
- `docs/path_bench.md` - the measurements the gate is calibrated against.
