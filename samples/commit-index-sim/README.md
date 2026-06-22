# commit-index-sim

A standalone cost simulator that evaluates an alternative neighborhood index for
TuringDB's versioned storage: a **per-commit, page-table-style directory of node
neighborhoods**, compared against the current design.

It does not touch the storage engine and links nothing but `argparse`. It does
not implement the index — it estimates the per-operation cost of each design
under a synthetic commit workload, using approximate latencies, so the idea can
be judged before any engine work.

## The two designs

**Current ("probe every reachable datapart").** A commit's reachable history is
an ordered list of immutable dataparts. To read a node's neighborhood, the
engine visits *every* reachable datapart and asks its `EdgeIndexer` for that
node's edges (`storage/iterators/GetInEdgesIterator.cpp`,
`storage/indexers/EdgeIndexer.cpp`):

- the datapart that *created* the node answers by direct array index;
- every *newer* datapart is consulted via an `unordered_map<NodeID, size_t>`
  patch-node probe — a hit if that commit added edges to the node, a miss
  otherwise.

So a neighborhood read costs one probe per reachable datapart. After a bulk load
plus *D* small mutation commits, every read of an original node pays ~*D* patch
probes — **most of them misses** — regardless of the node's degree. Writes, on
the other hand, are cheap: a commit just builds and appends its own datapart and
never touches existing ones.

**Page-table directory.** A sparse radix tree keyed by `NodeID`: the top
`--level1-bits` index a root page directory, each deeper level consumes
`--page-bits` more bits, and the leaf points at the node's neighborhood. The
tree is persistent — commits share substructure, and a new commit path-copies
only the pages from each modified leaf up to a fresh root. A read is a fixed
depth-*L* walk straight to the node's entry, with **no per-datapart probing**.

Two leaf variants are modeled:

- **consolidated** — the leaf holds the node's full merged neighborhood, so a
  read yields a single span. Best reads, but a write must re-copy a touched
  node's existing adjacency before appending (write amplification on hubs).
- **delta** — the leaf chains a small per-commit neighborhood delta. Writes stay
  cheap; a read still gathers one span per commit that touched the node, but
  pays none of the miss probes the current design does.

## Workload model

An initial bulk load creates every node and its initial edges, followed by a
long tail of small mutation commits, each adding edges to already-existing nodes
(the patch-edge case). Edge endpoints are drawn with a tunable skew (`--skew`,
>1 concentrates on low-id "hub" nodes, modelling preferential attachment), so
old hubs keep accreting edges across many commits — the worst case for the
current design. The run is fully deterministic for a given `--seed`.

## What it measures

- **Reads:** mean / median / p99 latency per neighborhood lookup for each
  design, under a hub-weighted ("hot") and a uniform read mix, plus speedups.
- **Writes:** mean / max latency to commit one mutation commit, pages rewritten
  per commit, and the persistent index bytes each commit adds.
- **Index space:** one-time directory size and projected growth over the run.

## Build & run

```bash
cd build && make commit-index-sim
./samples/commit-index-sim/commit-index-sim            # defaults
./samples/commit-index-sim/commit-index-sim --help     # all knobs
```

Useful experiments:

```bash
# Cheaper path-copy writes via a narrower root (deeper walk, still ~200 ns reads)
./commit-index-sim --level1-bits 10 --page-bits 6

# Post-compaction: only 50 reachable dataparts -> current reads recover
./commit-index-sim --commits 50

# Heavier hub skew and bigger commits
./commit-index-sim --skew 2.2 --edges-per-commit 5000

# Realistic append-heavy history: only a small fraction of commits patch
# existing nodes; the rest add new nodes and are skipped cheaply on reads
./commit-index-sim --patch-fraction 0.1
```

## Caveats

The latencies are coarse, order-of-magnitude constants (overridable via
`--dram-ns`, `--hash-hit-ns`, `--hash-miss-ns`, `--bandwidth`); the value is in
the *relative* comparison and the *scaling*, not absolute predictions. The
current-design read cost assumes no datapart merging — set `--commits` to the
expected post-compaction reachable-datapart count to model a merged history.
`--patch-fraction` models append-heavy histories: append-only commits hold an
empty patch map, so reading an existing node skips them at ~`_emptyHashProbeNs`
each — near-free per datapart, but still an O(D) walk over the whole history,
which is why the page-table keeps a large edge even as the fraction drops. Write
and memory figures are reported per *patching* commit. The model counts
out-edges only; in-edges are structurally symmetric.
