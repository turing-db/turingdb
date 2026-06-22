# Page-table neighborhood index — is it worth it?

## Overview

Currently, reading the neighborhood of node means visiting *every* reachable datapart in the commit history and doing a hash probe in each to see whether edges have been added to that node. 

After a bulk load and a long tail of small commits, that's a lot of hash probes and almost all of them miss. 

We want to evaluate an idea: keep a per-commit **page-table directory** (a radix tree with NodeID bits as keys) that points to each node's neighborhood, so a read is a very short constant-depth tree walk (1-2 levels) instead of N hash probes. 

The tree is shared across commits. A new commit only rewrites the pages on the path from each changed node up to a fresh root. We want to know what gain it gives us on reads and what it costs on writes.

## How the simulator works

We implement a quick and dirty simulator as a sample tool that doesn't touch the engine. It replays a synthetic workload:
* bulk-load 1M nodes
* 2000 small commits that add edges to existing nodes, with hub skew so old nodes keep growing

It adds up approximate latencies per operation: cache/DRAM hits, hash probes, sequential edge scans, and page-copy bandwidth. 
It then reports read and write cost for the current design and for two flavors of the page-table: **delta** leaves (store just this commit's new edges) and **consolidated** leaves (store the node's fully merged neighborhood).

## Results 

Default run: 1M nodes, 2000 mutation commits. Memory is the index-structure overhead (the raw edges, ~288 MB, are identical for all three): per commit, and the total once the full 2000-commit history is retained.

| Design | Neighborhood read | Commit write | Index mem/commit | Index mem total | The catch |
|---|---|---|---|---|---|
| Current (probe every datapart) | ~190 µs | ~72 µs | ~39 KB | ~107 MB | read cost scales with commit-history depth |
| Page-table — delta leaves | ~300–360 ns (~530–640×) | ~158 µs (2.2×) | ~1.5 MB | ~3.0 GB | cheap reads, modest write overhead |
| Page-table — consolidated leaves | ~210–220 ns (~860–915×) | ~218 µs (3.0×) | ~1.7 MB | ~3.4 GB | fastest reads, but re-copies hub adjacency every touch |

## Conclusion

Reads get ~500–900× faster with 2000 commits, because the walk cost is fixed while today's probe cost grows with every commit. 
Writes get 2–3× more expensive from path-copying, mostly the root rewrite, which shrinks fast with a narrower root (`--level1-bits 10` drops delta to ~1.1×). 
The real price is memory: retaining a copied page tree per commit costs ~30× the current index (narrower pages and merge/compaction both bring it down). 
The delta variant is the sweet spot. The read win is largest on deep, rarely-merged histories and narrows once compaction keeps the datapart count low.
