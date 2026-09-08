# Variable-length paths on Reactome: v2 against v3

Measured with `scripts/bench_paths.py`, which runs 28 biologically meaningful Cypher
queries through both engines in the turingdb shell — v2 directly, v3 behind the `#v3`
prefix — and reports the median of five runs per query, the first discarded as a warmup.

Provenance: this commit, default `-O3` build, 20-core Linux box, quiet machine.
Graph `reactome` loaded from its binary dump: 2,978,202 nodes and 11,537,331 edges (415
`TopLevelPathway`, 117,945 `Event`, 83,459 `Reaction`, 110,048 `Complex`).

```
scripts/bench_paths.py -reps 5 -verify
```

Both engines agreed on every result they both produced — row multisets, and aggregate
values up to the 125,690,888-path one — with a single exception, which turned out to be a
v2 bug (below).

## A. Property-seeded typed queries — what a curator actually writes

| Question | v2 | v3 | |
|---|---|---|---|
| Find Signal Transduction by accession | 45.77 ms | 0.56 ms | **82×** |
| Its direct sub-events (17 rows) | 45.80 | 0.85 | **54×** |
| Its sub-events exactly 3 levels down (480) | 46.10 | 1.01 | **46×** |
| Its sub-events exactly 4 levels down (1,169) | 47.28 | 1.28 | **37×** |
| Reactions exactly 4 steps downstream of hub `R-HSA-2993780` | 46.15 | 1.20 | **38×** |
| Sub-units of complex `R-HSA-6814275`, 2 levels in | 46.04 | 1.21 | **38×** |

**Most of this is not a path-engine result.** Subtract the seed lookup on the first row
and what is left of v2 is its traversal: 0.03 ms at depth 1, 0.33 at depth 3, 1.51 at
depth 4 — though those are differences of two ~46 ms measurements, so their resolution is
poor. v3's own increment is 0.29 / 0.45 / 0.72 ms at the same depths. The bulk of the
multiple is `FuseScanByPropertyValue` turning a 2.98M-node scan into a ranged property
lookup; v3 only pulls ahead on the walk itself by depth 4.

These queries start from **one** node, so there is little for either engine to do. That is
what makes them a good probe of fixed costs and a bad probe of traversal throughput.

## B. Label-seeded typed traversal — the engine, with no scan in the way

v2 cannot write a typed quantifier at all, but it runs a chain of plain typed hops, which
is the same set at a fixed depth. Both sides return identical counts.

| Question | v2 | v3 | | count |
|---|---|---|---|---|
| Events 2 `hasEvent` levels under all 415 top-level pathways | 2.40 ms | 1.46 ms | 1.7× | 11,630 |
| …3 levels | 10.19 | 3.37 | 3.0× | 28,799 |
| …4 levels | 24.57 | 7.04 | 3.5× | 40,012 |
| …5 levels | 50.89 | 11.26 | **4.5×** | 28,679 |
| …6 levels | 62.06 | 13.78 | **4.5×** | 10,260 |
| Every sub-unit two levels inside every complex | 57.48 | 30.29 | 1.9× | 213,396 |
| Reaction triples three `precedingEvent` steps apart | 46.36 | 17.60 | 2.6× | see below |
| Reactions two `hasEvent` levels under every top-level pathway | 2.53 | 1.17 | 2.2× | 6,371 |
| Every reaction's input entities | 21.26 | 7.78 | 2.7× | 186,017 |

The margin grows with depth: the walk amortises v3's fixed per-query cost.

## C. Untyped variable-length — the only path shapes v2 will run

| Question | v2 | v3 | | paths |
|---|---|---|---|---|
| Within 2 hops of the 415 top-level pathways | 3.17 ms | 1.43 ms | 2.2× | 34,669 |
| Within 3 hops of them | 17.02 | 4.87 | 3.5× | 228,878 |
| Events within 3 hops of them | 25.84 | 7.83 | 3.3× | 90,086 |
| Within 2 hops of all 83,459 reactions | 258.80 | 85.62 | 3.0× | 4,788,031 |
| Within 2 hops in either direction | 7290.65 | 830.18 | **8.8×** | 125,690,888 |

The last row is the cleanest read on the explorator alone: 151M paths/s against 17.2M.

## D. Queries v2 cannot express at all

Asked the same question, v2 answers
`ANALYZE_ERROR: Edge type filters are not supported with variable-length paths yet`.
Every Reactome hierarchy is typed, so this covers most of what a biologist would ask.

| Question | v2 | v3 | rows |
|---|---|---|---|
| Signal Transduction's entire sub-event tree, any depth | `ANALYZE_ERROR` | 2.19 ms | 3,154 |
| Every reaction it eventually decomposes into | `ANALYZE_ERROR` | 2.13 | 2,344 |
| Every reaction under every top-level pathway | `ANALYZE_ERROR` | 26.22 | 92,952 |
| Which top-level pathway contains reaction `R-HSA-2993780` | `ANALYZE_ERROR` | 1.22 | 1 |
| Reactions up to 4 steps downstream of that hub | `ANALYZE_ERROR` | 1.26 | 96 |
| All reactions downstream of that hub, any distance, `DISTINCT` | `ANALYZE_ERROR` | 32.71 | 1,520 |
| That complex's whole sub-unit tree | `ANALYZE_ERROR` | 1.25 | 23 |
| Reaction pairs of one pathway sharing an input entity | `PLAN_ERROR: Common Successor Joins With Common Ancestor Unsupported` | 61.10 | 1 |

The `DISTINCT` cascade is the query the unconditional-search rule fixed: as an all-trails
walk it was killed at 420 s.

## The gate that cost more than the walk

The first run of this benchmark showed v3 losing the traversal on group A, and the cause
was not the walk. An end-constrained exploration asks `PathDistanceIndex::isWorthBuilding`
whether a pruning index would pay, and that estimate needs the walked type's branching,
which `sampleBranching` measured by striding over 4,096 nodes and reading both edge lists
of each — 8,192 cold probes across a 2.4 GB graph, before a single edge of the walk. It
ran per chunk until the index was built, so a query that visits 17 nodes paid a pass over
the whole adjacency, and a multi-chunk query paid several.

The branching depends only on the parts, the direction and the edge type — nothing about
the query's seeds or hop bound — so it is now memoised on the commit's data
(`EdgeBranchingCache`), keyed by direction and type and invalidated by the part counts it
was measured on. Same protocol, before and after:

| Query | before | after | |
|---|---|---|---|
| Its direct sub-events | 1.33 ms | 0.85 | −0.48 |
| Its sub-events exactly 3 levels down | 1.48 | 1.01 | −0.47 |
| Reactions exactly 4 steps downstream of the hub | 1.66 | 1.20 | −0.46 |
| Events 4 `hasEvent` levels under all top-level pathways | 7.88 | 7.04 | −0.84 |
| Events 6 levels under all of them | 14.87 | 13.78 | −1.09 |
| Signal Transduction's entire sub-event tree | 2.65 | 2.19 | −0.46 |
| Which top-level pathway contains the reaction | 1.64 | 1.22 | −0.42 |
| **Sub-units of a complex, 2 levels in** (no end label) | 1.26 | 1.21 | −0.05 |
| **That complex's whole sub-unit tree** (no end label) | 1.25 | 1.25 | 0.00 |
| **Within 2 hops of the top-level pathways** (no end label) | 1.42 | 1.43 | +0.01 |
| **Within 3 hops of them** (no end label) | 4.88 | 4.87 | −0.01 |

Every end-constrained query drops by one sampling pass, 0.42–0.51 ms; the deeper ones drop
0.84–1.09 ms, being the multi-chunk queries that were re-sampling per chunk. The four
queries with no end constraint never reach the gate and do not move, which is what makes
the attribution a measurement rather than a story. The one untyped end-labelled query
saves only 0.13 ms, because `countMatching` returns `edges.size()` without reading an edge
record when there is no type to match, so its sample never faults the adjacency in.

What remains of v3's floor is ~0.55 ms for any query plus ~0.3 ms to start a walk. The
`DISTINCT` cascade moved +1.40 ms, which is inside the noise of a query that first-touches
95 MB — the dense `ReachWords` array, the fixed cost the next section removes.

## The gate that never searched

The distinct search had a gate of its own in front of it, `searchPaysForDistinctEnds`,
which compared the walk's estimated candidate checks against the search's estimated
relaxations and let only an unbounded quantifier through unconditionally. The estimate
caps its levels at 254 and floors the fan-out at 1, so on `precedingEvent` (fan-out 0.29)
both sides saturate near 16,000 checks and the gate chose the walk at every finite bound:
`<-[:precedingEvent]-{1,100}(d:Reaction) RETURN DISTINCT d.stId` from the hub ran past
28 s where the same query written `+` took 32 ms, and so did `{1,50000}`. The search
expands each (seed, node) pair at most once where the walk expands it at least once, so
no estimate can save more than the search's constant while the walk can lose without
bound.

The gate is gone: a `distinct` exploration always searches. What made the search cost a
flat 31 ms was `setDistinctEnds` writing one 32-byte record per node of the graph, 95 MB
first-touched per input chunk whatever the ball. The records now live in `PathReachTable`,
an open-addressing table keyed by the nodes a batch reaches, cleared at the cost of those
nodes. Same protocol as above, `RETURN DISTINCT` throughout; the third column is the old
binary with the gate patched to return true:

| Query | rows | walk (gate) | dense search | sparse search |
|---|---|---|---|---|
| Hub reactions within 32 `precedingEvent` hops | 902 | 7.4 ms | 34.3 | 0.88 |
| Within 44 | 1,308 | 34,525 | 32.5 | 0.92 |
| Within 100 | 1,520 | killed at 28 s | 33.0 | 0.93 |
| Unbounded | 1,520 | 32.2 (searched) | 33.2 | 0.91 |
| Signal Transduction's events within 100 `hasEvent` hops | 2,997 | 2.0 | 32.7 | 0.95 |
| Unbounded | 2,997 | 32.3 (searched) | 32.6 | 0.93 |
| Events within 100 hops of a `Species` | 0 | 0.32 | 31.0 | 0.31 |
| Reactions within 100 hops of the 415 top-level pathways | 81,798 | 39.9 | 59.7 | 30.2 |
| Within 3 hops | 24,127 | 6.5 | 38.0 | 5.7 |
| `count(DISTINCT b)` of every complex's components within 100 | 1 (110,048 seeds) | 84.2 | 120.8 | 61.2 |

The dense column minus the sparse one is the 31 ms allocation, paid once per input chunk
(twice for the 110,048 complexes). Against the walk the search is never slower here, and
on the shapes whose seeds share sub-trees it is a quarter to a third faster.

The search's worst case is seeds whose balls share nothing. On `path_bench`'s default shape
(2M nodes, degree 8, 1,000 seeds, hops 1 to 3, 583,905 distinct pairs of 584,000 rows) the
sparse search takes 21.8 ms against 16.1 for the walk, the 1.35× the dense array measured
before it - the table costs nothing the dense words did not.

## A v2 correctness bug, found by the comparison

"Reaction triples three `precedingEvent` steps apart" is the one query where the engines
disagree: v2 counts 89,068 and v3 counts 86,520. The difference is exactly 2,548, and
there are exactly 2,548 ordered reciprocal `precedingEvent` pairs:

```
#v3 MATCH (a:Reaction)-[:precedingEvent]->(b:Reaction), (b)-[:precedingEvent]->(a) RETURN count(a)
2548
```

(v2 refuses that one too, with the same `PLAN_ERROR` as the shared-input query above.)

v2's three-hop chain counts walks that traverse the same relationship twice — edges
`a→r`, `r→a`, `a→r` — which openCypher's relationship isomorphism forbids. v3's trail
semantics excludes them. At depth two both return 64,698, since a directed walk cannot
reuse an edge in two steps. So on this shape v2 is both slower and wrong.

## Reproducing

A single lock guards a turing dir and other sessions routinely hold `~/.turing`, so copy
the graph out before running:

```
mkdir -p ~/.turing-bench/graphs
cp -r ~/.turing/graphs/reactome ~/.turing-bench/graphs/
scripts/bench_paths.py -reps 5 -verify
```

Run it on an idle box. v3's times are sensitive to competing load in a way v2's are not:
repeating groups B and C during a parallel build on the same machine left v2 unchanged
(7140 ms against 7143 on the last row) while every v3 time roughly doubled, taking that
row from 8.6× down to 4.7×.

`-groups` selects a subset of the four tables, `-only` a comma-separated list of query
ids. `-verify` re-runs the counting queries with output on so their values are compared
and not just their row counts — the disagreement above is invisible without it, since
both engines return one row either way.

The storage-level harness for the same work is `samples/path_bench`, which times the
explorator directly (walker count, lookahead, the three index gates, the distinct search)
on generated out-of-cache graphs rather than through Cypher on Reactome.
