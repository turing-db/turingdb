# Variable-length paths on Reactome: v2 against v3

Measured with `scripts/bench_paths.py`, which runs 71 Cypher queries in five groups
through both engines in the turingdb shell — v2 directly, v3 behind the `#v3` prefix —
and reports the median of five runs per query, the first discarded as a warmup. Groups A
to D are biologically meaningful questions; group E times the same patterns as a trail
walk (`count()`) and as a distinct-end search (`count(DISTINCT)`) across hop bounds.

Provenance: `9e9704046` (the sparse reach table, no distinct gate), default `-O3` build,
20-core Linux box, quiet machine.
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
| Find Signal Transduction by accession | 45.81 ms | 0.55 ms | **83×** |
| Its direct sub-events (17 rows) | 45.93 | 0.85 | **54×** |
| Its sub-events exactly 3 levels down (480) | 46.34 | 1.02 | **46×** |
| Its sub-events exactly 4 levels down (1,169) | 47.36 | 1.29 | **37×** |
| Reactions exactly 4 steps downstream of hub `R-HSA-2993780` | 46.22 | 1.20 | **38×** |
| Sub-units of complex `R-HSA-6814275`, 2 levels in | 46.24 | 1.22 | **38×** |

**Most of this is not a path-engine result.** Subtract the seed lookup on the first row
and what is left of v2 is its traversal: 0.12 ms at depth 1, 0.53 at depth 3, 1.55 at
depth 4 — though those are differences of two ~46 ms measurements, so their resolution is
poor. v3's own increment is 0.30 / 0.47 / 0.74 ms at the same depths. The bulk of the
multiple is `FuseScanByPropertyValue` turning a 2.98M-node scan into a ranged property
lookup; v3 only pulls ahead on the walk itself by depth 4.

These queries start from **one** node, so there is little for either engine to do. That is
what makes them a good probe of fixed costs and a bad probe of traversal throughput.

## B. Label-seeded typed traversal — the engine, with no scan in the way

v2 cannot write a typed quantifier at all, but it runs a chain of plain typed hops, which
is the same set at a fixed depth. Both sides return identical counts.

| Question | v2 | v3 | | count |
|---|---|---|---|---|
| Events 2 `hasEvent` levels under all 415 top-level pathways | 2.30 ms | 1.46 ms | 1.6× | 11,630 |
| …3 levels | 9.86 | 3.37 | 2.9× | 28,799 |
| …4 levels | 24.10 | 7.06 | 3.4× | 40,012 |
| …5 levels | 51.06 | 11.40 | **4.5×** | 28,679 |
| …6 levels | 60.62 | 13.98 | **4.3×** | 10,260 |
| Every sub-unit two levels inside every complex | 58.25 | 30.12 | 1.9× | 213,396 |
| Reaction triples three `precedingEvent` steps apart | 46.54 | 17.53 | 2.7× | see below |
| Reactions two `hasEvent` levels under every top-level pathway | 2.45 | 1.17 | 2.1× | 6,371 |
| Every reaction's input entities | 21.25 | 7.75 | 2.7× | 186,017 |

The margin grows with depth: the walk amortises v3's fixed per-query cost.

## C. Untyped variable-length — the only path shapes v2 will run

| Question | v2 | v3 | | paths |
|---|---|---|---|---|
| Within 2 hops of the 415 top-level pathways | 2.89 ms | 1.42 ms | 2.0× | 34,669 |
| Within 3 hops of them | 16.29 | 4.90 | 3.3× | 228,878 |
| Events within 3 hops of them | 25.19 | 7.86 | 3.2× | 90,086 |
| Within 2 hops of all 83,459 reactions | 253.96 | 85.88 | 3.0× | 4,788,031 |
| Within 2 hops in either direction | 7118.42 | 836.27 | **8.5×** | 125,690,888 |

The last row is the cleanest read on the explorator alone: 150M paths/s against 17.7M.

## D. Queries v2 cannot express at all

Asked the same question, v2 answers
`ANALYZE_ERROR: Edge type filters are not supported with variable-length paths yet`.
Every Reactome hierarchy is typed, so this covers most of what a biologist would ask.

| Question | v2 | v3 | rows |
|---|---|---|---|
| Signal Transduction's entire sub-event tree, any depth | `ANALYZE_ERROR` | 2.23 ms | 3,154 |
| Every reaction it eventually decomposes into | `ANALYZE_ERROR` | 2.15 | 2,344 |
| Every reaction under every top-level pathway | `ANALYZE_ERROR` | 26.04 | 92,952 |
| Which top-level pathway contains reaction `R-HSA-2993780` | `ANALYZE_ERROR` | 1.20 | 1 |
| Reactions up to 4 steps downstream of that hub | `ANALYZE_ERROR` | 1.22 | 96 |
| All reactions downstream of that hub, any distance, `DISTINCT` | `ANALYZE_ERROR` | 2.01 | 1,520 |
| That complex's whole sub-unit tree | `ANALYZE_ERROR` | 1.22 | 23 |
| Reaction pairs of one pathway sharing an input entity | `PLAN_ERROR: Common Successor Joins With Common Ancestor Unsupported` | 60.62 | 1 (390,348 pairs) |

The `DISTINCT` cascade is the query the distinct search answers where the all-trails walk
was killed at 420 s; group E sweeps its bound in both modes.

## E. Search against walk, by hop bound

The same pattern twice: `RETURN count(z)` enumerates every trail, which is the walk, and
`RETURN count(DISTINCT z)` is marked `distinct` by the pass and runs the multi-source
search. v2 refuses every one of them, the quantifiers being typed. The walk's column stops
at the last bound whose trails are enumerable in seconds.

**Reactions downstream of hub `R-HSA-2993780`**, `<-[:precedingEvent]-`, one seed:

| bound | walk | trails | search | distinct ends |
|---|---|---|---|---|
| `{1,4}` | 1.23 ms | 96 | 1.23 ms | 96 |
| `{1,8}` | 1.23 ms | 101 | 1.24 ms | 101 |
| `{1,16}` | 1.25 ms | 179 | 1.27 ms | 177 |
| `{1,24}` | 1.53 ms | 1,412 | 1.40 ms | 511 |
| `{1,32}` | 5.15 ms | 43,489 | 1.53 ms | 902 |
| `{1,40}` | 1,329 ms | 16,956,465 | 1.62 ms | 1,175 |
| `{1,44}` | 24,275 ms | 302,221,591 | 1.68 ms | 1,308 |
| `{1,100}` | — | — | 1.77 ms | 1,520 |
| `+` | — | — | 1.77 ms | 1,520 |

Past 24 hops the trails double every two levels while the ends barely grow: the walk
enumerates 302,221,591 trails for 1,308 ends at 44 hops and does not finish at
100, where the search costs its ball, 1.23 ms for 96 ends to 1.77 ms for all
1,520. `{1,100}` and `+` are the same query here: the ball is complete well inside 100 hops.

**Signal Transduction's sub-events**, `-[:hasEvent]->`, one seed, a hierarchy 13 deep:

| bound | walk | trails | search | distinct ends |
|---|---|---|---|---|
| `{1,2}` | 0.92 ms | 157 | 0.93 ms | 157 |
| `{1,4}` | 1.25 ms | 1,806 | 1.30 ms | 1,784 |
| `{1,8}` | 1.92 ms | 3,119 | 1.85 ms | 2,994 |
| `{1,13}` | 1.94 ms | 3,154 | 1.85 ms | 2,997 |
| `{1,100}` | 1.92 ms | 3,154 | 1.85 ms | 2,997 |
| `+` | 1.91 ms | 3,154 | 1.85 ms | 2,997 |

The two agree within 5 % at every bound. The hierarchy has almost no re-convergence, so
there are no trails for the search to save and no fixed cost left for it to pay.

**Reactions under the 415 top-level pathways**, `-[:hasEvent]->`:

| bound | walk | trails | search | distinct ends |
|---|---|---|---|---|
| `{1,3}` | 3.79 ms | 24,834 | 5.64 ms | 24,127 |
| `{1,6}` | 16.33 ms | 86,616 | 20.91 ms | 79,761 |
| `{1,100}` | 18.48 ms | 92,952 | 22.39 ms | 81,798 |
| `+` | 21.33 ms | 92,952 | 22.42 ms | 81,798 |

**Sub-units of every complex**, `-[:hasComponent]->`, 110,048 seeds:

| bound | walk | trails | search | distinct ends |
|---|---|---|---|---|
| `{1,2}` | 30.26 ms | 487,883 | 44.19 ms | 143,187 |
| `{1,100}` | 52.62 ms | 740,283 | 59.07 ms | 143,187 |

**Reactions downstream of every reaction**, `<-[:precedingEvent]-`, 83,459 seeds:

| bound | walk | trails | search | distinct ends |
|---|---|---|---|---|
| `{1,3}` | 17.94 ms | 208,580 | 16.92 ms | 45,252 |
| `+` | — | — | 138.34 ms | 45,331 |

On the tree-shaped hierarchies the search costs 1.05× to 1.5× the walk — 5.64 against
3.79 ms for the pathways' reactions within 3 hops, 44.19 against 30.26 for the
complexes' components two levels in — which is its constant: one table probe per relaxed
edge against one candidate check, on shapes where each node is reached by one trail anyway.
Where the seeds' balls re-converge it is level or ahead (16.92 against 17.94 ms for
the reactions within 3 hops), and where the trails multiply it is the only one that
finishes: the transitive downstream set of every reaction, 45,331 distinct ends, takes
138.34 ms.

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
bound. Group E above sweeps the bound in both modes.

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

`-groups` selects a subset of the five tables, `-only` a comma-separated list of query
ids. `-verify` re-runs the counting queries with output on so their values are compared
and not just their row counts — the disagreement above is invisible without it, since
both engines return one row either way.

The storage-level harness for the same work is `samples/path_bench`, which times the
explorator directly (walker count, lookahead, the two index gates, the distinct search)
on generated out-of-cache graphs rather than through Cypher on Reactome.
