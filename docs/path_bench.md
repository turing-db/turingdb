# Variable-length paths on Reactome: v2, v3, ladybug and memgraph

Measured with `scripts/bench_paths.py`, which runs 71 Cypher queries in five groups
through both engines in the turingdb shell — v2 directly, v3 behind the `#v3` prefix —
and reports the median of five runs per query, the first discarded as a warmup. Groups A
to D are biologically meaningful questions; group E times the same patterns as a trail
walk (`count()`) and as a distinct-end search (`count(DISTINCT)`) across hop bounds.

Provenance: `9e9704046` (the sparse reach table, no distinct gate), default `-O3` build,
20-core Linux box, quiet machine.
Graph `reactome` loaded from its binary dump: 2,978,202 nodes and 11,537,843 edges (415
`TopLevelPathway`, 117,945 `Event`, 83,459 `Reaction`, 110,048 `Complex`).

```
scripts/bench_paths.py -reps 5 -verify
```

Both engines agreed on every result they both produced — row multisets, and aggregate
values up to the 125,690,888-path one — with a single exception, which turned out to be a
v2 bug (below). The same 71 queries were later run against ladybug and against memgraph on
the same graph and the same box; those are their own sections, after the v2 bug.

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

A third engine settles it from outside: ladybug returns 89,068 for this query under its
default recursive semantics and 86,520 for the same query written `TRAIL`, which is the
two numbers and the name of the difference between them.

## Ladybug on the same 71 queries

[Ladybug](https://github.com/LadybugDB/ladybug) 0.20.3, the fork that continues Kùzu, run
through its Python API on the same box and the same graph — 2,978,202 nodes and 11,537,843
edges converted out of turingdb's parquet dump of `reactome`, which copies in in 12 s. Same
protocol as above: five runs per query, the first discarded, median of the rest, with a
180 s timeout. Ladybug is embedded, so the process is the driver that holds the database; it
took all 20 cores and a 16 GB buffer pool.

Two things had to be decided before any of it could run. Reactome nodes carry six labels
each and ladybug is one label per node table, so every node goes into one `Node` table and
the labels the queries name become boolean columns: `(a:Event)` is written `(a:Node)` with
`a.isEvent`. And its recursive rels default to **walk** semantics, which counts the
repeated-edge paths v2 counts; `TRAIL` matches v3, so every quantifier below is written
`TRAIL`. Its upper bound is capped at 30 until `var_length_extend_max_depth` is raised,
here to 200, which is also what stands in for `+`.

**Every count ladybug produced matches v3's**, all 65 that finished — row multisets and
aggregates alike, up to the 125,690,888-path one. Six queries hit the 180 s timeout.

Ladybug indexes only the primary key, so `stId` seeds cost a scan of all 2.98M nodes. The
seeded queries are therefore timed twice, as written and re-seeded on the primary key, the
same split this page already makes when it subtracts v2's 45.81 ms seed lookup. The
`lb (pk)` column is the walk without the missing index.

| A. Property-seeded | v3 | lb | lb (pk) | |
|---|---|---|---|---|
| Find Signal Transduction by accession | 0.55 ms | 13.07 | 0.24 | **0.4×** |
| Its direct sub-events | 0.85 | 6.62 | 1.57 | 1.8× |
| Its sub-events exactly 3 levels down | 1.02 | 12.74 | 6.54 | 6.4× |
| Its sub-events exactly 4 levels down | 1.29 | 16.84 | 11.92 | 9.2× |
| Reactions exactly 4 steps downstream of the hub | 1.20 | 4.87 | 1.00 | **0.8×** |
| Sub-units of the complex, 2 levels in | 1.22 | 5.25 | 0.80 | **0.7×** |

Ladybug's floor is the lower of the two: a primary-key lookup answers the seed in 0.24 ms
against v3's 0.55, and the two shallowest walks finish under v3's time. Everything after
this table is the walk.

| B. Label-seeded typed traversal | v3 | lb | | count |
|---|---|---|---|---|
| Events 2 `hasEvent` levels under all 415 top-level pathways | 1.46 ms | 16.22 | 11.1× | 11,630 |
| …3 levels | 3.37 | 30.57 | 9.1× | 28,799 |
| …4 levels | 7.06 | 62.46 | 8.8× | 40,012 |
| …5 levels | 11.40 | 101.18 | 8.9× | 28,679 |
| …6 levels | 13.98 | 126.26 | 9.0× | 10,260 |
| Every sub-unit two levels inside every complex | 30.12 | 1,915.89 | **63.6×** | 213,396 |
| Reaction triples three `precedingEvent` steps apart | 17.53 | 1,358.53 | **77.5×** | 86,520 |
| Reactions two `hasEvent` levels under every top-level pathway | 1.17 | 10.02 | 8.6× | 6,371 |
| Every reaction's input entities | 7.75 | 17.91 | 2.3× | 186,017 |

| C. Untyped variable-length | v3 | lb | | paths |
|---|---|---|---|---|
| Within 2 hops of the 415 top-level pathways | 1.42 ms | 503.35 | 355× | 34,669 |
| Within 3 hops of them | 4.90 | 1,202.90 | 246× | 228,878 |
| Events within 3 hops of them | 7.86 | 1,186.66 | 151× | 90,086 |
| Within 2 hops of all 83,459 reactions | 85.88 | 91,956.57 | **1,071×** | 4,788,031 |
| Within 2 hops in either direction | 836.27 | 25,939.23 | 31× | 125,690,888 |

**Untyped is where ladybug is furthest behind, and its cost there follows the seeds, not
the paths.** 83,459 reactions two hops out is 4,788,031 paths and takes 92 s; 415 pathways
two hops out in both directions is 125,690,888 paths — twenty-six times as many — and
takes 26 s. An untyped quantifier has to walk all 88 rel tables, and ladybug appears to pay
that per seed. The undirected row is its best untyped showing only because it is v3's worst.

| D. Queries v2 cannot express | v3 | lb | lb (pk) | | rows |
|---|---|---|---|---|---|
| Signal Transduction's entire sub-event tree, any depth | 2.23 ms | 21.80 | 16.63 | 7.5× | 3,154 |
| Every reaction it eventually decomposes into | 2.15 | 20.01 | 15.47 | 7.2× | 2,344 |
| Every reaction under every top-level pathway | 26.04 | 214.47 | — | 8.2× | 92,952 |
| Which top-level pathway contains the hub reaction | 1.20 | 5.93 | 1.01 | **0.8×** | 1 |
| Reactions up to 4 steps downstream of that hub | 1.22 | 6.25 | 2.33 | 1.9× | 96 |
| All reactions downstream of that hub, any distance, `DISTINCT` | 2.01 | `TIMEOUT` | `TIMEOUT` | — | 1,520 |
| That complex's whole sub-unit tree | 1.22 | 5.37 | 1.52 | 1.2× | 23 |
| Reaction pairs of one pathway sharing an input entity | 60.62 | 17,757.71 | — | 293× | 390,348 |

The `DISTINCT` cascade is the first timeout, and it is the same query v2 was killed on at
420 s. Group E says why.

### Ladybug has no distinct-end search

`count(DISTINCT z)` costs ladybug what `count(z)` costs it, at every bound — it enumerates
the trails and deduplicates the ends. So its search column inherits the walk's explosion,
where v3's is flat in the bound. Primary-key seeds throughout, against the v3 columns from
the table above:

| bound | v3 walk | lb walk | v3 search | lb search | trails | ends |
|---|---|---|---|---|---|---|
| `{1,4}` | 1.23 ms | 1.84 | 1.23 ms | 2.76 | 96 | 96 |
| `{1,8}` | 1.23 | 1.93 | 1.24 | 3.24 | 101 | 101 |
| `{1,16}` | 1.25 | 3.08 | 1.27 | 3.63 | 179 | 177 |
| `{1,24}` | 1.53 | 6.72 | 1.40 | 8.21 | 1,412 | 511 |
| `{1,32}` | 5.15 | 54.19 | 1.53 | 58.26 | 43,489 | 902 |
| `{1,40}` | 1,329 | 8,015.57 | 1.62 | **8,422.73** | 16,956,465 | 1,175 |
| `{1,44}` | 24,275 | `TIMEOUT` | 1.68 | `TIMEOUT` | 302,221,591 | 1,308 |
| `{1,100}` | — | — | 1.77 | `TIMEOUT` | — | 1,520 |
| `+` | — | — | 1.77 | `TIMEOUT` | — | 1,520 |

58.26 ms against 54.19 at 32 hops, 8,422 against 8,015 at 40: the two modes are one
enumeration. At `{1,40}` that is 5,199× v3's 1.62 ms for the same 1,175 ends, and past it
ladybug stops answering the question at all while v3 stays at 1.77 ms. Five of the six
timeouts are in this column.

The other four sweeps have no such divergence, because on those shapes the walk is cheap
enough that v3 searches for no gain either:

| sweep, bound | v3 walk | lb walk | v3 search | lb search |
|---|---|---|---|---|
| Signal Transduction's sub-events, `{1,2}` | 0.92 ms | 2.81 | 0.93 ms | 3.73 |
| …`{1,13}` | 1.94 | 14.00 | 1.85 | 14.74 |
| …`+` | 1.91 | 14.57 | 1.85 | 15.43 |
| Reactions under the 415 top-level pathways, `{1,3}` | 3.79 | 29.24 | 5.64 | 31.04 |
| …`{1,6}` | 16.33 | 128.48 | 20.91 | 136.12 |
| …`+` | 21.33 | 166.52 | 22.42 | 173.48 |
| Sub-units of every complex, `{1,2}` | 30.26 | 1,919.23 | 44.19 | 1,921.89 |
| …`{1,100}` | 52.62 | 2,546.39 | 59.07 | 2,558.75 |
| Reactions downstream of every reaction, `{1,3}` | 17.94 | 1,346.44 | 16.92 | 1,354.07 |
| …`+` | — | — | 138.34 | `TIMEOUT` |

The last row is the sixth timeout and the same story as the cascade: 45,331 distinct ends
that v3 reaches in 138 ms and ladybug cannot enumerate its way to.

### What the comparison says

On a typed walk ladybug runs a steady 6× to 11× v3 — 8.6× to 11.1× across the `hasEvent`
depth ladder, 5.5× to 9× across the group E sweeps, 7.2× to 8.2× on the unbounded typed
walks of group D. That multiple is flat in depth and in result size, which makes it a
per-edge constant rather than anything about the shape. It widens to 43×–78× where the
seeds are many and their balls overlap (110,048 complexes, 83,459 reactions) and to
151×–1,071× on untyped quantifiers.

The one qualitative gap is the distinct search. Everywhere else ladybug is slower by a
factor; on `count(DISTINCT)` over a re-converging relation it is on the wrong side of the
walk's exponential, which is the whole case for `PathReachTable` and for searching
unconditionally. Its own fixed costs are lower than v3's — 0.24 ms to look up a node
against 0.55 — so none of this is a floor it is paying.

Two caveats. The boolean-column encoding of labels is forced by ladybug's data model and
may cost it something a native multi-label match would not, and the as-written seed column
measures a missing secondary index rather than a traversal, which is why the `lb (pk)`
column is there.

## Memgraph on the same 71 queries

[Memgraph](https://memgraph.com) 3.12.0 community, in its official container on the same
box and the same graph — 2,978,202 nodes and 11,537,843 edges converted out of turingdb's
parquet dump of `reactome` into one CSV per labelset and one per edge type, which `LOAD
CSV` reads in 31 s into 2.25 GiB resident. Same protocol as above: five runs per query,
the first discarded, median of the rest, with a 180 s timeout.

Memgraph is schemaless and multi-label, so nothing about the queries had to be re-encoded:
every one below is the v3 query with the quantifier rewritten and nothing else —
`-[:hasEvent]->{3,3}` becomes `-[:hasEvent*3..3]->`, `+` becomes `*1..`. Its variable-length
expansion enforces relationship uniqueness, so `preceding_d3` returns 86,520, which is v3's
trail count and the third engine to say v2's 89,068 counts paths that reuse an edge.

Both engines run one core per query — measured, 100 % of one core each on the undirected
untyped walk — which ladybug's column above does not do, having taken all 20.

One cost is the protocol's and not the engine's, and it is held out in its own column. A
query that returns rows pays bolt: 1,169 stIds cost 6.8 ms of serializing and hydrating on
top of a 0.4 ms walk. The `mg count` column is the same query with its projection wrapped
in `count()`, so the rows never leave the server; it is the column to compare against v3,
which counts rows in the shell without formatting them. The round trip itself is 0.115 ms.

Four queries hit the 180 s timeout, all four `count(DISTINCT)` over `precedingEvent`. **Of
the 67 that finished, 66 return exactly v3's count**; the one that differs, and the one
count the `*BFS` column differs on, are semantics rather than error — both below.

| A. Property-seeded | v3 | mg | mg (count) | | rows |
|---|---|---|---|---|---|
| Find Signal Transduction by accession | 0.55 ms | 0.49 | 0.37 | **0.7×** | 1 |
| Its direct sub-events | 0.85 | 0.63 | 0.45 | **0.5×** | 17 |
| Its sub-events exactly 3 levels down | 1.02 | 3.17 | 0.24 | **0.2×** | 480 |
| Its sub-events exactly 4 levels down | 1.29 | 7.16 | 0.41 | **0.3×** | 1,169 |
| Reactions exactly 4 steps downstream of the hub | 1.20 | 0.14 | 0.15 | **0.1×** | 1 |
| Sub-units of the complex, 2 levels in | 1.22 | 0.12 | 0.13 | **0.1×** | 2 |

Memgraph is faster on every query in this group, by 1.5× to 9×. Its floor is 0.12 ms
against v3's 0.55, and a label-property index on `stId` answers the seed the way
`FuseScanByPropertyValue` does, so these queries measure two fixed costs and memgraph's is
the smaller one. The two counts this table never recorded are 1 and 2; the depth ladder
sums to the 96 of `cascade_1_4`, which both engines agree on.

| B. Label-seeded typed traversal | v3 | mg | | count |
|---|---|---|---|---|
| Events 2 `hasEvent` levels under all 415 top-level pathways | 1.46 ms | 1.93 | 1.3× | 11,630 |
| …3 levels | 3.37 | 5.23 | 1.6× | 28,799 |
| …4 levels | 7.06 | 16.65 | 2.4× | 40,012 |
| …5 levels | 11.40 | 25.64 | 2.3× | 28,679 |
| …6 levels | 13.98 | 29.38 | 2.1× | 10,260 |
| Every sub-unit two levels inside every complex | 30.12 | 68.30 | 2.3× | 213,396 |
| Reaction triples three `precedingEvent` steps apart | 17.53 | 34.91 | 2.0× | 86,520 |
| Reactions two `hasEvent` levels under every top-level pathway | 1.17 | 1.40 | 1.2× | 6,371 |
| Every reaction's input entities | 7.75 | 34.12 | **4.4×** | 186,017 |

| C. Untyped variable-length | v3 | mg | | paths |
|---|---|---|---|---|
| Within 2 hops of the 415 top-level pathways | 1.42 ms | 1.68 | 1.2× | 34,669 |
| Within 3 hops of them | 4.90 | 9.84 | 2.0× | 228,878 |
| Events within 3 hops of them | 7.86 | 18.86 | 2.4× | 90,086 |
| Within 2 hops of all 83,459 reactions | 85.88 | 254.44 | 3.0× | 4,788,031 |
| Within 2 hops in either direction | 836.27 | 4,427.69 | **5.3×** | 125,690,888 |

Untyped is where ladybug lost two to three orders of magnitude; memgraph pays 1.2× to 5.3×,
and its cost follows the paths rather than the seeds. It beats v2 on four of the five,
including the undirected 125,690,888-path row where v2 takes 7,118 ms, and ties on the
fifth at 254.44 ms against 253.96.

| D. Queries v2 cannot express | v3 | mg | mg (count) | | rows |
|---|---|---|---|---|---|
| Signal Transduction's entire sub-event tree, any depth | 2.23 ms | 20.51 | 1.18 | **0.5×** | 3,154 |
| Every reaction it eventually decomposes into | 2.15 | 14.93 | 0.89 | **0.4×** | 2,344 |
| Every reaction under every top-level pathway | 26.04 | 699.47 | 37.46 | 1.4× | 92,952 |
| Which top-level pathway contains the hub reaction | 1.20 | 0.33 | 0.15 | **0.1×** | 1 |
| Reactions up to 4 steps downstream of that hub | 1.22 | 0.66 | 0.17 | **0.1×** | 96 |
| All reactions downstream of that hub, any distance, `DISTINCT` | 2.01 | `TIMEOUT` | `TIMEOUT` | — | 1,520 |
| That complex's whole sub-unit tree | 1.22 | 0.28 | 0.15 | **0.1×** | 23 |
| Reaction pairs of one pathway sharing an input entity | 60.62 | 327.96 | — | 5.4× | 198,362 |

The typed unbounded walks off one seed are memgraph's, by 1.9× to 8×. The `DISTINCT` cascade
is the first timeout, the same query v2 was killed on at 420 s and ladybug timed out on.

### Memgraph has a distinct-end search, but `count(DISTINCT)` does not use it

`-[:precedingEvent *BFS 1..44]-` visits every reachable node once and is memgraph's own
answer to the question the distinct search answers; written as a quantifier and a
`count(DISTINCT)`, the same question enumerates the trails. So the search column below is
the walk's explosion, and the BFS column beside it is flat in the bound, like v3's:

| bound | v3 walk | mg walk | v3 search | mg search | mg `*BFS` | trails | ends |
|---|---|---|---|---|---|---|---|
| `{1,4}` | 1.23 ms | 0.33 | 1.23 ms | 0.22 | 0.23 | 96 | 96 |
| `{1,8}` | 1.23 | 0.16 | 1.24 | 0.20 | 0.22 | 101 | 101 |
| `{1,16}` | 1.25 | 0.14 | 1.27 | 0.15 | 0.19 | 179 | 177 |
| `{1,24}` | 1.53 | 0.35 | 1.40 | 0.45 | 0.51 | 1,412 | 511 |
| `{1,32}` | 5.15 | 5.96 | 1.53 | 6.32 | 1.10 | 43,489 | 902 |
| `{1,40}` | 1,329 | 2,060.11 | 1.62 | 2,299.87 | 1.61 | 16,956,465 | 1,175 |
| `{1,44}` | 24,275 | 37,842.01 | 1.68 | 42,285.42 | 1.82 | 302,221,591 | 1,308 |
| `{1,100}` | — | — | 1.77 | `TIMEOUT` | 2.26 | — | 1,520 |
| `+` | — | — | 1.77 | `TIMEOUT` | 2.26 | — | 1,520 |

42,285 ms against 37,842 at 44 hops: `count(DISTINCT)` costs the walk plus the
deduplication, exactly as it does on ladybug. Written `*BFS` the same 1,308 ends cost
1.82 ms, 1.08× v3's 1.68 — so the engine has the search, and the gap is that its planner
will not reach for it. That is the difference v3 removed by dropping
`searchPaysForDistinctEnds` and marking the exploration `distinct` from the aggregate.

The other four sweeps have no such divergence, the walk being cheap enough on those shapes
that v3 searches for no gain either. On those, `*BFS` is *slower* than memgraph's own walk
— 114.83 ms against 101.44 for the complexes' components, 51.67 against 44.34 for the
pathways' reactions — because a BFS over 110,048 seeds pays a frontier per seed where the
walk streams:

| sweep, bound | v3 walk | mg walk | v3 search | mg search | mg `*BFS` |
|---|---|---|---|---|---|
| Signal Transduction's sub-events, `{1,2}` | 0.92 ms | 0.18 | 0.93 ms | 0.17 | 0.18 |
| …`{1,13}` | 1.94 | 0.76 | 1.85 | 0.72 | 1.13 |
| …`+` | 1.91 | 0.79 | 1.85 | 0.76 | 1.30 |
| Reactions under the 415 top-level pathways, `{1,3}` | 3.79 | 6.48 | 5.64 | 6.93 | 10.03 |
| …`{1,6}` | 16.33 | 34.40 | 20.91 | 40.80 | 49.02 |
| …`+` | 21.33 | 37.31 | 22.42 | 43.65 | 51.63 |
| Sub-units of every complex, `{1,2}` | 30.26 | 72.22 | 44.19 | 101.44 | 114.83 |
| …`{1,100}` | 52.62 | 106.12 | 59.07 | 140.27 | 174.57 |
| Reactions downstream of every reaction, `{1,3}` | 17.94 | 39.62 | 16.92 | 45.88 | 54.77 |
| …`+` | — | — | 138.34 | `TIMEOUT` | 1,544.97 |

The last row is the fourth timeout and the same story as the cascade: 45,331 distinct ends
that v3 reaches in 138 ms, memgraph cannot enumerate its way to, and its BFS reaches in
1.5 s.

### The two counts that differ, and what each one is

`reactions_search_inf` is 45,331 in v3 and 45,296 under `*BFS`. The 35 missing nodes are
reachable from themselves and from nothing else within the bound: BFS measures a node's
shortest distance to itself as 0, below the quantifier's lower bound of 1, where the trail
walk finds it again around a cycle. At bound 3 the same difference is 36 of 45,252, and
3,039 reactions are self-reachable in three `precedingEvent` hops.

`shared_input` is 390,348 in v3 and 198,362 in memgraph, and the difference is exactly the
191,986 `(pathway, reaction, entity)` triples where `r1` and `r2` are the same reaction.
openCypher's relationship uniqueness spans the whole `MATCH` clause, so binding `r1` and
`r2` to one reaction would bind both `hasEvent` relationships to one relationship, which
memgraph — and Neo4j — exclude. v3 and ladybug both match homomorphically across the
comma-separated patterns and count the self-pairs, so the query as written asks for pairs
and gets each reaction paired with itself.

### What the comparison says

Core for core, memgraph runs a typed walk at 1.2× to 2.4× v3, flat in depth and result
size — the same shape as ladybug's multiple but a quarter of its size, and without the 20
cores. Untyped costs it 1.2× to 5.3× rather than ladybug's 151×–1,071×, and many seeds
whose balls overlap cost it 2.0×–2.7× rather than 43×–78×. It is ahead of v3 wherever the
query is small: every seeded query in group A, every unbounded walk off one seed in group
D, and every bound of the two single-seed sweeps in group E — a 0.12 ms floor against 0.55
buys a lot when the walk is 96 nodes.

The one qualitative gap is the one ladybug has too, and memgraph has the answer in the box:
`count(DISTINCT)` over a re-converging relation enumerates trails until it times out, while
`*BFS` on the same pattern returns the same ends in 1.8 ms. The difference is not the
algorithm but who chooses it — v3 marks the exploration `distinct` from the aggregate and
searches, memgraph makes it the query author's job.

Two caveats, both v3's. Its numbers were measured through the turingdb shell in-process,
where memgraph's cross a bolt socket even in the `mg count` column, worth 0.115 ms a query.
And this comparison uses memgraph community; the enterprise build's storage modes and
parallel execution are not in it.

## Reproducing

A single lock guards a turing dir and other sessions routinely hold `~/.turing`, so copy
the graph out before running:

```
mkdir -p ~/.turing-bench/graphs
cp -r ~/.turing/graphs/reactome ~/.turing-bench/graphs/
bench/vlp/bench_vlp.py reactome-paths -clients turingdb -reps 5
```

The 71 queries are the `reactome-paths` workload in `bench/vlp/workloads.py`. The v2 column
was measured by `scripts/bench_paths.py` before v2 was removed.

Run it on an idle box. v3's times are sensitive to competing load in a way v2's are not:
repeating groups B and C during a parallel build on the same machine left v2 unchanged
(7140 ms against 7143 on the last row) while every v3 time roughly doubled, taking that
row from 8.6× down to 4.7×.

`-groups` selects a subset of the five tables, `-only` a comma-separated list of query
ids. Every client reports the value of a one-cell result, so counts are compared across
engines and not just row counts.

The storage-level harness for the same work is `samples/path_bench`, which times the
explorator directly (candidate lookahead, the two index gates, the distinct search)
on generated out-of-cache graphs rather than through Cypher on Reactome.

For the ladybug column, `pip install ladybug`, then load the graph out of a parquet dump
of it and run the same 71 queries against it:

```
scripts/ladybug_load_reactome.py -dump ~/reactome-parquet-dump -db ~/.ladybug-bench/reactome
bench/vlp/bench_vlp.py reactome-paths -clients ladybug -ladybug-db ~/.ladybug-bench/reactome -reps 5 -timeout 180
```

`-timeout` bounds a query that will not finish; 180 s is what produced the six above. The
`lb (pk)` column re-seeded each query on the primary key; `bench_vlp.py` asks the queries
as written only.

For the memgraph column, run the server in its container with the import directory mounted,
convert the same parquet dump into the CSVs `LOAD CSV` reads, and run the 71 queries over
bolt:

```
docker run -d --name memgraph-bench -p 7687:7687 \
    -v ~/.memgraph-bench/import:/import:ro -v memgraph-bench-data:/var/lib/memgraph \
    memgraph/memgraph:latest \
    --storage-wal-enabled=false --storage-snapshot-interval-sec=0 --memory-limit=45000

pip install neo4j
scripts/memgraph_load_reactome_vlp.py -dump ~/reactome-parquet-dump -import ~/.memgraph-bench/import
bench/vlp/bench_vlp.py reactome-paths -clients memgraph -reps 5 -timeout 180
```

The `mg count` column timed every row-returning query with its projection wrapped in
`count()`, and the `mg BFS` column timed every distinct query as a `*BFS` expansion;
`bench_vlp.py` asks the queries as written only.
