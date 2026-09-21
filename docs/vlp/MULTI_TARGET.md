# Ending a walk on many targets: `end_nodes`

## The three ways to bind the end

`MATCH (a {stId:'X'})-[e*1..N]->(b {stId:'Y'})` can reach the engine in three shapes, and
they differ only in when the end is known:

| Shape | db IR | When the end is known |
|---|---|---|
| Post-filter | `explore_paths` then `get_node_properties` on `tgtids`, `eq`, `filter` | After the walk, one property read per end the walk produced |
| Bound end | `explore_paths(%seeds, {%target}) ... end_column 0` | Before the walk, one target per row |
| End set | `explore_paths(%seeds, {}) ... end_nodes %targets` | Before the walk, one set for every row |

The post-filter is what codegen emits today for a property-pinned end. `end_column` is what
`fuse_explore_end_nodes` produces when the end is a carried node column, which a query gets
by binding both endpoints before the pattern (`MATCH (a {...}), (b {...}) WITH a, b MATCH
(a)-[e*1..N]->(b)`). `end_nodes` is the set operand; nothing emits it from Cypher yet.

## Measurements

Reactome, 2,978,202 nodes and 11,537,331 edges. Seed Signal Transduction `R-HSA-162582`,
untyped forward hops. Each shape is a hand-written db program run by `samples/mlir`
(`-f prog.mlir -g <graph> -e -quiet`); the figure is the `DBDialectInterpreter` execution
time, one run each, so read the large gaps and not the last 10%. Every shape returned the
same rows at every bound, which is what makes the times comparable.

One target, `R-DDI-5653968`:

| hops | trails in the ball | plain walk | post-filter | `end_column` | `end_nodes` | rows |
|---|---|---|---|---|---|---|
| 1..3 | 8,367 | 0.59 ms | 1.12 ms | 2.72 ms | 2.59 ms | 3 |
| 1..4 | 71,069 | 1.67 ms | 4.95 ms | 3.46 ms | 3.45 ms | 3 |
| 1..5 | 524,470 | 7.11 ms | 23.5 ms | 8.20 ms | 8.62 ms | 18 |
| 1..6 | 3,541,449 | 41.4 ms | 136 ms | 36.7 ms | 41.3 ms | 68 |
| 1..7 | 22,833,038 | 262 ms | 838 ms | 67.5 ms | 69.9 ms | 457 |
| 1..8 | 142,129,263 | 1,615 ms | 5,074 ms | 74.2 ms | 74.7 ms | 2,657 |

Many targets, taken from the ends the seed reaches in exactly three hops:

| targets | hops | `end_column` | `end_nodes` | rows |
|---|---|---|---|---|
| 200 | 1..5 | 9.05 ms | 7.95 ms | 7,016 |
| 200 | 1..6 | 24.1 ms | 43.9 ms | 37,977 |
| 200 | 1..7 | 316 ms | 272 ms | 252,517 |
| 200 | 1..8 | 745 ms | 482 ms | 1,467,849 |
| 2000 | 1..6 | 75,134 ms | 96 ms | 1,167,094 |
| 2000 | 1..7 | 485,705 ms | 651 ms | 7,697,006 |

## What the numbers say

**A bound end beats the post-filter from four hops on.** At three hops the post-filter wins,
1.12 ms against 2.59, because the ball is too small to pay for the reverse search that builds
the target index. At four it has lost, 4.95 against 3.45. The post-filter tracks the ball -
it reads `stId` on every end the walk produces - so the gap grows with every hop: 68x at
eight hops, where it also costs 3.1x the unconstrained walk that emits all 142 million rows.

**With one target the two bound forms are the same.** 2.72/2.59, 3.46/3.45, 8.20/8.62,
36.7/41.3, 67.5/69.9, 74.2/74.7 ms. The widest gap is 12% at one bound on single runs, which
is nothing: with one target both forms walk once, pruned against one node.

**The set form takes over somewhere between 200 and 2000 targets.** At 200 it is a wash, ahead
in three bounds of four. At 2000 it is 96 ms against 75 s at six hops and 651 ms against 486 s
at seven, around 750x, because `end_column` is one walk per target: the cross product that
pairs 2000 targets with the seed makes 2000 rows, and its cost model then refuses the index
that would prune them, so it walks the ball 2000 times unpruned.

## Where the threshold comes from

It is an artefact of `PathTargetIndex`, not of the shape of the problem. The index records
*which* of 64 targets reached each node, so it is built and priced one reverse search per 64
targets: 32 of them at 2000 targets, which the gate declines. A set-bound walk never asks
which target is in reach, only whether any is, and one multi-source reverse search from the
whole set answers that at any size.

Probing the executor confirms the gate is what decides these cells. With one target the set
index is declined at six hops and built at seven and eight. With 200 targets it is declined at
six and seven and built only at eight, so those two runs walk the whole ball and test
membership at the ends - 45 ms against a 41 ms ball.

A set mode for `PathTargetIndex` - one search from every target at once, a distance to the
nearest target per node instead of a word of target bits, priced as one search rather than
`ceil(k/64)` - would build the index where it is now refused and put the set form at least
level with the paired one from a single target upward.

## Choosing the shape

Three decisions, taken at two moments. The pass sees the shape of the query and nothing of
the data; the executor sees the seeds, the targets and what a sample of the seeds expands to.
Each decides what it can see.

**Per row or shared: the pass, from the shape.** `end_column` is one walk per row. When the
target column is a factor of a cross product, every row carries the same targets, and the set
walk never does more than the per-row walks: a prefix it expands can reach some target, which
the per-row walks expand once for every target it reaches, and its index is one search from
the set where theirs is one per 64 targets. With one target the two are equal. So an end
column yielded by a cross-product factor is rewritten to `end_nodes`, that factor becoming the
set and the other the walk's input. `end_column` stays for a target correlated with the seed,
bound by a hop from it or joined to it: there a row's own target prunes more than the union
of every row's would, and the walk runs once per row either way.

The one cell the column form won, 200 targets at six hops, 24 ms against 44, is the gate
below and not the shape: the per-target index was built and the set index declined.

**Post-filter or set: the pass, with no number in it.** The post-filter pays for the bound
end at the wrong time. The ball is walked unpruned and each row it emits reads a property,
25 ns a row (31, 27, 25 and 24 ns per emitted row at five to eight hops in the sweep). The
set pays for its scan once, 0.7 ns a node of the graph:

| scan | rows | time |
|---|---|---|
| `stId = R-DDI-5653968`, no label | 1 | 2.1 ms |
| `stId = R-HSA-162582` within `TopLevelPathway` | 1 | 0.04 ms |

With the set in hand the walk tests each end it produces for membership, which the 200-target
runs put within 6% of the plain walk against 2x to 3.3x for the property read, and the index
becomes possible at all.

The unlabelled scan is the whole price of the set, and it buys the post-filter 2.98M x 0.7 /
25 = 83,000 rows: the seed passes that between three hops (8,367 trails) and four (71,069),
which is where the sweep saw the post-filter lose. Below it the post-filter wins by at most
one scan; above it the set wins by the ball, 68x at eight hops. The pass cannot place a query
on either side: the ball is the seed's own expansion and not the type's fan-out (Signal
Transduction reaches 8,367 trails in three hops on a graph whose mean out-degree is 3.9), and
the seeds exist only when the query runs. A loss bounded by one scan against a win bounded by
nothing is a rule without a threshold: a property-pinned end becomes a scan of its own, the
end's labels folded into it, and an `end_nodes` operand. The post-filter remains for a
predicate that cannot be a scan, one that reads both ends.

Materialising both ends before the walk is also what choosing the direction needs, and the
post-filter forecloses it. From `R-DDI-5653968` backwards the same bounds cost:

| hops | trails backward | time | trails forward | time |
|---|---|---|---|---|
| 1..5 | 5,326 | 2.2 ms | 524,470 | 7.1 ms |
| 1..6 | 35,840 | 2.7 ms | 3,541,449 | 41 ms |
| 1..7 | 235,313 | 5.0 ms | 22,833,038 | 262 ms |
| 1..8 | 1,412,716 | 17 ms | 142,129,263 | 1,615 ms |

The backward times include the 2.1 ms scan. Picking the side is deferred (PLAN.md), but it is
an executor decision over two materialised sets and two seed samples, and this shape is the one
that has both.

**Prune or test: the executor, from the numbers.** With S seeds, T targets and the seeds'
expansion sampled, the gate is `isWorthBuilding`: build iff the enumeration the sample
predicts costs more than the index, the index priced for what it is. A set-bound walk asks
only whether any target is in reach, so its index is one multi-source search from all T
targets, a distance byte per node: `PathTargetIndex::buildSet`, which is `PathDistanceIndex`
built from the set instead of a label's nodes. `isWorthBuildingSet` prices it as that index
prices its own build, 0.35 checks per node and edge the search touches, at most T times one
target's reach and at most the graph, plus the byte written for every node. A per-row walk
keeps its batches of 64 distinct targets, each priced by the targets it holds. Until
2026-09-21 the set was priced as ceil(T/64) batches and every batch at 64 targets' reach, 64
times over for a single target, which is what kept the one-target index unbuilt below seven
hops.

Both gates re-measured on the same programs, same machine, one run each:

| targets | hops | `end_column` before | after | `end_nodes` before | after | rows |
|---|---|---|---|---|---|---|
| 1 | 1..3 | 2.72 ms | 2.85 ms | 2.59 ms | 2.60 ms | 3 |
| 1 | 1..4 | 3.46 ms | 2.75 ms | 3.45 ms | 3.46 ms | 3 |
| 1 | 1..5 | 8.20 ms | 2.89 ms | 8.62 ms | 3.71 ms | 18 |
| 1 | 1..6 | 36.7 ms | 3.50 ms | 41.3 ms | 3.74 ms | 68 |
| 1 | 1..7 | 67.5 ms | 3.83 ms | 69.9 ms | 4.07 ms | 457 |
| 1 | 1..8 | 74.2 ms | 4.82 ms | 74.7 ms | 4.76 ms | 2,657 |
| 200 | 1..5 | 9.05 ms | 8.87 ms | 7.95 ms | 3.14 ms | 7,016 |
| 200 | 1..6 | 24.1 ms | 24.0 ms | 43.9 ms | 6.49 ms | 37,977 |
| 200 | 1..7 | 316 ms | 265 ms | 272 ms | 28.2 ms | 252,517 |
| 200 | 1..8 | 745 ms | 635 ms | 482 ms | 152 ms | 1,467,849 |
| 2000 | 1..6 | 75,134 ms | - | 96 ms | 94 ms | 1,167,094 |
| 2000 | 1..7 | 485,705 ms | - | 651 ms | 577 ms | 7,697,006 |

Every one-target figure past three hops is now the 2.1 ms target scan plus a pruned walk of
under 3 ms; the per-row index is built from four hops on, the set index from five. The 200-target set index is built at every
bound, and the six-hop cell went from 44 ms to 6.5 against the column form's 24. At 2000
targets the set is a third of the ball - 1.17M rows out of 3.5M trails at six hops - so there
is little to prune and the time is the emission. The 2000-target column form was not re-run.

**The rule.**

| the end is | shape |
|---|---|
| a label | `end_labels`, as today |
| a property equality | a scan of its own, labels folded in, `end_nodes` |
| a cross-product factor | `end_nodes` |
| bound by a hop or a join from the seed | `end_column` |
| a predicate over both ends | post-filter |

The pass decides all of that from the shape alone. The only decision that needs a number is
prune or test, and the executor takes it with the numbers in hand.

## State

`end_nodes` is implemented through the db and nl dialects, lowering, the translator, the
executor and `PathExplorator`; `test/query/ir/ExploreEndNodeSetTest.cpp` covers it. The set-mode
index, the per-batch pricing and `fuse_explore_end_set` landed on 2026-09-21. That pass turns
the post-filter codegen emits for a property-pinned end - `explore_paths`,
`get_node_properties` on `tgtids`, `eq` against a literal, `filter` - into a
`scan_nodes_by_property_value` of its own at the head of the function, the end's labels
folded into it, and an `end_nodes` operand on the walk. `test/query/ir/ExploreEndPropertyTest.cpp`
covers it, down to the Cypher query over simpledb. On reactome the query of the first table
now runs through Cypher in 2.8 ms at three hops, 4.4 at five and 5.8 at eight, against 1.12,
23.5 and 5,074 for the post-filter it replaces.

`fuse_explore_end_factor` landed the same day and closes the rule: an exploration whose
`end_column` is one factor of the cross product its seeds come from has that factor's ops
moved to the head of the function as the set, the product collapsed to its other factor or
rebuilt one column narrower, and the walk rebuilt with `end_nodes` and the end column gone
from its carry set. It peels the end out of a product nested inside the factor too, which is
what `MATCH (a), (b), (c) WITH a, b, c MATCH (a)-[e*]->(b)` compiles to; an end drawn from
the seed's own factor, or yielded beside a hop off it, stays per row.
`test/query/ir/ExploreEndFactorTest.cpp` covers it, down to the Cypher queries over simpledb.
