# Edge uniqueness in v3: fixed multi-hop patterns and paths

## Context

openCypher matches patterns under relationship isomorphism: within one `MATCH` clause no
relationship is bound twice, and nodes may repeat. The rule spans the whole clause, so it
binds a fixed hop against another fixed hop, a fixed hop against the edges of a
variable-length path, two paths against each other, and both sides of a comma-separated
pattern against each other. It does not reach across clauses: a second `MATCH`, an
`OPTIONAL MATCH`, a `UNION` branch or a subquery body is its own scope.

v3 enforces it in one place only: inside a single variable-length path, where
`PathExplorator` keeps trail semantics with a 64-bit signature and an exact scan on a hit
(`docs/vlp/PLAN.md`, `docs/vlp/PATH_RESEARCH.md` section 2.3). Every other pair matches
homomorphically. `MERGE` has its own check (`NLMergeExecutor::extendHop`) and is not part
of this plan.

Measured on simpledb (18 nodes, 18 edges, of which two reciprocal `KNOWS_WELL` pairs) through
`query_test_suite_cli`, against a Python enumeration of the same graph under both semantics:

| query | v3 today | isomorphic |
|---|---|---|
| `(a)-->(b)--(a)` | 22 | 4 |
| `(a)-[e1]->(b)-[e2]-(c)` | 44 | 26 |
| `(a)-[e1]->(b)<-[e2]-(c)` | 32 | 14 |
| `(a)-[e1]-(b)-[e2]-(c)` | 100 | 64 |
| `(a)-[e1]-(b)-[e2]-(c)-[e3]-(d)` | 278 | 106 |
| `(a)-[e1]->(b)-[e2]->(c)` | 12 | 12 |
| `(a)-[e1]->(b)-[e2]->(c)-[e3]->(d)` | 16 | 12 |
| `(a)-[e1:KNOWS_WELL]->(b)-[e2:INTERESTED_IN]->(c)` | 8 | 8 |
| `(a)-[e1]->(b), (c)-[e2]->(d)` | 324 | 306 |
| `(a)-[e1]->(b)-[e*1..2]->(c)` | 28 | 24 |
| `(a)-[e*1..2]->(b)-[f*1..2]->(c)` | 68 | 46 |
| `(a)-[e*1..3]->(b)` | 42 | 42 |
| `(a)-[e1]->(b)` then `MATCH (b)-[e2]-(c)` | 44 | 44 |
| `(a)-[e1]->(b)` then `OPTIONAL MATCH (b)-[e2]-(c)` | 44 | 44 |
| `(a)-[e1]->(b)-[e2]-(c) WHERE e1 <> e2` | 26 | 26 |

The three rows that agree by luck say where repeats come from. Two directed hops in a
row can only share an edge that is a self-loop; a directed chain of three can only share
its first and third edge around a 2-cycle, and simpledb has two of those. On reactome the
same gap is the 89,068 against 86,520 of `docs/path_bench.md` ("A v2 correctness bug") and
the 390,348 against memgraph's 198,362 of `shared_input`, where a reaction is paired with
itself through the same `hasEvent` edge twice.

Goal: the isomorphic count everywhere, with the check erased where the schema or the data
proves it cannot fail, and run inside the chunk writers where it cannot be erased, so a
repeated candidate is never materialized, gathered or filtered.

### What the incumbents do, and where this goes beyond

`RESEARCH.md` has the sources. Neo4j rewrites the rule into predicates before planning,
one `not r1 = r2` per pair and one `disjoint` or `not in` per pair involving a path, drops
a pair whose relationship type sets cannot overlap, and plans the rest as filters after the
expand that binds the second relationship; inside a variable-length expand the path holds a
persistent set. Memgraph plans an `EdgeUniquenessFilter` after every expand against every
edge bound before it, a linear scan per row, with no static elimination. Kùzu matches walks
by default, fixed patterns included, and applies `TRAIL` only when it unrolls its BFS parent
chains into paths. The relational formalisation of openCypher writes the rule as one
all-different selection appended to the clause, which is the filter-at-the-end baseline.

Beyond those three: (1) the pairs are proven away from labels, from the schema graph and
from the acyclicity of the data, where Neo4j reads only the types; (2) the surviving check
runs inside the chunk writer, before any row exists, where Neo4j and Memgraph filter rows
after the expand; (3) the shared-endpoint test skips a run of candidates whole when its source
node is not an endpoint of any earlier edge of the row, one node compare per input row
where every engine compares per candidate edge; (4) paths and hops share one mechanism, the exclusion
set per seed, so a path beside a hop or beside another path costs what the walk's own trail
check costs.

## What variable-length paths need

The explorator already rejects an edge held by its own walk. Four things are missing, in
the order the codegen meets them:

1. **A fixed hop before the path.** `(a)-[e1]->(b)-[e*1..2]->(c)`: the walk from `b` must
   never take `e1`. The seed row carries `e1`, so the exclusion is per seed: one or more
   edge columns aligned with `input_nodes`, the way `hop_imports` are aligned. The
   explorator folds their bits into the walk's base signature and keeps them in a small
   per-seed array that `positionOnPath` scans after `_pathEdges`. A candidate then costs
   what it costs today: a multiply, a shift, an AND, and an exact scan only on a hit.
2. **A path before a fixed hop.** `(a)-[e*1..2]->(b)-[f]->(c)`: the hop from `b` must
   reject any edge on the row's path. The row carries a `PathRef`; the writer walks the
   trie chain once per input row into a scratch span (at most `depth` entries) and rejects
   candidates against it. Storing the cumulative signature in each trie entry, which the
   explorator computes anyway (`_pathSignatures`), makes the common case one AND per
   candidate.
3. **Two paths.** `(a)-[e*1..2]->(b)-[f*1..2]->(c)`: the second exploration's seed row
   carries the first path; the exclusion set is that path's edges, read from the trie as in
   point 2 and installed as in point 1.
4. **The distinct search.** `hop_imports` today force a seed-by-seed walk and turn off the
   bit-parallel reachability search, because an imported value differs per seed. An
   exclusion set differs per seed too, but it is sparse: a batch of 64 seeds excludes at
   most `64 × depth` edges. A per-batch open-addressing map from excluded edge to the mask
   of seeds excluding it lets the search keep expanding a node for all 64 seeds at once
   and clear the excluded seeds' bits when it crosses one of those edges. The search stays
   exact for `min_hops <= 1`: a walk that avoids a set of forbidden edges shortens to a
   trail that avoids them.

Nothing else in the explorator changes: `ends_on_seed`, `end_column`, `end_labels`, the
pruning indexes and the hop predicate are unaffected, and a zero-length path has no edge to
check. The structural proofs of the next section apply to paths exactly as to hops: a typed
walk after a hop of another type needs no exclusion, and a directed walk of an acyclic type
can never come back to the fixed edge it left from.

## Where a repeat can come from, and when it provably cannot

Two edge variables bind the same stored edge only if their endpoints bind the same stored
nodes. For `(u)-[e]->(v)` the edge runs from `u` to `v`; for `(u)<-[e]-(v)` from `v` to `u`;
for `(u)-[e]-(v)` either way. So `e_i = e_j` forces a *coincidence* of pattern nodes: the two
sources are one node and the two targets are one node, in one of the orientations the
directions allow. Merging the coincident nodes turns the pattern into a smaller one with a
cycle in it, and the question "can `e_i = e_j` happen" becomes "can the data hold that
merged pattern". This is what each shape reduces to:

| shape | coincidence forced by `e_i = e_j` | what the data needs |
|---|---|---|
| `(a)-[e1]-(b)-[e2]-(c)` | `a = c` | any edge at all: the backtrack |
| `(a)-[e1]->(b)<-[e2]-(c)` | `a = c` | any edge at all |
| `(a)<-[e1]-(b)-[e2]->(c)` | `a = c` | any edge at all |
| `(a)-[e1]->(b)-[e2]->(c)` | `a = b = c` | a self-loop of the hop's type |
| `(a)-[e1]->(b)-[e2]->(c)-[e3]->(d)`, `e1 = e3` | `a = c`, `b = d` | a closed walk of two edges, typed `T1` then `T2` |
| a directed chain, `e_i = e_j` | `u_i = u_j`, `v_i = v_j` | a closed walk of `j - i` edges typed `T_i .. T_{j-1}` |
| `(a)-[e1]->(b), (c)-[e2]->(d)` | `a = c`, `b = d` | any edge matching both hops' types and labels |

Four facts, in increasing cost, each prove a pair distinct on their own. A pair is checked
at runtime only when none of them applies.

**P1, types.** Disjoint type sets cannot bind one edge. `-[:ASSIGNED_TO]->` beside
`-[:MEMBER_OF]->` never needs a check; an untyped hop is every type. This one needs no
data at all and settles most hops of a typed query.

**P2, labels.** A coincidence identifies two pattern nodes; if no label set in the graph's
`LabelSetMap` carries both nodes' label constraints, the coincidence is impossible.
`(t:Ticket)-[e1]->(u:User)-[e2]->(g:Group)`: `e1 = e2` needs `t = u = g`, and no node is
both a Ticket and a User.

**P3, the schema graph.** A summary of the graph with one arc per observed
`(source label set, edge type, target label set)` triple and, per type, the count of
self-loops. The merged pattern must embed into it homomorphically, hop by hop, or the data
cannot hold it; the embedding is a DFS of a pattern of at most a dozen hops over a graph of
tens of arcs, microseconds. A merged hop whose two ends are one node is a self-loop, and a
type with a self-loop count of zero cannot bind it. The summary is a superset of the data
under deletions, so it is sound across tombstones; each commit that adds edges extends it.

**P4, acyclicity of the data.** The schema graph has the arc `(Pathway, hasEvent, Pathway)`,
so it cannot rule out a `hasEvent` chain coming back on itself, but the data can: no pathway
contains itself (`docs/reactome_cycles.md`, `(p:Pathway)-[:hasEvent]->+(p)` is 0), so every
directed `hasEvent` chain is a simple path and its edges are distinct. Whether the edges of
a type set form a DAG is one topological sort over them, cached per commit and per type
set, the way `EdgeBranchingCache` caches fan-out. A merged pattern that contains a directed
cycle over an acyclic type set is impossible. `precedingEvent` keeps its check: 4,492
reactions lie on a cycle of it.

The four rules also say where node distinctness enters. When P4 holds, every node of a
directed chain is distinct, and distinct nodes force distinct edges: the implication is
free there. At runtime it takes a second form, per input row rather than per candidate. An
edge of hop `j` can only repeat `e_i` if the hop's source node is already the endpoint of
`e_i` that the candidate would share: the stored source of `e_i` when hop `j` reads out
edges, its stored target when it reads in edges, either when hop `j` or hop `i` is
undirected. So the writer runs a *shared-endpoint test* on each input row before it reads
the adjacency: the row's source node against one or two node ids per surviving pair. A row
whose source is new to the pattern writes its whole run with no edge compare at all. On a
directed chain that is every row that has not come back to a node it left, and on a chain
of an acyclic type it is every row, which P4 already knew. The test needs the earlier
hops' node columns in flight;
`(a)-[e1]->(b)-[e2]->(c)` carries them anyway, and a pattern that reads none of them pays
the per-candidate compare instead, which is the cheap case. A rolling XOR of identifiers
cannot certify distinctness: `a ^ b ^ c` cancels on sets with no repeat, and a repeat can
leave it non-zero. The sound rolling form is the explorator's, an OR of one hashed bit per
element, where a clear bit proves the element new and a set bit falls back to the exact
scan. For a fixed pattern the exact form is cheaper still: at hop `k` the earlier edges are
`k - 1` scalars held in registers for one row's run, and `k - 1 <= 6` compares beat a hash.
A per-row signature column pays only past that, which no fixed pattern reaches.

Uncommitted edges of an open change live in the write buffer, not in the parts, and can
close a cycle or add a self-loop the summary has not seen. When the view has pending
edges, P3 and P4 are off and only P1 and P2 apply; the check then runs.

## Where the check runs

Three layers, following the house rule that codegen emits the obvious form and passes
improve it.

**Codegen emits every pair.** After a hop's edge constraints and before its target
constraints, `DBProgramGenerator` emits one `db.check_edge_distinct` over the new edge (or
the new path) against every edge and path column of the same clause bound before it, and
filters every column in flight on the mask, exactly the shape `resolveEdgeIdentities` uses
for a shared edge variable:

    %s, %e2, %t2, %tg = db.get_out_edges(%b, {%e1})
    %ok = db.check_edge_distinct(%e2, {%e1})
    %e2f, %tgf, %e1f = db.filter(%ok, {%e2, %tg, %e1})

The subject is an edge column or a path column; the operands are edge or path columns; the
mask is true where the subject shares no edge with any operand. Two occurrences of one
named edge across comma-separated patterns are one variable joined on equality and are
left out of the pair set. The question codegen asks, "which edge columns of this clause
are bound at this insertion point", is about what it has already built, which is the only
kind it may ask. A cross product or a join of two edge-bearing patterns gets the same op
right after the product. A hop that later fuses into a root scan has no earlier edge and
gets nothing.

**A pass erases the provable pairs.** `prove_distinct_edges` runs first in the pipeline,
where every hop, type check and label check is still in its unfused form and the shapes of
the clause's hops (direction, types, labels at both ends) read straight off the IR. For
each `check_edge_distinct` and each of its operands it runs P1 to P4 over the merged
pattern and drops the operand when one of them holds; an op left with no operand goes
with its filter. It reads the schema summary and the acyclicity cache through the
`DBPassContext`'s view. `EXPLAIN` reports each pair as kept or as proven, with the rule
that proved it, so a query that pays the check says why.

**A pass folds the rest into the hop.** `fuse_distinct_edges` matches a hop whose only
reader is a `check_edge_distinct` filter over its own edge column and moves the operands
onto the hop as a `distinct_from {%e1, %p}` operand list, the way `fuse_edges_by_type`
moves the type onto it. Both run right after `push_down_filters`, before the type and
label fusions, so those matchers see a hop with nothing between it and its type or label
filter; `createByTypeHop` and the label variants carry `distinct_from` over. The nl
sibling of each hop op takes the same operands, and the chunk writers take them through
`setDistinctFrom` for edge columns and `setDistinctFromPaths` for path columns.

**The writer prunes.** In `fill`, a run of candidates belongs to one input row. The
writer first runs the shared-endpoint test: the row's source node against the endpoint
ids of each surviving pair, and a row that shares none writes its run as today. Otherwise
the excluded
edges of that row are loaded once per run, `k` scalars from the edge columns plus the
trie chain of each path column, and the run is written by a compacting copy that skips a
candidate equal to any of them, in place of today's `std::generate`; with the scalars in
registers the compare vectorizes across the run. The tombstone filter stays as it is,
after the fill. `NLPendingEdgeHop` gets the same treatment for uncommitted edges.
`PathExplorator` takes the exclusions per seed as described above.

**Should the writer always do it?** Once a pair survives the proof, yes. The writer sees
each candidate before the row exists: nothing is gathered for it, no carried column is
compacted, no mask column is written. A filter after the hop does the same `k` compares
per row and then pays a compaction of every column in flight, which on an unselective check
is a copy of the chunk. The two places where the post-filter stays are the cross product
and the hash join, whose output is the product of two chunks and whose pruning removes one
row in the size of a side; measure before folding it into the product iterator. And when
the proof erases the pair the writer does nothing, which is the point: a typed directed
chain on a ticketing schema, or a `hasEvent` hierarchy, runs exactly as it runs today.

## Storage

Two additions under `storage/metadata/`, both cached per commit next to
`EdgeBranchingCache` in `CommitData`, both computed lazily on first use and never written
to disk in this plan; a persisted form in the part's dump is a follow-up once the first-use
cost is measured.

`SchemaGraph`: the sorted arcs `(srcLabelSetID, edgeTypeID, dstLabelSetID)` with a count,
and the self-loop count per type, built by one pass over each part's `EdgeRecord`s with a
label set lookup per endpoint (`NodeContainer::getNodeLabelSet` on the owning part). On
reactome that is 11.5M edges, tens of milliseconds once per commit, then a lookup of a few
arcs per query.

`EdgeTypeAcyclicityCache`: for a type set, whether its edges over all parts form a DAG, by
Kahn's algorithm over the adjacency of those types; keyed like `EdgeBranchingCache`, so an
entry measured on fewer parts than the view holds is a miss. Cost is linear in the type's
edges and paid once per commit and type set; the first query on a large cyclic type pays
it and learns nothing, which the EXPLAIN report shows.

## Tests and oracles that change

These pin the homomorphic counts and must move to the isomorphic ones:

    test/query/ir/UndirectedCycleTest.cpp          (a)-->(b)--(a) 22 -> 4, (a)-->(b)<--(a) 18 -> 0,
                                                   (a)--(b)--(a) 44 -> 8, (a)-[:KNOWS_WELL]->(b)--(a) 6 -> 3
    test/query/ir/MultiHopUnionTest.cpp            undirected walks from Remy: 2 hops 15 -> 9, 3 hops 70 -> 21
    test/query/ir/NamedPathTest.cpp                runsThroughAFixedHopAndAWalk 10 rows -> 5
    test/query/ir/EquivalenceTest.cpp              (a)--(b)--(c), (a)-->(b)-->(c)-->(d)
    test/query/ir/ExploreEdgeTypeDisjunctionTest.cpp  one-MATCH against two-MATCH forms
    test/query-test-suite/tests/fuzz-undirected-cycle-merge.json   22 rows -> 4

The suite's 394 oracles were generated by v2, which matched homomorphically, so every
oracle with two or more edges in one clause is suspect. The procedure is the one that
produced the table above: enumerate the query on simpledb in Python under isomorphism and
compare, or run memgraph on the same graph as `docs/path_bench.md` did. A new
`EdgeUniquenessTest.cpp` on simpledb pins the fifteen counts of the table, the clause
scoping (a second `MATCH` and an `OPTIONAL MATCH` keep 44), and each proof rule on a shape
it decides. `test/storage/iterators/PathExplorationReference.cpp` gains the exclusion set so
the explorator's random sweeps cover points 1 to 4.

## Steps

1. **Correct first.** `db.check_edge_distinct`, its emission in codegen, its lowering,
   the executor's evaluation over edge and path columns (trie walk per row), and the
   explorator's per-seed exclusions with the batch mask for the distinct search. All
   fifteen counts right with a filter after each hop. Fix the pinned tests, regenerate the
   suite oracles. Measure the filter's cost on reactome: the three-hop `precedingEvent`
   chain (89,068 -> 86,520), the `shared_input` pair (390,348 -> 198,362), the untyped
   undirected two- and three-hop walks from every node.
2. **Prune in the writers.** `distinct_from` on the db and nl hop ops and on
   `explore_paths`, `fuse_distinct_edges`, the compacting fill in `GetOutEdgesChunkWriter`
   and its in, both, by-type and by-label siblings, and in `NLPendingEdgeHop`. Measure the
   same shapes: the undirected walks are where the gathers saved show. Then the
   shared-endpoint test, measured on the directed chains, where it should leave the
   per-candidate compare to the rows that closed a cycle.
3. **Prove with types and labels.** `prove_distinct_edges` with P1 and P2 and the EXPLAIN
   report. No storage change. Typed chains of distinct types lose the check entirely.
4. **Prove with the schema graph.** `SchemaGraph` in storage, P3 with self-loops. The
   directed two-hop chains of one type lose the check where the type has no self-loop.
5. **Prove with the data.** `EdgeTypeAcyclicityCache`, P4. `hasEvent` and `hasComponent`
   chains of any length lose the check; `precedingEvent` keeps it and says so.
6. **Later, if measured.** Persist the summary in the dump; prune inside the cross
   product; a signature column for chains past six hops; `shortestPath` beside a hop in
   one clause, which today is neither checked nor proven.

## Benchmarks

`samples/path_bench` measures the explorator; the fixed-hop shapes need their own table,
on reactome and on the generated graph, each run with the check, with the writer prune and
with the pair proven, so the three costs are separate numbers:

    typed directed chain, unprovable        (r:Reaction)-[:precedingEvent]->(b)-[:precedingEvent]->(c)-[:precedingEvent]->(d)
    typed directed chain, proven by P4      (p:TopLevelPathway)-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)
    typed chain, proven by P1               (p:Pathway)-[:hasEvent]->(r:Reaction)-[:precedingEvent]->(r2)
    untyped undirected, backtracks          (a)--(b)--(c) from every node, and three hops
    the V, one compare per candidate        (a)-->(b)<--(c) from every node
    comma patterns                          (p)-[:hasEvent]->(r), (p)-[:hasEvent]->(r2)
    fixed hop then path                     (r:Reaction)-[e:precedingEvent]->(b)-[:precedingEvent]->{1,4}(d)
    path then hop, two paths                the same shapes reversed and doubled

What to read off them: candidate checks per emitted row, rows gathered against rows
emitted, and the proof pass's own time, which the harness times on its own and which must
stay in the microseconds: the schema graph is tens of arcs and the pattern a dozen hops.
