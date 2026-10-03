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

`test/query/ir/PathEdgeUniquenessTest.cpp` pins the four on simpledb and fails today on
each: 28 rows for 24 with a hop before the path, 28 for 24 with a hop after it, 68 for 46
with two paths, and under `WITH DISTINCT a, b, c`, where `EXPLAIN` shows the exploration
marked `distinct`, 26 for 24 after a hop and 57 for 43 after a path; the trail rule inside
one path, 42, is its control.

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
compare, or run memgraph on the same graph as `docs/path_bench.md` did.
`test/query/ir/EdgeUniquenessTest.cpp` pins the fixed-pattern counts of the table and the
clause scoping (a second `MATCH` and an `OPTIONAL MATCH` keep 44): seven of its twelve
cases fail today, and the five that pass are the shapes no repeat can reach on simpledb
and the explicit `WHERE e1 <> e2`. `PathEdgeUniquenessTest.cpp` pins the four path cases.
Each proof rule gets its own case, on a shape it decides, with the pass that implements it.
`test/storage/iterators/PathExplorationReference.cpp` gains the exclusion set so the
explorator's random sweeps cover points 1 to 4.

## Status (2026-09-29)

Step 1 is implemented on the branch `edge-uniqueness-plan`. `db.check_edge_distinct` is
emitted after every hop and path of a clause against every edge or path bound before it,
and once more after the clause's `WHERE` for the pairs a cross product or a hash join
brought together, which keeps `fuse_hash_join` matching the product and its equality;
`fuse_distinct_edges` folds the check over a path into `explore_paths` as `distinct_from`,
so the walk and the distinct search skip the excluded edges themselves. `MATCH` clauses
are numbered in the dependency graph, so a second `MATCH` or an `OPTIONAL MATCH` is its own
scope. All 18 cases of `EdgeUniquenessTest` and `PathEdgeUniquenessTest` pass, and so do
the 490 tests of `test/query/ir`, the query suite and the storage iterators.

What moved with the semantics: 27 test files pinned homomorphic counts and now pin the
isomorphic ones, checked against a Python enumeration of simpledb; 14 suite oracles were
regenerated the same way; `CycleShapesTest`'s graph gained the 2-cycles b-d and a-d and a
second d->c edge, since the triangle and one 2-cycle hold no square or double cycle as a
trail; `CascadedMergeJoinTest`, `MultiPatternJoinTest` and `CommaPatternJoinKeysTest` now
match nothing on simpledb, which has no parallel edge, and want a fixture with one.
`FuzzHangTest` lost its count over eight islands: the product's 39,182,082,048 rows used to
count without a column, and the checks between its three edge islands, sitting after the
whole cascade, now read every row (12,697,896,960 survive, in 150 s). Checking each pair at
the innermost product that carries both sides would cut that to the 9,072 rows of the edge
islands, and needs the cascade to order those islands innermost; row order is what stops
that today. The codegen tests over two hops of different types (`FuseEdgesByTypeCodegenTest`,
`FuseEdgesByEndpointLabelCodegenTest`) expect the check's filter until step 3 proves it away.

Measured on reactome (2,978,202 nodes, 11,537,843 edges, rebuilt from the parquet dump
through `LOAD JSONL` since both dumps on the machine predate the current format), median of
three runs after a warmup through the shell, before and after this step. Each query is also
run split into one clause per hop, which the rule does not reach, as the control:

    query                                                        before       after      control
    (r:Reaction)-[:precedingEvent]->()-[:precedingEvent]->()-[:precedingEvent]->()
                                                   count   94,326 17.3 ms   91,706 21.4 ms   94,326 17.2 ms
    shared_input (two hasEvent hops from one pathway, two input hops to one entity)
                                                   count  390,348 164 ms   198,362 245 ms   390,348 206 ms
    (tlp:TopLevelPathway)-[:hasEvent]->(p:Pathway)-[:hasEvent]->(r:ReactionLikeEvent)
                                                   count    6,371 1.66 ms    6,371 1.66 ms    6,371 1.72 ms
    (p:TopLevelPathway)--(b)--(c)             count  125,690,888 66 ms   125,684,994 609 ms   125,690,888 65 ms
    (p:TopLevelPathway)--(b)--(c)--(d)      count  2,547,338,854 8.6 s   2,421,500,620 25.7 s   2,547,338,854 5.8 s
    (r:Reaction {stId: hub})<-[:precedingEvent]-(b)<-[:precedingEvent]-{1,3}(d:Reaction)
                                                   count        4 1.6 ms        4 1.3 ms        4 1.3 ms

198,362 is memgraph's count in `docs/path_bench.md`. The filter after the hop costs 24 %
on the typed chain and 49 % on `shared_input`, whose check reads four edge columns after
the hash join. The untyped undirected walks are the case step 2 exists for: the count used
to read no column at all, and the check now materializes both hops' edges over 125 million
rows and compacts them, 66 to 609 ms; in the writer the backtrack is one compare per
candidate against a register and no row is built. The typed two-hop chain, the shape
steps 3 to 5 prove away, costs nothing measurable at 6,371 rows.

The explorator's random reference sweep covers the exclusion set since 2026-10-03:
`PathExploratorExcludedEdgesTest` gives every input row its own excluded edges, with rows of
one node excluding different edges inside one batch of the distinct search.

Step 2 is implemented on the same branch. `distinct_from` names carried columns by index on
the seven hop ops and on `explore_paths`, in both dialects; `fuse_distinct_edges` folds the
check into whichever op bound the subject, so the plans of a chain, a V, a typed or
labelled hop, a walk after a hop and a hop after a walk carry no filter
(`FuseDistinctEdgesTest`), and the check stays a filter only over a cross product or a
join. Before a loop runs, its loop data lays the excluded edges of the input's rows out
flat, one span of edge IDs per row, resolving the edge columns and the paths of an earlier
walk through the trie there in the query layer; storage sees only that
(`ExcludedEdges`, two spans), and the seven chunk writers, the pending-edge hop and the
explorator read a row's span by index. A node's out-edges carry consecutive IDs within a
part, so on an out-run the excluded edges are located by arithmetic and no record is read;
an in-run is scanned. The translator gathers no copy of a carried column nothing reads
back, which the fold leaves behind. Same machine, same protocol, on a quieter day (the
controls moved by up to 20 % between runs):

    query                                                     step 1        step 2       control
    three precedingEvent hops from every Reaction     91,706  21.4 ms     91,706  14.7 ms   94,326  13.0 ms
    shared_input                                     198,362  245 ms     198,362  200 ms   390,348  179 ms
    (tlp)-[:hasEvent]->(p:Pathway)-[:hasEvent]->(r)    6,371  1.66 ms      6,371  1.49 ms    6,371  1.36 ms
    (p:TopLevelPathway)--(b)--(c)               125,684,994  609 ms  125,684,994  190 ms  125,690,888  66 ms
    (p:TopLevelPathway)--(b)--(c)--(d)        2,421,500,620  25.7 s  2,421,500,620  22.0 s  2,547,338,854  5.8 s
    hub, one hop then a {1,3} walk                        4  1.3 ms           4  1.3 ms         4  1.3 ms

The directed shapes are within 13 % of their controls. What the undirected walks still pay
is the scan of the in-runs: a node's in-edges are sorted by target only
(`EdgeContainer::create`), so finding the backtrack among them reads every record of the
run, 32 bytes each, where the control reads none. Sorting the in-edges by target then edge
ID at build time would make that a binary search of a few records; it changes what
`edges-in` holds on disk, so it needs a raise of `UP_TO_DATE_VERSION`, which is the decision
to take before it. It also supersedes the shared-endpoint test of this step: on an out-run
the arithmetic already costs one compare per excluded edge and no record, and on an in-run
the test could only spare the scan the sort removes.

Still owed from step 2: that in-edge order, and `NLPendingEdgeHop` walks its excluded edges
one by one, which no measurement has reached.

Step 3 is implemented on the same branch. `prove_distinct_edges` runs first in the
pipeline, on the shape codegen emits, and drops from each `db.check_edge_distinct` the
operands the types or the labels prove. A fixed hop's type set is read off the
`check_edge_type_constraint` filters over its etypes column and a walk's off its
`edge_types`; the labels of a hop's two ends are read off the `check_label_constraint`
filters over a node column born where the end was. Only the filters the rows reaching the
check passed through count, climbing from the check through filters, carry sets and the
factors of a cross product. P1 proves a pair whose type sets share no name. P2 takes the
stored source and target of each edge in every orientation its direction allows, and proves
the pair when no orientation lets both sources and both targets coincide, a coincidence
being possible when some label set of the graph holds both nodes' labels; a path has no
labelled ends, so a path is proven by types only. A check left with no operand goes with its
filter and one left with fewer is rebuilt, so the pairs kept fold into the hops as before.
P2 reads the view's `LabelSetMap`, which already holds the label sets a change's earlier
queries wrote, and is off in a query that writes the graph, whose own label sets are
registered after the pass runs. `check_edge_distinct` carries `names`, the query's name for
each column, and EXPLAIN reports each pair under a `pairs` stage, on by default:
`e2 <> e1: proven by types`, `proven by labels` or `kept`. `ProveDistinctEdgesTest` pins
15 cases on simpledb, each proven count against the split-clause form, and the codegen test
of a typed chain no longer expects a `distinct_from`. Same machine, same protocol, on the
same day as the step 2 table:

    query                                                        step 2        step 3       control
    (p:Pathway)-[:hasEvent]->(r:Reaction)-[:precedingEvent]->(r2)
                              proven by types    68,370  12.35 ms   68,370  11.76 ms   68,370  11.77 ms
    (tlp)-[:hasEvent]->(p:Pathway)-[:hasEvent]->(r:ReactionLikeEvent)
                              proven by labels    6,371  1.48 ms     6,371  1.45 ms     6,371  1.40 ms
    three precedingEvent hops from every Reaction
                              kept               91,706  14.3 ms    91,706  14.8 ms    94,326  12.9 ms
    shared_input              2 of 6 pairs kept 198,362  196 ms    198,362  187 ms    390,348  156 ms

The two proven chains run as their controls. `shared_input`'s check over the hash join
reads two pairs where it read four, the `input` hops being typed apart from the `hasEvent`
ones, and the pairs it keeps are the two of one type. The kept chain moved with its
control. The pass's own time is not measured yet.

Step 4 is implemented on the same branch. `SchemaGraph` under `storage/metadata/` holds
one arc per (source label set, edge type, target label set) the parts hold, with the
count of its edges and of those closing on their own node, built by one pass over each
part's out-records with a label set lookup per end; `CommitData` caches it beside
`EdgeBranchingCache`, `GraphView::schemaGraph()` refreshes it for a view holding more parts
than it was built on, and nothing writes it to disk. It answers `embeds`: whether a pattern
of labelled nodes and typed edges maps homomorphically onto the arcs, by a depth-first
search taking next the edge with the most ends placed and giving up, embedding, past
200,000 arcs visited. The pass reads the whole clause for it now: every fixed hop the rows
reaching the check came through, with the node columns two of them share, and the node
columns an equality of the flow holds equal, including the one the hop closing a cycle
lands beside, which codegen emits after that hop's check and the pass reads down the
filter chain as long as nothing else consumes the rows. P3 merges the pair's sources and
targets in each orientation the directions allow, turns the hops into one pattern with
the pair's edge first, and proves the pair when no orientation embeds; it is off with
pending writes, whose edges are in no part. `ProveDistinctEdgesTest` gains the two-hop
chains with no self-loop to share, the three-hop chains whose ends a two-cycle of the
schema does or does not close, the closed triangle, and the change that holds an
uncommitted self-loop; `SchemaGraphTest` pins the summary of simpledb, the embedding of a
hop, a self-loop, a two-cycle and an undirected edge, and the refresh to a commit adding
a self-loop. The fold test's typed and labelled shapes moved to a V, the chain they used
being proven now. Same machine, same protocol:

    query                                                        step 3        step 4       control
    (p:Pathway)-[:hasEvent]->(a)-[:hasEvent]->(b)
                              proven by schema                 -   121,323  13.7 ms   121,323  13.9 ms
    (p:TopLevelPathway)-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)
                              3 of 3 proven by schema          -    28,799  2.75 ms    28,799  2.73 ms
    three precedingEvent hops from every Reaction
                              2 of 3 proven by schema   91,706  14.7 ms    91,706  14.4 ms    94,326  13.3 ms
    shared_input              2 of 6 kept, as before   198,362  187 ms    198,362  193 ms    390,348  163 ms

The hasEvent chains run as their controls. Of the precedingEvent chain's three pairs the
schema proves the two adjacent ones, which only a self-loop could share and reactome has
none, and keeps the first against the third, since a reaction precedes one that precedes
it and the arc closes both ways; that one pair is what the query still pays for. The
summary of reactome is built once per commit, on the first query that asks for it: that
query runs in 71 ms against 13 ms for the next, so the build over 11.5M edges costs
58 ms, 5 ns per out-record. It got there from 135 ms in two steps, measured the same
way: the arcs are counted in a table dense over (source label set, edge type), each cell
holding the few target label sets it reaches, in place of a hash map keyed by the three,
which took 20 ms off; and an edge end's label set is read from an array by node ID,
filled range by range from the ranges each part keeps its nodes in per label set, in
place of a read into the node array, which took the other 57 ms off.

Step 5 is implemented on the same branch. `EdgeTypeAcyclicityCache` under
`storage/metadata/` answers whether the edges of a set of types, over every part and the
deleted ones included, form a DAG: one pass over the out-records collects the edges of the
types, and Kahn's algorithm over them, laid out by source node, takes every node exactly
when no closed walk holds an in-degree up. `CommitData` caches one answer per type set
beside `EdgeBranchingCache`, keyed by the node and edge counts of the parts it ran on, so
a view holding more parts sorts again, and `GraphView::isAcyclicOver` reads it. P4 takes
the merged pattern of each orientation, as P3 does, and searches it for a directed cycle
whose edges all carry types the parts hold no cycle of: a depth-first search from each
node over the nodes after it, an undirected edge left out since it runs either way, an
edge of any type widening the set to every type of the graph. The pair is proven when
every orientation closes such a cycle; the rule runs after P3, is off with pending writes
as P3 is, and reports `proven by acyclicity`. An orientation the summary rules out is not
searched, so the sort is paid only for the ones it leaves open: the undirected two-hop
walk merges into a self-loop in one orientation, which the summary refuses, and into one
edge in the other, which holds no cycle, so it sorts nothing. `ProveDistinctEdgesTest` gains a reporting
line of four engineers of one label set, whose chain the summary cannot tell apart, the
self-arc closing it, and the sort can: the three-hop chain, the four-hop chain, the chain
two types acyclic on their own close together, the untyped chain some type closes, the
change holding an uncommitted edge and the commit closing the line into a ring.
`EdgeTypeAcyclicityCacheTest` pins the sort of simpledb by type, the self-loop and the
sort again for a commit adding a cycle. Ten suite oracles the earlier steps had left at
their homomorphic counts moved to the isomorphic ones, checked against a Python
enumeration: eight comma patterns joined on one node are empty, `value-hash-join-where-2`
loses its 7 self-pairs and keeps 20 rows, `variable-length-paths-7` loses the 4 walks
through its fixed hop and keeps 3. Same machine, same protocol:

    query                                                        step 4        step 5       control
    (p:Pathway)-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)
                              1 of 3 kept -> proven by acyclicity
                                                       112,689  24.0 ms   112,689  22.8 ms   112,689  22.6 ms
    (p:Pathway)-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)-[:hasEvent]->(d)
                              3 of 6 kept -> 6 of 6 proven
                                                        85,898  35.0 ms    85,898  31.8 ms    85,898  31.6 ms
    (x:Complex)-[:hasComponent]->(a)-[:hasComponent]->(b)-[:hasComponent]->(c)
                              1 of 3 kept -> proven by acyclicity
                                                       149,061  28.1 ms   149,061  26.1 ms   149,061  26.4 ms
    three precedingEvent hops from every Reaction
                              1 of 3 kept, as before   91,706  13.7 ms    91,706  13.8 ms    94,326  12.9 ms
    shared_input              2 of 6 kept, as before  198,362  184 ms    198,362  185 ms    390,348  152 ms
    (p:TopLevelPathway)-->(a)-->(b)-->(c)
                              1 of 3 kept, every type sorted    -        194,209  3.1 ms    194,209  2.3 ms
    (p:TopLevelPathway)--(b)--(c)
                              kept, nothing sorted              -    125,684,994  188 ms  125,690,888  60 ms
    (p:TopLevelPathway)--(b)--(c)--(d)
                              3 of 3 kept, nothing sorted       -  2,421,500,620  22.2 s  2,547,338,854  6.5 s

The hasEvent and hasComponent chains run as their controls. The precedingEvent chain
keeps the pair between its first and third hops: the sort finds the cycles its 4,492
reactions lie on and the report says `kept`. The untyped undirected walks of steps 1 and 2
keep every pair, as the backtrack demands, and sort nothing; they run as they did after
step 2 (190 ms and 22.0 s then, against controls of 66 ms and 5.8 s). The sort of one type
is paid once per commit, on the first query that asks for it, measured as that query's
first run against its next once the summary was built: 87 against 23 ms for hasEvent, 106
against 26 for hasComponent and 91 against 14 for precedingEvent, so 64 to 80 ms each. The
directed untyped chain asks for every type at once, since its second-cycle orientation
embeds through precedingEvent: it pays 236 ms (239 against 3.1) and keeps its pair, the
graph being cyclic. Each sort is a pass over the 11.5M out-records and three arrays over
the 3M nodes; sorting only the nodes the types touch would cut the arrays, and a persisted
answer the pass.

Still owed from step 5: P4 beside a walk, which the pass proves by types only, as P3 does.
A directed walk of an acyclic type leaving the fixed hop's target can never take that
hop's edge, and the rule needs the walk's direction and start beside the hop's ends,
which `PatternEdge` does not carry for a path.

The undirected hop writer stopped scanning its runs on 2026-09-30. `GetEdgesChunkWriter`
classifies a row's excluded edges once, on the node's first run. An edge is in the node's
out-run when its offset in the part's out-edges falls in the node's out-range. It is in the
node's in-run when its out-record's target is the node, or, if it is in the out-run, when
it is a self-loop, which `EdgeContainer` lists per part. A hop that writes only indices
drops that many rows from the run. A hop that writes columns copies the run in plain
segments around the excluded edges, found by arithmetic on an out-run and by a scan on an
in-run. The six other hop writers still call `ExcludedEdges::pruneRun`. Measured before and
after, same machine, two fresh shells each, minimum over the shells; the control is the
split-clause form:

    query                                                       rows      before      after    control
    three precedingEvent hops from every Reaction              91,706    13.9 ms    13.5 ms    12.8 ms
    shared_input                                              198,362     182 ms     182 ms     153 ms
    (tlp)-[:hasEvent]->(p:Pathway)-[:hasEvent]->(r:ReactionLikeEvent)
                                                                6,371    1.39 ms    1.37 ms    1.37 ms
    hub, one hop then a {1,3} walk                                  4    1.31 ms    1.27 ms    1.26 ms
    (p:Pathway)-[:hasEvent]->(r:Reaction)-[:precedingEvent]->(r2)
                                                               68,370    11.6 ms    11.7 ms    11.5 ms
    (p:Pathway)-[:hasEvent]->(a)-[:hasEvent]->(b)             121,323    13.1 ms    13.2 ms    13.0 ms
    (p:TopLevelPathway)-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)
                                                               28,799    2.71 ms    2.67 ms    2.68 ms
    (p:Pathway)-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)
                                                              112,689    22.6 ms    22.5 ms    22.5 ms
    (p:Pathway)-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)-[:hasEvent]->(d)
                                                               85,898    31.2 ms    31.2 ms    31.2 ms
    (x:Complex)-[:hasComponent]->(a)-[:hasComponent]->(b)-[:hasComponent]->(c)
                                                              149,061    26.0 ms    26.3 ms    26.0 ms
    (p:TopLevelPathway)-->(a)-->(b)-->(c)                     194,209    3.11 ms    3.05 ms    2.18 ms
    (p:TopLevelPathway)--(b)--(c)             count(*)    125,684,994     185 ms    64.0 ms    57.3 ms
    (p:TopLevelPathway)--(b)--(c)             count(c)    125,684,994     273 ms     169 ms     151 ms
    (p:TopLevelPathway)--(b)--(c)--(d)        count(*)  2,421,500,620    21.97 s     6.93 s     5.87 s
    (p:TopLevelPathway)--(b)--(c)--(d)        count(d)  2,421,500,620    24.96 s    18.57 s    15.18 s

Only the undirected walks moved. Writing indices only, the two-hop walk runs as its
control; writing a column, both walks still scan the in-run that holds the excluded edge.
The directed untyped chain pays 0.9 ms in `GetOutEdgesChunkWriter`, which still scans, and
`shared_input` pays its filter after the hash join. Timings of one query vary by up to 2x
between shell processes and stay within 1 ms inside one, so each query and its control
were run alternately in the same shells.

## Owed (2026-09-30)

Steps 1 to 5 are done. What the status above leaves open, by where it shows:

1. **The in-edge order.** The undirected writer finds an excluded edge in an in-run by a
   scan, and the six other hop writers still scan every in-run. Sort a node's in-edges by
   target then edge ID at build time, so both are a binary search. The undirected walks
   that write a column pay the scan: 169 ms against a 151 ms control for two hops, 18.6 s
   against 15.2 s for three. It changes what `edges-in` holds on disk and needs a raise of
   `UP_TO_DATE_VERSION`, which is the decision to take first. Before it, the six writers
   can take the undirected writer's out-range and self-loop classification. (step 2)
2. **Pruning inside the cross product and the hash join.** The check stays a filter after
   the product; `shared_input` pays 182 ms against 153 ms for it. `FuzzHangTest` reads
   12,697,896,960 rows in 150 s for the same reason, and checking each pair at the innermost
   product that carries both sides would cut that to 9,072 rows once the cascade orders the
   edge islands innermost. (steps 1 and 6)
3. **The benchmark table.** Measured on reactome against Memgraph and Neo4j on
   2026-10-01 (Benchmarks). Still missing: the generated graph, the check, the writer
   prune and the proof as three separate costs, and the pass's own time, which the plan
   wants in the microseconds. (step 3 and Benchmarks)
4. **The suite audit.** Every oracle with two or more edges in one clause is suspect, v2
   having generated them homomorphically. Step 1 moved 14 and step 5 the 10 that were
   failing; the fixtures that agree by luck have not been swept. `CascadedMergeJoinTest`
   and `MultiPatternJoinTest` match nothing on simpledb, which has no parallel edge;
   `EdgeUniquenessWritesTest` adds one and pins their shapes: three patterns on one pair
   match 6 rows over three parallel edges, and `(b)-->(c), (a)-->(b)-->(c)` 4 rows over
   two. The four-pattern query of `CommaPatternJoinKeysTest` still matches nothing and has
   no parallel-edge case. (step 1)
5. **P4 beside a walk.** A directed walk of an acyclic type leaving the fixed hop's target
   can never take that hop's edge. `PatternEdge` carries no direction or start for a path,
   so a hop beside a walk is proven by types only. (step 5)
6. **`NLPendingEdgeHop`** walks its excluded edges one by one, which no measurement has
   reached. (step 2)
7. **Persisting the answers.** The summary and the acyclicity of each type set are rebuilt
   on the first query of every commit, 58 ms for the summary and 64 to 80 ms per type set
   on reactome, 236 ms for every type at once. A persisted form in the part's dump removes
   both; before that, the sort's three arrays over the 3M nodes could shrink to the nodes
   the types touch. (steps 4, 5 and 6)
8. **Later still.** A signature column for chains past six hops, and `shortestPath` beside
   a hop in one clause, which is neither checked nor proven. (step 6)

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

Measured on 2026-10-01 at c7189c46c on reactome, on a 48-core machine with 251 GB, against
Memgraph (`memgraph/memgraph:latest`, image of 2026-07-13, WAL and snapshots off) and Neo4j
5.26.30 Community (slotted runtime, 32 GB heap). Each query is one `MATCH` clause returning
`count(*)`. Its control is the same pattern split into one `MATCH` per hop, which the rule does
not reach. Times are wall time at the client: HTTP for turingdb, Bolt for the other two.
turingdb ran in three fresh server processes, each query alternating with its control after
one warmup, 5 or 10 timed pairs per process. The table gives the median of the three
per-process medians, and `check` the median over the processes of query over control. Memgraph
and Neo4j ran one warmup and 5 timed runs; a query whose first run took over 20 s ran once. The
first column is EXPLAIN's `pairs` stage:

    query                                      rows   turingdb   control  check   memgraph      neo4j
    (r:Reaction)-[:precedingEvent]->(b)-[:precedingEvent]->(c)-[:precedingEvent]->(d)
    2 of 3 proven by schema                  91,706    13.1 ms   12.5 ms    +5%    76.7 ms     219 ms
    (p:Pathway)-[:hasEvent]->(r:Reaction)-[:precedingEvent]->(r2)
    proven by types                          68,370    15.9 ms   15.9 ms    -0%    64.7 ms     151 ms
    (t:TopLevelPathway)-[:hasEvent]->(p:Pathway)-[:hasEvent]->(r:ReactionLikeEvent)
    proven by labels                          6,371    1.31 ms   1.81 ms    -1%    3.51 ms    7.31 ms
    (p:Pathway)-[:hasEvent]->(a)-[:hasEvent]->(b)
    proven by schema                        121,323    18.9 ms   19.3 ms    -1%    55.7 ms     175 ms
    (p:TopLevelPathway)-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)
    3 of 3 proven by schema                  28,799    3.78 ms   3.69 ms    +2%    4.81 ms    20.5 ms
    (p:Pathway)-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)
    3 of 3 proven, schema and P4            112,689    22.6 ms   22.3 ms    +1%    94.5 ms     294 ms
    (p:Pathway)-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)-[:hasEvent]->(d)
    6 of 6 proven, schema and P4             85,898    30.1 ms   30.1 ms    +1%     126 ms     411 ms
    (x:Complex)-[:hasComponent]->(a)-[:hasComponent]->(b)-[:hasComponent]->(c)
    3 of 3 proven, schema and P4            149,061    27.8 ms   28.2 ms    -1%     107 ms     477 ms

    (p:TopLevelPathway)-->(a)-->(b)-->(c)
    2 of 3 proven by schema                 194,209    2.31 ms   2.55 ms   -14%    15.0 ms    45.8 ms
    (p:TopLevelPathway)-->(b)<--(c)
    kept                                105,791,834    66.1 ms   78.4 ms   -11%     5.06 s     7.99 s
    (p:Pathway)--(b)--(p)
    kept                                     11,728     527 ms    480 ms   +10%    75.7 ms     241 ms
    (p:TopLevelPathway)--(b)--(c)
    kept                                125,684,994    76.1 ms   76.8 ms    -1%     6.40 s     9.50 s
    (p:TopLevelPathway)--(b)--(c)    RETURN count(c)
    kept                                125,684,994     271 ms    220 ms   +23%     7.17 s     9.55 s
    (p:TopLevelPathway)--(b)--(c)--(d)
    3 of 3 kept                       2,421,500,620     8.92 s    7.52 s   +19%    169.7 s    340.0 s

    (p:TopLevelPathway)-[:hasEvent]->(r), (p)-[:hasEvent]->(r2)
    kept                                     14,678    1.58 ms   1.60 ms   +11%    4.09 ms    4.51 ms
    (a:TopLevelPathway)-[:hasEvent]->(b), (c:TopLevelPathway)-[:hasEvent]->(d)
    kept                                  4,262,160    4.98 ms   3.79 ms   +31%     520 ms     661 ms
    shared_input
    4 of 6 proven by types                  198,362     143 ms    125 ms   +15%     683 ms    169.4 s

    (r:Reaction {stId: "R-HSA-2993780"})<-[:precedingEvent]-(b)<-[:precedingEvent*1..3]-(d:Reaction)
    kept                                          4    1.71 ms   2.05 ms    +0%    0.57 ms    4.06 ms
    (r:Reaction)-[:precedingEvent]->(b)-[:precedingEvent*1..3]->(d)
    kept                                    297,318    28.7 ms   27.5 ms    +4%     140 ms     353 ms
    (r:Reaction)-[:precedingEvent*1..3]->(b)-[:precedingEvent]->(d)
    kept                                    297,318    41.1 ms   33.6 ms   +22%     170 ms     442 ms
    (r:Reaction)-[:precedingEvent*1..2]->(b)-[:precedingEvent*1..2]->(d)
    kept                                    389,024    46.1 ms   40.5 ms   +14%     188 ms     468 ms
    (p:TopLevelPathway)-[:hasEvent]->(a)-[:hasEvent*1..3]->(b)
    kept                                     80,441    7.68 ms   8.69 ms    +3%    32.1 ms    70.8 ms

All three engines return the same count on all 22 queries, and on 12 of them the control
returns more. Where every pair is proven, the query runs within 2 % of its control. Where a
pair is kept, it costs 0 to 31 %: 31 % for the cross product, 19 and 23 % for the undirected
walks that read a column, 22 % for a walk then a hop. The checks of the queries under 5 ms are
noise: those switch between about 2.1 and 3.5 ms from one run to the next. The machine also
drifted over the hour of the run: `(p:Pathway)-[:hasEvent]->(r:Reaction)-[:precedingEvent]->(r2)`
ran 22, 10 and 16 ms in the three processes, and its control moved with it.

turingdb is faster than Memgraph on 20 of the 22 queries, by 1.3x to 104x, and faster than
Neo4j on 21, by 2.4x to 1,185x. Memgraph is faster on two. `(p:Pathway)--(b)--(p)` runs in
527 ms against 76 ms; its control is as slow, 480 ms, and its plan starts from
`db.scan_nodes()` over all 2,978,202 nodes. The hub hop then walk returns 4 rows in 1.71 ms
against 0.57 ms. Neo4j's 169 s on `shared_input` is its plan: from each input entity it
expands back to every reaction that uses it, a step the planner estimates at 87,684 rows and
that hub inputs such as ATP make far larger.

The first query of a commit builds what the proofs read. In a fresh server, the first run of
the three-hop `precedingEvent` chain, the first query after the load, took 342 ms against
13.1 ms warm. The first runs that sort `hasEvent` and `hasComponent` took 171 and 130 ms, and
the untyped directed chain, which sorts every type, took 313 ms against 2.31 ms.

Still not measured: the generated graph, the check, the writer prune and the proof as three
separate costs, which needs builds with the prune and the pass turned off, and the pass's own
time.
