# Variable-length paths in v3: `PathExplorator`

## Context

Cypher variable-length patterns (`MATCH (n)-[e]->+(m)`, `-[e]->*`, `-[e]->{2,4}`, `<-[e]-+`,
`-[e]-*`) already parse into `QuantifiedPath` (min/max) on `EdgePattern`, and v2 executes
them with `PathExplorerProcessor` (a level-synchronous BFS over a parent-pointer tree with an
O(depth) trail check per candidate edge). v3 (the MLIR engine under `query/ir/`) ignores the
quantifier: `VariableDependencyGraph::registerPatternElement` emits one plain hop, so
`MATCH (n)-[e]->{2,4}(m)` silently compiles to a single hop and returns wrong rows. There is
no path op in either dialect and no path runtime.

Goal: a storage-level path enumerator, `PathExplorator`, built as a chunk writer with the
same contract as `GetOutEdgesChunkWriter`, plus the db/nl op, lowering, translation,
executor handler and codegen so v3 runs variable-length paths end to end, reproducing v2's
semantics (the `test/query-test-suite/tests/variable-length-paths-*.json` oracle). The
engine is designed to beat the incumbents (Neo4j `VarLengthExpand`, Memgraph
`ExpandVariable`, Kùzu's recursive join) on both constants and asymptotics, in tiers:

| Tier | What | Why it wins |
|---|---|---|
| 1 (this change) | Stack-based depth-first trail enumeration over immutable `DataPart` spans; per-depth candidate frames; 64-bit signature trail check; lazy path materialization; chunked emission into a factorized path column (per-query prefix trie, one 8-byte handle per row); hop predicates evaluated inside the walk, vectorized per frame; per-frame candidate prefetch within a walk | Bounded memory, O(1) expected trail check, O(1) emission and carry per row, vectorizable candidate generation, failing hops cut at the frame, DRAM latency hidden within a walk and across seeds |
| 2 | End-constraint fusion + query-time reverse-distance pruning index (PathEnum's principle) gated by a runtime cost model | Prunes every prefix that cannot end on a valid node: intermediate cardinality, not traversal speed, dominates constrained queries (ReCAP: up to 400,000×; PathEnum: ~100× fewer edges touched) |
| 3 | Per-row bound targets with an MS-BFS distance index; join-based bidirectional enumeration cut at hubs where both searches suspend (GraphS); DISTINCT mode via bit-parallel MS-BFS | s-t path queries and DISTINCT reachability stop being enumeration problems; hubs, which carry 93–99 % of long paths, are never expanded blindly |
| 4 | Storage: type-sorted adjacency runs, SoA edge arrays, patched-node bitmap; optional per-commit oracles (2-hop distance labels, landmark reachability with doomed sets, interval labels, hub segment cache) | Typed traversals touch only matching edges; half the bytes per candidate; no per-row list copies; pruning without a per-query BFS on large append-mostly graphs |

## Status (2026-09-13)

Rebased onto `535a145e8`, a commit per tier, one recalibrating the gates and one removing the
distinct gate:

| Commit | Tier | What |
|---|---|---|
| `c0488138e` | 1 | Drops main's stopgap rejection of variable-length paths and its test; `PathExplorator`, `PathTrie` and the `PathRef` column, `PathHopFilter`, `db/nl.explore_paths` with the `hop` region, `db.expand_path`, `db.path_length`, lowering, translator, executor, codegen, analyzer hop scope, grammar for inline `WHERE` and the parenthesized quantified pattern |
| `68d015254` | 2 | `end_labels` and `fuse_explore_end_constraint`, `PartDirectory`, `PathDistanceIndex` (reverse-direction BFS from the labelled nodes) behind a cost gate, since recalibrated - see the commit below for the estimate and the constant it is compared against |
| `f36461521` | 3 | `end_column` and `fuse_explore_end_nodes`, `PathTargetIndex` (multi-source BFS, 64 targets per word, per-chunk, gate charged per batch), `distinct` and `fuse_explore_distinct_ends` with the bit-parallel reachability mode of the explorator |
| `f475c48bf` | 2-3 | The estimate the three gates share samples the fan-out of the edge type the walk follows and sums the candidates over `min(max, farthest)` levels with the frontier clamped at the graph, so an unbounded walk no longer estimates as infinite; `indexUnitCostInChecks` re-fitted from 0.5 to 0.35; `PathExploratorCyclicTest` |
| `9e9704046` | 3 | Drops `searchPaysForDistinctEnds`, so a `distinct` exploration always searches; `PathReachTable`, an open-addressing table keyed by the nodes a batch reaches, replaces the dense `ReachWords` array, so a batch of the search costs its ball; `PathReachTableTest` |
| uncommitted | 1 | `PathTrie` holds one entry arena per walk, named in the handle's top bits; the explorator truncates an arena on backtrack above the chunk's last emitted row and cuts it back to the walk's current path at the next fill, so the trie holds one chunk and the live prefixes instead of the whole search tree; `PathExploratorReclaimTest` |
| `8ff96f5d3` | 1 | Removes the interleaved walkers and the `Stage` machine that yielded between them: `PathExplorator` holds one walk, a descent reads the adjacency and pushes its frame in one step, and `setWalkerCount` goes with them. 2.4-2.5x on reactome's walks, 12.6 ns per emitted path against 37.8 |
| `3da8feb88` | 3 | Extends `distinct` past a minimum of one hop with a pruned walk: a frame's `_taint` records the shallowest held edge its subtree could not take, and a subtree no edge above it constrained is remembered as `(node, remaining depth)` so later arrivals stop there. The `min_hops <= 1` verifiers and the pass gate go. 195x at ten hops on reactome |

The eight `variable-length-paths-*.json` oracles run through the MLIR engine, and the two v2
oracles that expected the old "not yet supported" plan error now expect the analyzer's
rejection of `e.name` on a quantified edge.

The benchmark harness is `samples/path_bench` (excluded from CI): it generates an out-of-cache
graph in one commit (`-nodes`, `-degree`, a seed label S every `-seed-stride` nodes, an end
label T every `-end-stride`, one edge in four of type B) and times, at the storage level, the
candidate lookahead, the end-label filter against
`PathDistanceIndex`, a bound end against `PathTargetIndex`, the distinct mode against the
enumeration, and, through `QueryInterpreterV3`, the same shapes as Cypher; `-section` runs
one table. First measurements on the default shape (2M nodes, degree 8, 1000 seeds, hops
1 to 3, 584,000 rows):

- **Walkers and lookahead** (superseded, see below): on the generated graph 1 walker with no
  lookahead 33.4 ms; lookahead 1 alone 25.0 ms; 8 walkers 15.1 ms; 16 walkers 14.8 ms,
  lookahead indifferent past 4 walkers; in cache (20K nodes) every cell within noise. Three
  more shapes agreed - degree 3 at six hops 105 / 38 / 35 / 35 ms for 1 / 8 / 16 / 32
  walkers - so the default was set to 16 unconditionally.
- **Walkers removed (2026-09-19).** That sweep only ever ran on the generated graph, where
  every hop is a dependent miss by construction. On reactome one walker wins everywhere:
  hot seed at 6 hops 42.6 ms against 106.4 at sixteen, at 8 hops 1.65 s against 4.37, at 9
  hops 10.1 s against 27.0; 83K seeds at 2 hops 68.9 against 91.0; 110K complexes at 3 hops
  204 against 298. Only a two-hop expansion from all 2.98M nodes is indifferent (1005 ms at
  four walkers against 1041 at one), which is the cold wide frontier the interleaving was
  built for. The callgrind profile said why: of 2.30 billion instructions for the 6-hop walk,
  1.20 billion were the round robin picking the next walker, `advance` entered 5,114,862
  times against 524,471 real expansions. `PathExplorator` now holds one walk, the `Stage`
  machine that existed to yield between walkers is gone, and a descent reads the adjacency
  and pushes its frame in one step: 12.6 ns per emitted path against 37.8, flat from 6 hops
  to 9. The lookahead stays at 1 and keeps its sweep (`-section lookahead`).
- **End labels**: the filter costs 36.6 ms, the index 245 ms to build (it reaches the whole
  graph) then 4.4 ms to run, so the break-even is about 7,600 seeds where the gate formula
  with its multiple of 8 needed 281,000. Calibrated on three shapes at 1,000 to 100,000
  seeds (degree 8 at three hops, 3 at six, 32 at two), one index unit - a node or edge the
  search touches - costs 0.26, 0.40 and 0.34 of a candidate check once the enumeration
  estimate sums the candidates of every hop instead of capping the depth at four (the cap
  made the six-hop estimate nine times too small). **Applied**: the estimate sums the
  candidates of every hop and the label index is built when that exceeds a fraction of
  `V + E`.
- **End labels, recalibrated on reactome**: the estimate divided every edge by every node,
  so it modelled a typed walk with the whole graph's fan-out, and an absent max made
  `pow(fanOut, uint64 max)` infinite - together they built a whole-graph index for a single
  seed on every unbounded typed query. On reactome `(p:TopLevelPathway {stId})-[:hasEvent]->+(ev:Event)`
  paid 5.0 ms against 1.2 ms for the same walk bounded at its own depth. Neither defect
  explains it alone: with the graph's fan-out of 3.9 a finite estimate still saturates at
  2.8e9 per seed, and with the type's fan-out of 0.04 the floor of one still multiplies by an
  infinite depth. **Applied**: the fan-out is sampled over a strided sample of at most 4,096
  nodes counting only the walked type's edges (no per-type edge count is kept, and counting
  them exactly costs about what the index does), and the sum runs over `min(max, farthest)`
  levels with the frontier clamped at the node count. The estimate is then exact on the
  bench's uniform shapes - it predicts 584, 1,092 and 1,056 candidates a seed against
  584,000/1,000, 1,091,997/1,000 and 1,056,000/1,000 measured - so the gate's only remaining
  error is the constant.
- **The index unit cost**: re-measured at 0.222, 0.346 and 0.254 of a candidate check on the
  same three shapes, which no longer supports 0.5. Choosing by worst-case regret - what the
  gate's own switch point costs against the better plan there - gives 1.50× at 0.25, 1.27× at
  0.30, 1.32× at 0.35 and 1.76× at 0.5. **Applied**: `indexUnitCostInChecks` is 0.35, the
  highest ratio measured and within 0.1 of the regret optimum, which moves the switch points
  from 15,411 / 3,663 / 7,812 seeds to 10,788 / 2,564 / 5,469 against measured break-evens of
  7,768 / 2,867 / 5,022. The residual is irreducible with one scalar: the ratio itself spans
  1.6× across the shapes, so the degree-8 three-hop shape still walks at 10,000 seeds where
  building wins by 1.32× (409 ms against 264 + 46). Closing that would mean charging the walk
  what it has already spent rather than what it is predicted to spend.
- **The seed predicate reaching the walk** (fixed on main, PR #871): `PushDownFilters` stopped
  climbing a column's lineage at the first filter, so a seed predicate sitting behind one -
  behind an end-label filter, or behind the type filter of a following hop - was reported
  already pushed and stayed above the scan. The exploration then ran from every node of the
  seed's label: `(r:Reaction {stId})<-[e:precedingEvent]-{1,4}(down:Reaction)` walked all
  83,459 Reactions for 33.8 ms against 0.7 ms for the one seed, and the trailing-hop shape
  paid 709 ms. The climb now passes through a filter to the column it carried, and pushes only
  when it crossed an op that builds rows, which keeps a predicate on a hop's own target where
  the type fusion can still absorb the constraint filter beside it. Every reactome measurement
  here was taken with that fix applied.
- **Bound ends**: the target index costs 27 ms per batch of 64 targets on 2M nodes at three
  hops (50 ms at six, 4 ms on 500K nodes at two) against about 1 ms of enumeration a batch
  spares: its cost was not the search but the dense word per node per level, about 3 ns a
  word, and a 10,000-seed run laid out 10 GB of them. **Applied**: each batch is now an
  open-addressing table of the nodes it reaches (a key, the word of the targets that reached
  the node, and a hop count per target: 81 bytes a slot, half full), so a batch costs its
  ball. Same runs after the change: 162 → 55 ms at 1,000 seeds, 3,920 → 848 ms at 10,000,
  7,762 → 2,904 ms at degree 3 and six hops. The shallow high-degree shape went the other
  way (638 → 1,067 ms on 500K nodes at two hops): three small dense levels beat table
  probes there, so the batch keeps both layouts and `planBatch` picks the cheaper by the
  gate's own costs - 5 checks per node a batch is expected to reach
  (`min(V, 64 × candidates per target)`) for the table, a tenth of a check per word for
  `(levels + 1) × V` dense words - which puts that shape back at 790 ms and leaves the
  others sparse. The gate charges the chosen layout's cost per batch and refuses above 1 GiB
  of slots or words; the index pays off when many seeds share few targets.
- **Distinct mode**: 39.9 ms against 17.9 ms for the enumeration at equal edge work. On a
  random graph the balls of 64 seeds barely overlap (583,905 distinct pairs of 584,000
  rows), so the search shares nothing. **Applied**, on a quiet machine: a node's three
  words in one struct (39.9 → 32.6 ms: one miss per candidate instead of three), the
  words dated by the batch that wrote them so nothing is cleared between batches
  (neutral, but it drops a pass and a list), and the frontier's adjacency fetched sixteen
  nodes ahead of the one being expanded (32.6 → 24.5 ms), the search having nothing else to
  hide those misses behind. Prefetching the candidates' words in the collecting
  pass changed nothing and was dropped. Result: 1.5× the walk on the no-overlap graph, and
  65 ms against 70 ms for the walk on the deeper degree-3 six-hop shape. **Applied, then
  removed, the overlap gate** (`searchPaysForDistinctEnds`): the pass keeps marking the op
  `distinct` (it only proves duplicates are harmless downstream), and the executor asked the
  gate whether to search or walk, pricing the nodes a batch's 64 seeds were expected to
  reach, along their `fanOut` edges at 1.5 checks each, against the `64 × c(max)` checks of
  the walk. Its verdicts matched the bench - walk on the default shape (16.9 against 25.7
  ms) and on the deeper degree-3 shape; walk on 50K nodes at four hops from 10,000 seeds (491
  against 524 ms); search on the same graph at five hops, where the balls saturate it (1,516
  against 3,983 ms) - and failed on reactome (below). Since `9e9704046` the mark alone
  decides: the search expands each (seed, node) pair at most once where the walk expands it
  at least once, so no estimate can save more than the search's constant while the walk can
  lose without bound. The Cypher `RETURN DISTINCT n, m` takes 307 ms, most of it in
  `db.remove_duplicates` over the pairs, which is the next lever there.
- **Reactome, what v2 will not run** (2,978,202 nodes, 11,537,331 edges, 415
  `TopLevelPathway`, 117,945 `Event`, 83,459 `Reaction`, 110,048 `Complex`, from the binary
  dump): v2's analyzer rejects an edge type filter on a quantified path, and every Reactome
  hierarchy is typed, so no query a biologist writes reaches its explorer - `hasEvent`,
  `precedingEvent` and `hasComponent` are all refused, as are the edge property filter, the
  hop predicate, `size(path)` and `DISTINCT`, and an untyped bound end fails at execution
  with "Left input column is not trivially copyable". All of them run on v3, median of three
  after a warmup, one shell per query: every Event under Signal Transduction
  (`-[:hasEvent]->+`, 3,154 rows) 1.51 ms, 1.12 ms bounded at three hops (637 rows); every
  Reaction under all 415 top-level pathways (92,952 rows) 29.78 ms; every Event under them
  (129,772 rows) 17.45 ms; the 29 human ones (20,654 rows) 10.33 ms, and 13.24 ms as
  `RETURN DISTINCT` (19,425 pairs); one reaction's ancestor pathways backward 1.52 ms; which
  top-level pathway reaches it with `m` bound 1.52 ms; `RETURN size(e)` over its 3,154 events
  2.42 ms; the downstream cascade `<-[:precedingEvent]-{1,4}` (96 rows) 1.18 ms; the deepest
  human complex's subunit tree (23 rows) 0.74 ms, every human complex's 12.99 ms; Signal
  Transduction with the diseased hops cut inside the walk (1,801 rows) 0.58 ms. `EXPLAIN`
  puts the type and the end label inside the op, and answers the bound end with one backward
  walk from the single reaction rather than 415 forward ones.
- **Reactome, the untyped shapes both engines run**: identical results everywhere - row
  counts, the aggregate values up to 125,690,888, and the rows themselves on the oracle below
  - and v3 between 1.8× and 8.6× faster, the margin growing with the work. From 415
  label-scanned pathway seeds, v2 against v3: `{1,2}` 1.65 / 0.86 ms (34,669 paths), `{1,3}`
  11.81 / 4.39 ms (228,878) and 23.24 / 12.85 ms returning them, `{0,2}` 1.65 / 0.84 ms,
  `{1,3}` under an `Event` end label 19.94 / 7.20 ms, backward `{1,2}` 0.60 / 0.52 ms, both
  directions `{1,2}` 7,280 / 843 ms - 125,690,888 paths, 149M against 17M a second; from
  83,459 reaction seeds `{1,2}` 249 / 86 ms (4,788,031); from one reaction both directions
  `{1,2}` 76.9 / 17.6 ms (253,253) and `{1,3}` 2,315 / 613 ms (35,960,749); from every
  `Species` backward `{1,2}` 263 / 96 ms (3,656,856). v2 wins the shapes that finish under a
  millisecond (`{1,1}` 0.22 against 0.36 ms), which is v3's fixed per-query cost.
  **The seed lookup is not the walk**: `MATCH (p:TopLevelPathway {stId})` alone costs v2
  46.04 ms and v3 0.42 ms, since `FuseScanByPropertyValue` turns it into
  `db.scan_nodes_by_property_value` over the label's ranges where v2 scans all 2.98M nodes -
  a property-seeded comparison measures that fusion, not the explorator, which is why the
  shapes above are label-seeded. The quantifier costs what the equivalent chain of plain hops
  costs in both: `{3,3}` 46.29 / 0.62 ms against three hops 45.45 / 0.56 ms.
- **Reactome, the oracle for the typed walk**: v2 cannot run a typed quantifier but runs a
  typed chain of plain hops, the same set at a fixed depth. On Signal Transduction's
  `hasEvent`, v3's `{k,k}` and v2's chain of k hops return the same rows - compared as
  multisets of `stId`, not counts - at every depth: 17, 140, 480 and 1,169 rows for k of 1 to
  4, and their union is exactly v3's `{1,4}`, 1,806 rows. Same at depth two for
  `precedingEvent` and `hasComponent`.
- **Reactome, the fan-out the three gates estimate, and its fix**: `sampledFanOut` divided
  the walked type's edges by every node it sampled and `estimatedEnumerationChecks` floored
  that at 1.0. Every Reactome relation is carried by a small subset of the 2.98M nodes, so
  all of them landed below 1, the floor pinned the fan-out at exactly 1.0, and the model
  priced every typed walk as a chain of one candidate per hop - at most `farthest` = 254 per
  seed even unbounded. **Applied**: `sampleBranching` weights each sampled node's continuing
  edges by its arriving ones, which is the branching a walk sees at the nodes it reaches
  rather than an average over nodes no hop of it visits, and reports the nodes the type can
  reach so the frontier is bounded by those instead of by the graph. The estimate charges the
  frontier only while it grows and stops at the first level that covers the type, rather than
  extrapolating a saturated frontier to the bound. Measured over the whole graph, the new
  statistic is the type's two-hop paths over its edges: `hasEvent` 121,323/73,882 = 1.64,
  `hasComponent` 213,396/186,017 = 1.15, `precedingEvent` 79,802/274,487 = 0.29. On the
  bench's uniform shapes, where every node carries the walked type and degrees are
  uncorrelated, it returns the same fan-out as before, so the calibration of
  `indexUnitCostInChecks` and of the layout gates stands.
- **Reactome, why the DISTINCT rule cannot be an estimate**: the relation that explodes has
  the *lowest* mean branching of the three. `precedingEvent` averages 0.29 continuations per
  node reached and is the walk that never finishes, because the hub it passes through carries
  95 of them - a mean below one with a heavy tail is still supercritical in the region the
  trails proliferate in - while `hasEvent` averages 1.64 and finishes in milliseconds. No
  mean-field statistic orders these two, so no gate tries: **applied**, first for an unbounded
  maximum alone (`bf634f88a`), since the trails it would enumerate have no cost bound while
  the search stops at its fixpoint and, under DISTINCT, answers the same question - `MATCH
  (r:Reaction {stId})<-[:precedingEvent]-+(d:Reaction) RETURN DISTINCT r.stId, d.stId` went
  from killed at 200 s to 1,520 rows in 32 ms - and since `9e9704046` for every `distinct`
  exploration: the estimate caps its levels at 254 and floors the fan-out at 1, so on this
  relation both sides saturated near 16,000 checks and it chose the walk at every finite
  bound, `{1,100}` and `{1,50000}` both killed past 28 s where the same query written `+`
  took 32 ms.
- **What that rule costs, and the fixed cost it uncovered**: the unbounded hierarchy shapes
  that used to walk now search - the 29 human top-level pathways' events go from 11.2 ms
  (the same query bounded at its own depth of 13 still walks) to 38.7 ms for the same 19,425
  rows, all 415 to 82.5 ms, and the human complexes' components to 70.7 ms. Every one of
  those is one fixed cost: an unbounded DISTINCT query whose search reaches **nothing at all**
  (`(s:Species)-[:hasEvent]->+(x:Event)`, 0 rows) takes 31.1 ms against 1.44 ms walked, which
  is `setDistinctEnds` assigning one 32-byte `ReachWords` per node of the graph - 95 MB
  written and first-touched per input chunk, whatever the ball. **Applied** (`9e9704046`):
  `PathReachTable`, an open-addressing table keyed by the nodes a batch reaches - 32-byte
  aligned slots of node, seen, frontier and gained words, half full, doubled on growth,
  cleared through the slots it occupied - replaces the array, and one pass per level moves
  a node's gained word into its frontier word where three passes over the dense words ran
  before. The 0-row query drops to 0.31 ms, the hub's `+` from 32.2 to 0.91 ms and its
  `{1,100}` from killed to 0.93 ms with the same 1,520 rows, Signal Transduction's
  `hasEvent {1,100}` from 2.0 walked to 0.95, the 415 pathways' reactions within 100 hops
  from 39.9 walked to 30.2, and the complexes' `count(DISTINCT)` from 84.2 walked to 61.2. On
  the bench's no-overlap shape the search stays at 1.35× the walk (21.8 against 16.1 ms), the
  constant the dense words had. `docs/path_bench.md` holds the table.
- **Reactome, where all-trails becomes exponential**: `<-[:precedingEvent]-{1,N}(d:Reaction)`
  from the 95-successor hub `R-HSA-2993780` counts 96, 101, 103, 105, 108, 179, 256, 349 and
  1,412 rows for N of 4, 8, 10, 12, 14, 16, 18, 20 and 24, each under 2 ms; 9.55 ms at 32,
  past a 90 s cap at 64, and the unbounded form killed at 420 s. Trail counts double every
  two levels past 14, so `+` on that relation is an exponential query and not an engine
  defect - v2 cannot express it at all - which is what makes the gate above the lever.
- **The trie's memory, measured and bounded**: with `e` bound, the trie appended an entry on
  every step of the walk that emitted or expanded and freed none until the query ended, so it
  held the search tree, not the rows. From one seed, `-[e:precedingEvent]->{1,N}(r) RETURN
  count(e)` against `count(r)` cost 146 MB more at N = 24, 983 MB at 28, 3.7 GB at 32 and
  7.7 GB after 60 s at 60, and 8 to 19 % of the time; the unbounded form grows at about
  130 MB/s, so it would have filled the box before its 240 s kill. **Applied**: the trie
  holds one entry arena per walk - a `PathRef` names its arena in its top 16 bits and its
  index in the low 48, arena 0 is the root - and an arena is the walk's stack: emitting a
  row pins the arena at its size, `popFrame` truncates the arena to the entry it backs out of
  when that lies above the pin, and each `fill` first rewrites the walk's current path to
  the bottom of its arena and drops the rest (`retainChain`), the previous chunk having been
  consumed. Exact because no consumer keeps a handle past its chunk: sort, dedup, `WITH` and
  `RETURN` expand the path first, `count` reads the handle column row by row, and the cross
  product is a nested-loop join with both factors' chunks in flight; nested explorations
  share the query's trie and hold their own arenas. Medians of interleaved rounds against
  the previous binary: the unbounded `count(e)` flat at 4,701 MB after 60 s (12,371 before),
  `(r:Reaction)-[e]->{1,2}(m) RETURN size(e)` 156 → 108 ms for 4,788,031 rows, the 415
  pathways' `[e]-{1,2}` in both directions 1,860 → 1,491 ms and 8.6 → 4.8 GB for
  125,690,888 rows, the seed's `{1,28}` 6,291 → 5,164 ms, `RETURN e` over 228,878 paths
  9.79 → 9.68 ms. Trie-free queries move nowhere: the 26 queries of `scripts/bench_paths.py`
  run as a simultaneous pair on separate turing dirs give a median ratio of 1.01 with a 0.99
  to 1.02 spread, `samples/path_bench`'s enumeration with paths 15.3 → 14.8 ms, a
  19-query corpus of every shape a path flows through (cross product, `ORDER BY size(e)`,
  `DISTINCT e`, `WITH e`, nested `e`, `f`, `UNWIND e`, `SKIP`/`LIMIT`) byte-identical.

Remaining, in the suggested order:

1. **Rerun the harness on a small-world graph**, where the multi-source search is expected to
   share more than on a uniform random graph; reactome covered the larger, non-uniform case
   through Cypher (Status, above), but the storage-level sections have only run on uniform
   random shapes. The two index gates, the hybrid target index and the distinct mode's
   table are all measured and applied; the walkers those shapes argued for were later
   measured on reactome and removed (Status). `db.remove_duplicates` is the cost
   that remains in `RETURN DISTINCT`.
2. **Tier 4, type-sorted adjacency runs** first: every typed traversal benefits, not only
   paths. Then SoA edge records, the patched-node bitmap, and the per-commit oracles
   (Section 4). The factorized path column listed there landed in Tier 1 as the trie.
3. **Deferred from Tier 3** (Section 3, "Deferred"): join-based enumeration, hub-suspended
   search, direction choice. All need a forward index from the seeds and a tuning constant;
   the direction choice also needs cardinality estimates or a path column carrying its
   orientation.
4. **Open items across the tiers**:
   - A sparse variant of `PathDistanceIndex` for small reached balls; it is dense today (a
     byte per node). `PathReachTable` holds two to four times the dense 32 bytes per node on a
     ball that saturates the graph; if that ever matters, a dense layout chosen the way
     `PathTargetIndex::planBatch` does, with sparse kept as the default, is the shape.
   - DISTINCT over both directions is exact only at a minimum of 0 (the one-edge backtrack
     is a closed walk with no trail); the one-hop case needs a per-seed closed-trail check
     the bit-parallel search cannot express, so the pass leaves it to enumeration.
   - A bound end with end labels builds only the target index, not the label distance index.
   - `type(e)` and `id(e)` on a quantified `e` are moot rather than open: neither function
     exists in the engine at all - `type` does not parse ("unexpected TYPE") and `id` gives
     "Function 'id' does not exist" - on a quantified `e` or anywhere else.
   - Longer inner patterns, `((a)-[e1]->(b)-[e2]->(c)){1,3}`, are rejected by the parser, not
     the analyzer: "unexpected TAIL_BRACKET, expecting CPAREN".
   - No sample under `samples/mlir` carries `end_labels`, `end_column` or `distinct`; the
     `mlir` sample tool still crashes on `-dump-lowered` for a sample with a label
     constraint and no graph, which keeps `explore_paths.mlir` schema-free.
   - An arena keeps the chains of the chunk's emitted rows until the next fill: at most
     rows × depth entries of 32 bytes, 126 MB for a 65,536-row chunk of 60-hop paths sharing
     nothing, and in practice far less through shared prefixes. Not measured to matter.

### Semantics (fixed by Cypher and the v2 oracle)
- Trail semantics: an edge may not appear twice in one path; nodes may repeat.
- One output row per path of length L with min ≤ L ≤ max. `min = 0` emits a zero-length
  path per seed (end node = seed, empty list). Unbounded max is finite by trail semantics.
- Row = (seed input row, end node, path). `e` binds to the path: a `PathRef` handle into the
  query's `PathTrie`, valid for the chunk that emitted it, which expands to the ordered list
  of edge IDs wherever a list is consumed (output, `UNWIND`, list functions, comparisons).
  `m` binds to the end node column, `n` is gathered back through `indices`.
- Hop predicates: a predicate written on the quantified relationship
  (`-[e:KNOWS {since: 2020}]->{1,3}`, `-[e WHERE e.since > 2020]->{1,3}`) or in a
  parenthesized quantified path pattern (`(n)((a)-[e]->(b) WHERE b.age > 30){1,3}(m)`) is
  evaluated once per hop on that hop's source node, edge and end node; a hop that fails it is
  neither emitted nor expanded. A zero-length path has no hop and always passes. Inside the
  pattern `e`, `a` and `b` are the hop's single entities; outside it `e` is the path and `a`,
  `b` are the path's source and end node lists (GQL group variables), read through the same
  trie. The end node pattern `(m:Label {..})` constrains the last node only and stays a
  filter after the op until Tier 2 fuses it.
- Direction FORWARD (out edges), BACKWARD (in edges), BOTH (outs then ins per node, as
  `GetEdgesIterator` does; a self-loop is emitted twice, matching the single-hop op).

### What the incumbents do, and where this goes beyond
- Neo4j `VarLengthExpand(All)`: per-input-row DFS over relationship iterators, relationship
  uniqueness by scanning the path's relationship list per candidate, node/relationship
  predicates inlined into the expansion; `VarLengthExpand(Pruning)` keeps visited state for
  DISTINCT results; the BFS pruning cursor is used when min ≤ 1. Object-per-relationship,
  row-at-a-time, no latency hiding, no distance pruning.
- Memgraph `ExpandVariable`: DFS with a stack of edge iterators per frame, filter lambdas,
  hop bounds; BFS/wShortest are separate operators. Row-at-a-time, no pruning index.
- Kùzu: frontier BFS with morsel-driven parallelism, factorized results, WALK default with
  TRAIL/ACYCLIC checked during enumeration; its own TODO list names tuple-at-a-time path
  output, virtual-call overhead, and bidirectional joins as future work.
- Beyond all three: (1) the trail check is a signature test, not a list scan; (2) candidate
  generation is a straight loop over a contiguous `EdgeRecord` span, emission is chunked
  through `indices` and costs one trie append per row, so a downstream filter, sort or LIMIT
  moves 8 bytes per path where Kùzu writes paths tuple at a time and Neo4j builds path
  objects; (3) DRAM latency is hidden twice over: within a walk, each frame prefetches the
  adjacency of its next candidate while the current one is descended; across walks,
  independent seed walks are interleaved (AMAC / coroutine interleaving, which no graph
  engine applies inside its traversal operator); (4) constrained explorations prune by a
  query-time reverse-distance index, the technique that made PathEnum orders of magnitude
  faster than dynamic-pruning DFS; (5) DISTINCT reachability runs 64 seeds per machine word;
  (6) hop predicates run vectorized over a whole frame of candidates through the same chunk
  operators as any filter, where Neo4j and Memgraph evaluate them per relationship.

### Cost model that orders the work
Total work for all-trails enumeration = Σ over partial paths of (adjacency fetch +
candidate checks + hop predicate, plus one trie append and one truncation on backtrack when
`e` is read) + O(depth) per row where the path is expanded. For an unconstrained
`(n)-[e]->{1,k}(m) RETURN e` the output itself is exponential and inherent, so only
constants matter: candidate-check cost, adjacency latency, materialization bandwidth
(Tier 1). For a constrained end (`(m:Rare)`, a bound `m`, `WHERE m.x = 1`, DISTINCT,
`count`, LIMIT) the room is algorithmic: every partial path that cannot complete is wasted,
and pruning them changes the exponent (Tiers 2–3).

### Why a global visited set does not apply to the enumerator
A visited set changes cardinality: Cypher wants every trail, not every reachable node (the
oracle has `[[0],[4]]` and `[[1],[7]]` both ending at Remy, and paths that revisit nodes).
Reachability tools (visited bitsets, MS-BFS) enter only as a *pruning index* (Tier 2) or
under DISTINCT semantics an optimizer pass has proved (Tier 3).

## Design

### 1. `PathExplorator` (storage/iterators/PathExplorator.{h,cpp}) — Tier 1

Chunk-writer contract, so `NLExecutor` drives it exactly like the edge writers
(`static_assert(NonRootChunkWriter<PathExplorator>)`):

```cpp
class PathExplorator {
public:
    PathExplorator(const GraphView& view,
                   const ColumnNodeIDs* inputNodeIDs,
                   PathExplorationDir direction,
                   uint64_t minHops,
                   uint64_t maxHops);
    ~PathExplorator();

    void setIndices(ColumnVector<size_t>* indices);
    void setTargets(ColumnNodeIDs* targets);                                 // nullable
    void setPaths(ColumnVector<PathRef>* paths, PathTrie* trie);             // nullable
    void setHopFilter(PathHopFilter* filter);                                // nullable
    void setEdgeTypeFilter(EdgeTypeID edgeType);
    void setCandidateLookahead(size_t lookahead);                            // prefetch distance, 0 disables

    void reset();
    void fill(size_t maxCount);
    bool isValid() const;
```

Contract details the executor relies on: `maxHops == 0` is legal and means "never expand"
(only zero-length rows when `minHops == 0`); `fill` leaves `indices` empty when nothing was
produced; every `fill` either emits ≥ 1 row or flips `_valid`.

`PathExplorationDir` moves from `query/pipeline/PathExplorationDir.h` to
`storage/iterators/PathExplorationDir.h` (storage cannot include query). Includers to update
to `"iterators/PathExplorationDir.h"`: `query/pipeline/PipelineBuilder.h:9`,
`query/pipeline/processors/PathExplorerProcessor.h:15`, `query/plan/nodes/PathExplorerNode.h:5`.

**Part directory, built once in the constructor** from `view.dataparts()` (append-ordered,
disjoint node ranges): `_partFirstNodeIDs`, `_partIndexers` (`&part->edgeIndexer()`),
`_partEdges` (`&part->edges()`), `_patchPartIndices` (parts with `getPatchNodeCount() > 0`,
including zero-node SET-only parts, ascending), `_filterTombstones =
view.tombstones().hasEdges()`, and `_adjacencyBytes` (Σ parts' edge array bytes), which the
index gates weigh their cost against.

**Adjacency of a node `u`**: owner part = last part with `firstNodeID <= u`
(`std::upper_bound` over `_partFirstNodeIDs`, a handful of parts), its span(s)
(`getNodeOutEdges` for FORWARD, `getNodeInEdges` for BACKWARD, out then in for BOTH), then
the span(s) of every patch part with index > owner. Each edge lives in exactly one part's out
array and once in its in array, so this yields each edge once per direction with no dedup set.

**Walk state: the current path and one frame per depth** (plain `std::vector`s, reused
across seeds and input chunks, bounded by depth × degree), held by the explorator itself:
- `_seedRow` of the active seed.
- Current path, root first: `std::vector<EdgeID> _pathEdges` (length = depth),
  `std::vector<PathRef> _pathEntries` (`[d]` = trie entry of the path's first `d` edges,
  `[0] = PathTrie::ROOT`), `std::vector<uint64_t> _pathSignatures` (`[d]` = OR of one
  Fibonacci-hashed bit `1 << ((id * 0x9E3779B97F4A7C15) >> 58)` per edge of
  `_pathEdges[0..d)`, so `[0] = 0`).
- Frames: `struct Frame { size_t _candidateBegin; size_t _candidateEnd; size_t _next;
  NodeID _node; uint64_t _budget; size_t _taint; }` in `std::vector<Frame> _frames`, one per
  node on the path whose children are being walked (`_node`, `_budget` and `_taint` serve the
  distinct mode below and cost nothing otherwise);
  candidates live in two flat stacks `std::vector<NodeID> _candidateNodes`,
  `std::vector<EdgeID> _candidateEdges`, each frame owning the range above the previous
  frame's end. Popping a frame truncates the candidate stacks to its `_candidateBegin`.
- `_arena`, the walk's entry stack in the trie, and `_pinned`, the arena size the chunk's
  last emitted row holds.

**Walk (depth-first):**
1. `startSeed(row)`: `_pathEdges`/`_frames` cleared, `_pathSignatures = {0}`,
   `_pathEntries = {ROOT}`; when `minHops == 0` emit the zero-length row (target = seed,
   path = `ROOT`); when `maxHops > 0` request a descent into the seed (step 2). An empty
   candidate range is never pushed.
2. `descend(node)` at `depth`: resolve the owner part and `__builtin_prefetch` its
   `NodeEdgeData` entry (`getNodeData()[getPatchNodeCount() + (node - firstNodeID)]`), read
   the range and prefetch the first cache lines of the span
   (`edges().getOuts(first, count)` / `getIns`), then generate the accepted
   children into the candidate stacks in one straight loop over the owner span(s) and the
   patch parts' span(s): reject `edge == _pathEdges.back()` (the immediate backtrack,
   dominant in BOTH), a type mismatch when a filter is set, a tombstoned edge when
   `_filterTombstones` (so a deleted edge is neither emitted nor expanded), then the trail
   check: `(_pathSignatures[depth] & bit(edge)) == 0` accepts outright; on a hit scan
   `_pathEdges` (contiguous, ≤ depth entries). Append `(record._otherID, edge)` and record
   the frame. When a hop filter is set, run it once over the frame's range (the source node,
   the candidate node span and the candidate edge span); it compacts the two spans in place
   and the frame shrinks to the survivors. Seeds need no special case: signature 0 and an
   empty path never match.
3. Consume: top frame exhausted (`_next == _candidateEnd`) → pop it (truncate candidate
   stacks, `_pathEdges.pop_back()`, `_pathSignatures.pop_back()`, `_pathEntries.pop_back()`
   with the arena truncated to that entry's index when it lies above `_pinned`, unless it is
   the seed's frame); no frames left → seed done, the walk takes the next seed. Otherwise
   take candidate `(node, edge)` at `_next++`, child depth `d = _frames.size()`: when
   `d >= minHops` emit a row (indices ← `_seedRow`, targets ← `node`, paths ← the trie entry
   of step 4); when `d < maxHops` push `edge` onto the path (`_pathSignatures.push_back(top |
   bit(edge))`, `_pathEntries.push_back(entry)`), issue the frame's candidate prefetch (below)
   and request a descent into `node`. A leaf candidate (`d == maxHops`) never touches
   the path arrays. Unbounded max is `UINT64_MAX`; trail semantics bound the depth by the
   number of live edges.
4. Path emission is one trie append: `entry = trie.append(_arena, _pathEntries.back(), edge,
   node, d)` returns the `PathRef` written to `paths`, and a descent into that candidate
   pushes the same entry, so a prefix shared by many paths is stored once and a row costs one
   entry whatever its depth. Emitting sets `_pinned` to the arena's size: the entries under an
   emitted row stay through the backtracks over them, everything above the pin is truncated
   as the walk backs out of it, and the next `fill` rewrites the walk's current path to the
   bottom of the arena and drops the rest, the previous chunk's rows having been consumed. A
   candidate below `min` that is descended still gets an entry, as the parent of the rows
   under it; a leaf gets one only when it is emitted. When `paths` is null
   (neither `e` nor a group variable is read) nothing is appended and `_pathEntries` stays
   `{ROOT}`.

**Per-frame candidate prefetch:** when a frame descends into candidate
`k`, it first `__builtin_prefetch`es the `NodeEdgeData` entry of candidate `k + lookahead` of
the same frame (skipped when `_next + lookahead >= _candidateEnd`; owner part resolved by
NodeID range, the address stage A computes), so the frame's next descent finds the entry
resident when the child's subtree returns. A frame descends into its candidates only while
their depth is below `maxHops`, and most descents happen at the deepest expanding depth, whose
children generate leaves and return at once: there the window between prefetch and use is one
adjacency fetch plus leaf emission, about the latency being hidden. A deeper subtree may evict
the line before the frame resumes; the prefetch is then wasted, never wrong. Patch-part
lookups (`_patchNodeOffsets` hash probe) are not prefetched; the patched-node bitmap (Tier 4)
is where that goes. `lookahead` is `setCandidateLookahead`, default 1, 0 disables. It is the walk's only latency
hiding.

**Loop (`fill(maxCount)`):** clear the bound columns, `resize` them to `maxCount`, then step
the walk until `maxCount` rows are written (the frame stack keeps the position, so the next
`fill` resumes from it) or the seeds run out (truncate the outputs, `_valid = false`). A step
is one consume: pop an exhausted frame, or take the next candidate and descend into it when
it expands. Seeds are walked one after another, which the interleaved walkers this replaced
did not do; nothing relies on the order of `indices` either way.

Unused outputs are null and skipped; the trie is not touched when `e` and the group
variables are unread (the translator passes null when the block argument has no uses).

**Path column: a per-query prefix trie (`storage/list/PathTrie.{h,cpp}`).** `PathRef` is a
new `ID` instantiation in `storage/ID.h`; `ColumnVector<PathRef>` is registered like any
column (`ContainerKind::Types`, `LocalMemory` pool, `staticKind`) and joins every row-movement
kind switch that lists `ListView` today (filter compaction, gather through indices, block
repeat, copy range, sort permutation), so the path column is carried like a node ID column.
`PathTrie` holds `PathTrieEntry` `{PathRef _parent; EdgeID _edge; NodeID _node; uint64_t
_depth;}` in arenas, one per walk, acquired by the explorator and released with it (nested
explorations share the query's trie and hold their own); a `PathRef` names its arena in its
top 16 bits and its index in the low 48, and `ROOT`, the zero-length path, is arena 0. An
arena is the walk's stack: the entries above the chunk's last emitted row are truncated as
the walk backs out of them, and each `fill` first rewrites the walk's current path to the
arena's bottom (`retainChain`) and drops the rest, so the trie holds the chunk being filled
and the live prefixes, never the search tree. That is exact because no consumer keeps a
handle past its chunk: sort, dedup, `WITH` and `RETURN` expand the path first, `count` reads
the handle column row by row, and the cross product is a nested-loop join with both factors'
chunks in flight. The trie is owned by `LocalMemory` next to `listBuffer()` (`pathTrie()`,
cleared with the query); each prefix is stored once and the list buffer only grows where a
path is expanded. Expansion is `db.expand_path(%paths, %srcids) kind edges|sources|ends`
(`column<path>` → `column<list<edge_id>>` or `column<list<node_id>>`): per row,
`reserveList(depth, ...)` then fill from the tail while walking parents, so the last-first
chain needs no reversal buffer; `sources` is the seed followed by the ends of all entries but
the last. `ListWriteCursor::writeValue` needs explicit instantiations for `EdgeID` and
`NodeID` in `storage/list/ListWriteCursor.cpp` for it. `db.path_length(%paths)` reads
`_depth`. Codegen binds `e` to the path column and emits the conversion each use's type
demands: `db.expand_path` where a list is consumed (`UNWIND`, `IN`, list functions,
comparisons, `DISTINCT` or `ORDER BY` keys, `collect`), `db.path_length` for `size(e)`, the
column itself to `db.output`. `nl.output` expands a `chunk<path>` operand through the trie
into a chunk-scoped `ListBuffer` cleared after each write, so the sinks keep receiving
`ColumnVector<ListView>` and render exactly as today.

**Hop predicates.** Grammar (`CypherParser.y`): `edgeDetail` and `nodePattern` gain an
optional inline `WHERE expr` (Neo4j 5 syntax), and `patternElemChain` gains the parenthesized
form `OPAREN nodePattern edgePattern nodePattern opt_where CPAREN quantifiedPath nodePattern`,
one hop inside in Tier 1. The parser desugars the parenthesized form onto the `EdgePattern`:
the two inner `NodePattern`s become its hop source and hop end, the WHERE its inner
predicate; the property map on a quantified edge desugars to equality conjuncts on the hop
edge. An inline WHERE on a non-quantified pattern is a conjunct of the enclosing MATCH's
WHERE. Analyzer: `ReadStmtAnalyzer::analyze(EdgePattern*)` creates hop decls for the edge and
the two inner nodes (`EvaluatedType::EdgePattern`/`NodePattern`, not quantified) and analyzes
the inner node constraints and the inner predicate in a nested scope where the names resolve
to them; the outer decls stay quantified (`_isQuantifiedPath`), so `e.prop`, `a.prop` and
`a:Label` outside the pattern are rejected as list access. Codegen: `addExplorePaths` builds
the op's `hop` region with block arguments `(%source: column<node_id>, %edge:
column<edge_id>, %end: column<node_id>)`, binds the hop decls to them in `_varMap` for the
duration, emits the inner label, property and WHERE predicates through the ordinary
expression path (`translateExpr`, the mask building of `applyPredicateFilters`) and
terminates with `db.yield %mask`; no region when there is nothing to check. Group variables
`a`/`b` outside register to the path column, the use-site conversion picking `kind
sources`/`ends`. Execution: the region lowers like a `db.cross_product` factor
(`lowerFactor`) into `nl.explore_paths`'s region, the translator turns it into an
`NLStmtContainer` over three loop-owned columns, and `NLHopFilter : PathHopFilter`
(`storage/iterators/PathHopFilter.h`, an abstract class with one `filter` method so storage
stays free of query types) fills the source column with `std::fill_n`, copies the two spans
into the edge and end columns, runs the statements and compacts the spans by the mask. The
property, label and type reads inside are the existing chunk ops, so their gathers stay
vectorized and their misses overlap. Filtered-out candidates are never prefetched, since the
per-frame prefetch runs at consume time over the compacted frame. Passes: `PushDownFilters`
neither enters nor leaves the region; `TrimUnreadColumns` skips it (block arguments are not
carries); Tier 2's end-constraint fusion is unchanged, the region being per hop and end
labels per path; the reverse-distance index ignores hop predicates, which keeps its distances
lower bounds, and tightening it with monotone predicates (ReCAP's viability) is a Tier 2
refinement.

Ordering note: rows come per seed in depth-first order (span order within a part, parts
ascending), not v2's depth-major order, so the v2 JSON oracles
(exact-string, chunk-sensitive) stay v2-only; v3 tests compare sorted rows as the existing ir
tests do.

### 2. Pruning — Tier 2

**End-constraint fusion (pass `FuseExploreEndConstraint`, `DBPasses.cpp`).** The codegen
emits `db.explore_paths` followed by `db.get_node_label_set(tgtids)` →
`db.check_label_constraint` → `db.filter` over every column (that is how `(m:Interest)`
lands). The pass folds that chain into an `OptionalAttr<StrArrayAttr>:$end_labels` on the
op, mirroring `FuseScanByLabel`. The explorator then emits a candidate only when its end
node carries the labels (`NodeContainer::getNodeLabelSet` via the owner part: O(1)), which
skips path materialization and the carried-column gathers for every rejected row. A bound
end node (`closeBoundJoin`'s `tgt == bound` equality filter) folds the same way into an
`end_nodes` operand handled in Tier 3.

**Reverse-distance pruning index (PathEnum's light-weight index, adapted).** When the op
carries an end constraint, the explorator can know before descending whether a node can
still reach a valid end within the remaining budget:
- Target set T = nodes carrying `end_labels` (enumerated from each part's
  `NodeContainer::getLabelSetIndexer()` ranges; no scan of all nodes) or the bound nodes.
- `distToTarget[v]` = shortest number of hops from `v` to any node of T along the
  exploration direction, computed by one multi-source BFS from T over the *reverse*
  direction (in-edges for FORWARD, out-edges for BACKWARD, both for BOTH), bounded by `max`
  hops (unbounded max: full reachability). Dense `std::vector<uint8_t>` sized by the total
  node count (255 = unreachable / beyond budget); a sparse map when the reached ball is a
  small fraction of the graph.
- DFS rule at depth `d` (remaining budget `r = max - d`): a candidate `v'` is pushed only if
  `distToTarget[v'] <= r - 1`, and emitted only if `distToTarget[v'] == 0` (i.e. it is a
  target) and `d + 1 >= min`. Distances ignore edge uniqueness, so they lower-bound the
  remaining hops of any trail: the rule prunes only prefixes that cannot complete, never a
  valid one. This is the PathEnum observation that a query-time distance index removes
  ~100× more edges from the search than dynamic barrier pruning, applied to label- and
  node-constrained ends instead of a single t.
- Runtime cost gate (in the executor, which knows the graph): build the index when the
  estimated enumeration work - the seeds times the candidates of every hop up to `max`, at
  the sampled fan-out of the walked edge type and with the frontier clamped at the graph -
  exceeds 0.35 of the index cost `V + E` (the measured ratio of an index unit to a candidate
  check is 0.22 to 0.35; see Status). Tiny explorations skip it; deep or wide ones always
  take it. The BFS itself is the same bounded frontier BFS with a visited bitset that
  Tier 3 reuses.

### 3. Bound targets and DISTINCT — Tier 3

Implemented in this change: the bound end node and the DISTINCT mode, both as attributes
on the explore ops set by passes, with two bit-parallel multi-source searches in storage.
Deferred to their own change, with the reasons at the end of the section: the join-based
enumeration, the hub-suspended search and the direction choice.

**Bound end node (`end_column`).** `closeBoundJoin` emits, for `MATCH (a)-[e]->+(b),
(a)-->(b)`, `db.explore_paths(%a, {.., %b, ..})` followed by `db.eq(%tgtids, %b')` over the
carried copy of `b` and a `db.filter` over every column. The pass `fuse_explore_end_nodes`
folds that pair into `OptionalAttr<UI64Attr>:$end_column`, the index into
`columns_to_filter` of the carried node column holding each seed row's target: the op keeps
its operands and results (the carried copy still comes back, now equal to `tgtids` row by
row), so the carry-set passes need only keep that column (`keepRequiredColumns`) and
renumber the index when the carry set is trimmed (`trimAttributes`). The verifier checks the
index is in range and names a node column. The explorator (`setEndNodes`, the input-aligned
column) emits a candidate only when it is the seed's own target, and every seed of a chunk
prunes against its own target through `PathTargetIndex`: one multi-source BFS per batch of
64 distinct targets over the reverse direction (Then et al.), into an open-addressing table per batch keyed by the
nodes it reaches, holding the word of the targets that reached each and a hop count per
target, so `canReachWithin(v, hops)` is one probe and one byte compare. The walk
resolves its target's batch and bit once at `startSeed`; a seed whose target is beyond
`max` is not descended at all. The index
honours the edge type filter and edge tombstones and ignores hop predicates, as the Tier 2
index does, so it stays a lower bound. The executor builds it per input chunk (the targets
are the chunk's) behind a gate charging each node a batch is expected to reach (see Status).

**DISTINCT mode (`distinct`).** The pass `fuse_explore_distinct_ends` runs after
`trim_unread_columns` and marks an exploration `distinct` when its `paths`
result has no use and every consumer of its other results is dedup-insensitive: a
`db.remove_duplicates`, a `db.count` with `distinct`, a `db.group_aggregate` whose kinds are
all distinct or min/max, or a row-wise op (property and label reads, expressions, filters,
plain hops, further explorations) all of whose users are. Deduplicating a relation earlier
never changes a result that is a set, so the pass is a set-semantics argument, not a
cardinality one: anything counting, cutting or outputting rows before a dedup refuses. In
the explorator `setDistinctEnds` picks between two algorithms by the hop bounds.

*Level search, `min <= 1`.* `fill` becomes a multi-source BFS from 64 seeds per
word with a seen, a frontier and a gained word per node reached, emitting `(seed, end)` the
first time the end gains the seed's bit at a level in `[min, max]`. It answers "reached
within `max`", which coincides with "reached by a trail within `max`" only because a walk
of length in `[1, max]` shortens to a trail of the same range - hence `min <= 1`. A `min` of
1 leaves the seed's own bit out of its seen word so that a closed trail back to the seed -
whose shortest form in a directed walk is a simple cycle - is reported once, and a `min` of 0
reports the seed at level 0 and keeps the bit set. Undirected, the one-edge backtrack is a
closed walk of two hops with no trail behind it, so the search is exact for `both` only at a
`min` of 0 and the walk below takes the rest.

*Pruned walk, deeper minimums (2026-09-19).* The shortening argument fails at an exact
depth - on `a→b`, `b→a` a 3-hop walk reaches `b` and no 3-hop trail exists - so the walk
keeps trail semantics and prunes instead. Each frame carries `_taint`, the shallowest path
position of an edge its subtree could not take because the walk already held it. On pop the
subtree is remembered as `(node, remaining depth)` when `_taint >= its own depth`: every
collision was then with an edge the subtree held itself, so the same walk leaves that node
whatever the prefix above it used, and a later arrival at that pair stops there. Ends are
deduplicated per seed, so each is emitted once. Remembering unconditionally - the obvious
rule - is *incomplete*: on `s→v`, `v→s`, `s→t` the only 3-trail needs `s` re-expanded at
depth 2, and on `s→x→v`, `s→y→v`, `v→x` the only 4-trail needs `v` re-expanded under the
cleaner prefix. Both are pinned as tests, alongside random cyclic graphs at minimums 2 to 4
in all three directions against the reference enumerator. Pruning is off when a hop filter
is set, since a path-dependent predicate would break the invariant. Measured on reactome
from `R-HSA-162582`, `count(DISTINCT m)` over `-[e*k..k]->`: 96 → 28 ms at 6 hops,
597 → 87 at 7, 3,782 → 200 at 8, 24,214 → 392 at 9 and 147,109 → 725 at 10, the last a
195x cut that tracks the 876,262 ends instead of the 4,416,812,730 trails.

Edge type filter, tombstones, hop predicates (run once per
frontier node, as they depend on the hop alone), end labels and a bound end all compose.
The words live in `PathReachTable`, an open-addressing table keyed by node that a batch
fills and clears at the cost of the nodes it reached, so a batch costs its ball, not the
graph; the executor searches whenever the pass marked the op, with no cost gate in between
(see Status for the one that was tried). Rows carry no path.

**Deferred.** The join-based enumeration (PathEnum IDX-JOIN) and the hub-suspended search
(GraphS) both need a second, forward index from the seeds and a cost-based cut or
threshold that PathEnum and GraphS tune against measurements; without the benchmark
harness the plan calls for from step 1, the cut would be a guess that the paper itself
says loses on short paths. The direction choice needs cardinality estimates in codegen or
a path column that can carry its orientation (a reversed walk yields the edge list
backwards), which is a trie change of its own. All three remain listed here as the next
change.

### 4. Storage-level improvements — Tier 4
- **Type-sorted adjacency runs**: sort each node's edges by `EdgeTypeID` inside its run when
  a `DataPart` is built or merged (IDs are assigned by position, so nothing else changes) and
  add `EdgeIndexer::getNodeOutEdgesOfType` via `std::equal_range` (O(log degree)). Typed
  traversals then touch only matching edges, as Neo4j's per-type relationship groups do; the
  by-type single-hop writers regain the contiguous `std::generate` fill. Persisted parts
  built before the change are not type-sorted: keep a per-part flag and the linear-scan
  fallback until they are rebuilt or merged.
- **SoA edge arrays** (`_otherID` and `_edgeID` in separate arrays): the enumerator reads
  2–3 of the 4 fields of every 32-byte `EdgeRecord`; SoA halves the bytes per candidate for
  every iterator, not just this one.
- **Patched-node bitmap per commit**: one bit per node that has patch edges in any later
  part, so the per-part hash probes are skipped for the vast majority of nodes on graphs
  built by many small commits; `mergeDataParts` remains the big lever.
- **Factorized path column**: represent `e` as an 8-byte handle into a per-query prefix trie
  and expand only at `db.output`, `UNWIND e`, or `size(e)` (Kùzu's factorized intermediate
  results and PathFinder's DAG of parent pointers, applied to the result column). Filters,
  SKIP/LIMIT, sorts on other columns then cost O(1) per row instead of O(depth), and the
  list buffer holds each prefix once. Needs a new column kind understood by the sinks.
- **Per-commit pruning oracles** (opt-in per graph, see PATH_RESEARCH.md Section 5). All
  plug into the same DFS rule `prune v if dist(v, T) > remaining budget` and replace the
  per-query bounded BFS when the graph is large and append-mostly:
  - 2-hop distance labels built by pruned landmark labeling (degree-ordered pruned BFSs,
    8-bit distances, bit-parallel labels for the top hubs): distance to a bound target in
    microseconds; right for many bound or per-row targets, wrong for large label target
    sets (a min over targets per lookup). Build minutes to hours, size a few times the edge
    array (PLL Table 3). Soundness under staleness is asymmetric: a label that has not seen
    a deletion under-estimates distances and prunes less (still exact); an unseen insertion
    can over-estimate and over-prune, so insertions must be applied incrementally (WWW 2014
    / M2HL VLDB 2025 maintenance), while tombstones can be ignored by the oracle.
  - Landmark reachability index (Valstar et al.) for typed `EXISTS`/`DISTINCT` over `*`:
    top-degree landmarks store the vertices they reach with minimal edge-type sets;
    non-landmarks store a few shortcuts to landmarks; a landmark that cannot reach the target
    condemns its whole "reachable-by" set (the precomputed form of ReCAP's doomed prefix).
  - DFS interval labels (GRAIL / BFL / O'Reach): linear size, O(1) negative answers, the
    oracle for unbounded `*` toward a bound target where no distance exists.
  - Hub-to-hub segment cache (GraphS HP-Index): persists the segments the hub-suspended
    search of Tier 3 computes, so repeated bound queries (cycle detection, fraud monitoring)
    stitch instead of search; maintenance is a by-product of the queries themselves.

### 5. Dialect ops
- `StorageEnums.td`: `PathDirection {forward, backward, both}` (`I64EnumAttr`, like
  `AggregateKind`), usable by both dialects through `StorageTypes.td`.
- `StorageTypes.td`: `PathRefType` (`!storage.path_ref`); `DBTypes.td`: `ColumnPathRefs` constraint
  (`CPred` over `ColumnType` → `PathRefType`) for the paths result.
- `db.explore_paths(%input, forward, {carries}) hops 1 to 3 type "KNOWS"` →
  `(srcids: node_id, tgtids: node_id, paths: path, filtered...)`. Attributes:
  `PathDirection:$direction`, `UI64Attr:$min_hops`, `OptionalAttr<UI64Attr>:$max_hops`
  (absent = unbounded), `OptionalAttr<StrAttr>:$edge_type`; Tier 2 adds
  `OptionalAttr<StrArrayAttr>:$end_labels`. `Pure`, numeric result names. Verifier: carried
  count and types = filtered, `max >= min`, non-empty type when present. `srcids` is always
  the seed (result 0) regardless of direction: no orientation swap. An optional single-block
  `hop` region (`MaxSizedRegion<1>`) with the three block arguments of Hop predicates,
  terminated by `db.yield` of one `column<bool>`; `Yield`'s `HasParent` widens to
  `CrossProduct, ExplorePaths`. Verifier: argument types, exactly one yielded mask, and every
  value used inside but defined outside is a `db.constant`.
- `db.expand_path` (`kind` enum attribute `edges|sources|ends`, operands paths and srcids,
  result `column<list<edge_id>>` or `column<list<node_id>>` by kind) and `db.path_length`
  (`column<path>` → `column<int64>`), both `Pure`, with `nl.expand_path`/`nl.path_length`
  mirrors over chunks.
- `nl.explore_paths` mirror (`InferTypeOpAdaptor`) with the same `hop` region, returning
  `!nl.iter<chunk<node_id>, chunk<node_id>, chunk<path>, carried...>` via a
  `getPathIteratorType` helper next to `getEdgeIteratorType` in `NLOps.cpp`.
- Passes (`DBPasses.cpp`): do NOT add the op to `isEdgeHop`/`isReverseHop` (its layout is 3
  fixed results, not 4). Add `pathFixedResultCount = 3`; a separate `ExplorePaths` branch in
  `carrySetLayout` (`{_operandOffset = 1, _resultOffset = 3}`), which enrols it in
  `TrimUnreadColumns`; and a separate branch in `climbToLineageAnchor` (result 0 continues
  the input, results ≥ 3 continue operand `1 + (i - 3)`, results 1–2 are born here) so
  `PushDownFilters` moves seed/carried predicates above the exploration and stops target
  and path predicates at it. No pass pushes anything into the exploration except Tier 2's
  end-constraint fusion; hop predicates are placed in the region by codegen because they have
  no column outside it.

### 6. Lowering, translation, execution
- `DBLowering`: add `ExplorePaths` to `opensSourceLoop`; `lowerExplorePaths` mirrors
  `lowerGetOutEdges` (map input and carries, nest in the input's loop, create
  `nl.explore_paths` passing the db attrs through, `buildLoopForSource`) and lowers the `hop`
  region the way `lowerFactor` lowers a cross-product factor; `lowerExpandPath` and
  `lowerPathLength` are one-for-one.
- `NLProgram.h`: extract the seed-expansion core of `NLEdgeLoopData` (input, sources,
  targets, indices, carried columns, limit, body) into a base `NLExpansionLoopData`;
  `NLEdgeLoopData` keeps edge IDs/types on top; new `NLExplorePathsLoopData : NLExpansionLoopData`
  adds the paths column, the trie, the hop filter's statements and its three bound columns,
  direction, min, max, the resolved type filter (`filtersByType`, `edgeType`, `matchable`)
  and, in Tier 2, the resolved end label set.
  `runEdgeLoopSteps` takes the base pointer.
- `NLTranslator`: `IteratorKind::ExplorePaths`; `IteratorConfig` gains `_direction`,
  `_minHops`, `_maxHops` (reuse `_edgeType`); a `toPathExplorationDir` switch; op → config in
  `translateBlock`; `translateExplorePathsLoop` allocates srcids/tgtids/paths with
  `allocColumnIfUsed` (`chunk<path>` maps to `ColumnVector<PathRef>`), reserves `indices`,
  resolves the type name against `view.metadata().edgeTypes()`, passes
  `&_memory->pathTrie()`, and translates the `hop` region with `translateBlock` into the loop
  data's filter statements, its block arguments bound to three loop-owned columns. Factor
  the carried-column loop of `translateEdgeLoop` into `bindCarriedColumns(config, loopBody,
  firstCarriedArgument, loopData)` so both loops share it.
- `NLExecutor::runExplorePathsLoop`: build `PathExplorator`, bind outputs, set the type
  filter and the hop filter (`NLHopFilter` over the loop data's statements), and when the
  type is unmatchable pass `maxHops = 0` so zero-length rows still emit for `min = 0` (do
  not copy the by-type loop's early return); then the existing `runEdgeLoopSteps`. New
  `runExpandPath` and `runPathLength`; `runOutput` expands `ColumnVector<PathRef>` chunks
  through the trie before `appendChunks`. Tier 2 adds the cost gate and the pruning index
  build here.

### 7. Codegen
- `EdgeMetadata` gains `isQuantified()`, `getMinHops()`, `getMaxHops()` (`UNBOUNDED_HOPS`
  sentinel), included in `operator==`, still trivially copyable. `registerPatternElement`
  builds it from `edge->getQuantifiedPath()` on the edge-producing dependency edge only.
- Factor the carry-set gathering of `walkEdge` (carried vars minus `src`, in-flight
  edge-type columns, CALL-yielded columns) into `collectHopCarrySet(src, carrySet,
  InFlightColumns&)` and reuse the existing `rebindInFlightColumns(results, fixedCount,
  carried)` for the rebind; `walkEdge` behaviour unchanged (do not switch it to
  `collectInFlightColumns`, which skips constants and walks all of `_varMap`).
- `walkExplorePaths`/`addExplorePaths(src, edge, tgt, carrySet, direction, min, max)`:
  results `{node, node, path} ++ carried types`; the edge's type constraint from
  `edge->constraints()` becomes the `edge_type` attribute; the inner node constraints, the
  property map and the inline WHERE become the `hop` region (see Hop predicates); register
  `src` → srcids, `edge` → paths, the hop node decls → paths as group variables, `tgt` →
  tgtids (or `joinedTarget`); `rebindInFlightColumns(results, 3, carried)`; no
  `_part._edgeTypeMap` entry for a quantified edge.
- Uses of a path-typed value: `getOrTranslateExprColumn` on a quantified decl yields the path
  column; a consumer that needs a list (`UNWIND`, `IN`, list functions, comparisons,
  `DISTINCT`/`ORDER BY` keys, `collect`) emits `db.expand_path` at the use with the decl's
  kind (`edges` for `e`, `sources`/`ends` for the hop nodes), `size(e)` emits
  `db.path_length`, and `db.output` takes the column as is.
- `expandComponent`: read the whole `EdgeMetadata`; if quantified call `addExplorePaths` with
  the same `edgeSrcDefined ? type : reverseEdge(type)` orientation (a walk against the
  pattern explores BACKWARD from the right node), else the existing switch; skip
  `applyConstraints(edge)` for a quantified edge, keep `applyConstraints(tgt)` (label
  filters gather the list column like any carried column; Tier 2's pass folds them into the
  op). Same branch in `closeBoundJoin` for `MATCH (a)-[e]->+(b), (a)-->(b)`.

### 8. Analyzer
- `ReadStmtAnalyzer.cpp:499-511`: gate both "Edge type filters are not supported with
  variable-length paths yet" and the property-filter rejection on `!_isV3` (v2 keeps them).
  Unknown type names are still rejected by the schema check at
  `:421-430`, so the runtime unmatchable path is reachable only from hand-written IR.
- `VarDecl`: add `_isQuantifiedPath` (set in `ReadStmtAnalyzer::analyze(EdgePattern*)`
  next to the decl creation, following the `_isUnwound` precedent), also on the outer decls
  of the hop nodes; the hop decls themselves are plain. Reject `e.prop`
  (`ExprAnalyzer::analyzePropertyExpr`) and `e:TYPE` predicates (`analyzeEntityTypeExpr`)
  on a quantified edge or node with a message saying the variable binds a list. This is
  invalid Cypher (property access on a list), so it belongs in the analyzer for both engines.
- Hop scope: the inner predicate and the inner node patterns are analyzed in a nested scope
  where `e`, `a`, `b` resolve to the hop decls (see Hop predicates); an inline WHERE on a
  non-quantified pattern is appended to the MATCH's WHERE conjuncts.
- Keep `EvaluatedType` as `EdgePattern` (retyping to list would break v2 and the pattern
  type comparisons).

### 9. Output typing
The sinks are unchanged: `nl.output` expands a path chunk into a `ColumnVector<ListView>`
scratch column through the trie before `appendChunks`, so the shell, the server and the test
sinks keep rendering `EdgeID` lists (`StringRowSink` joins elements with `", "`, empty list
= `""`) and the ported oracle strings hold. A group variable renders as its node list.

## Files

New:
- `storage/iterators/PathExplorator.h/.cpp`, `storage/iterators/PathExplorationDir.h`
  (moved), `storage/iterators/PathHopFilter.h`, `storage/list/PathTrie.h/.cpp`,
  `storage/CMakeLists.txt` entries (iterators and list blocks).
- `test/storage/list/PathTrieTest.cpp` (`add_storage_tests`): append, expansion from the
  tail for `edges`/`sources`/`ends`, `ROOT`, entry count equals rows plus descended prefixes
  on a small fixture; arenas acquired, released and reused, truncation, `retainChain`
  rewriting a chain to the bottom of its arena.
- `test/storage/iterators/PathExploratorReclaimTest.cpp` (`add_storage_tests`): a complete
  digraph on six nodes walked 1, 64 and 1,000 rows a fill - after every
  fill the trie holds at most the chunk's paths and the walk's prefix and the rows match
  the reference; two explorators sharing one trie leave each other's chunk intact.
- `test/storage/iterators/PathExploratorTest.cpp` (`add_storage_tests`), modeled on
  `GetEdgesByTypeIteratorTest.cpp`'s fixture and collector, with a graph submitted in two
  commits so second-commit edges between first-commit nodes are patch edges.
- `test/query/ir/ExplorePathsDialectTest.cpp` (`add_ir_tests`): parse/print round trip,
  verifier rejections (including `hop` region mask type and outside values), lowering shape
  with and without the region, trim keeps attrs and drops unread carries, pushdown
  placement, hand-written `type "NOPE" hops 0` executed on SimpleGraph → 8 zero-length rows.
- `test/query/ir/ExplorePathsCypherTest.cpp` (`add_call_v3_test`): the eight
  `variable-length-paths-*.json` queries with rows ported (`[[0],[4]]` → `"0, 4"`, `[]` →
  `""`), sorted comparison; `SKIP 50 LIMIT 5` asserts count 5 only; v3-only cases:
  `-[e:KNOWS_WELL]->+`, `RETURN count(*)`, `WHERE m.name = ...` after the exploration,
  `MATCH (a)-[e]->+(b), (a)-->(b)`, error for `RETURN e.name`; path column: `size(e)`,
  `UNWIND e AS x RETURN x`, `RETURN DISTINCT e`, `ORDER BY e`, `RETURN e` after a `LIMIT`
  and after a `WHERE` on `m`; hop predicates in the three syntaxes, the same predicate on
  the end only compared against the post-filter form, a failing inner hop cutting its
  subtree, `min = 0` with a predicate no edge passes still emitting the zero-length rows,
  `RETURN b` for a group variable, inline WHERE on a non-quantified pattern.
- `samples/mlir/explore_paths.mlir` + `.nl.mlir` (regress `mlir_samples_parse` picks them
  up; re-run cmake once for the sample glob).
- Tier 2: `FuseExploreEndConstraint` in `DBPasses.td/.cpp`, a bounded multi-source BFS
  helper in `storage/iterators/` (reused by Tier 3), tests for fusion and for pruned vs
  unpruned row equality.

Modified: `DBOps.td`, `DBTypes.td`, `DBOps.cpp`, `NLOps.td/.cpp`, `StorageEnums.td`,
`StorageTypes.td`, `CypherParser.y`, `EdgePattern.h/.cpp`, `NodePattern.h/.cpp`,
`storage/ID.h`, `ContainerKind.h`, `LocalMemory.h`,
`DBPasses.cpp`, `DBLowering.h/.cpp`, `NLTranslator.h/.cpp`, `NLProgram.h`,
`NLExecutor.h/.cpp`, `EdgeMetadata.h`, `VariableDependencyGraph.cpp`,
`DBProgramGenerator.h/.cpp`, `ReadStmtAnalyzer.cpp`, `ExprAnalyzer.cpp`, `VarDecl.h`,
`storage/list/ListWriteCursor.cpp`, the three `PathExplorationDir.h` includers,
`test/query/ir/CMakeLists.txt`, `test/storage/CMakeLists.txt`.

## Implementation order
1. Storage (Tier 1): move the enum, `ListWriteCursor` instantiations, `PathRef` and its
   column, `PathTrie`, `PathHopFilter`, `PathExplorator` with the walker state machine
   (`walkerCount` 1 first, then the per-frame candidate prefetch, then the interleaved
   scheduler), storage unit tests. Build and run them before touching the IR.
2. Dialects and passes: enum, `PathRefType`, ops with the `hop` region, `expand_path` and
   `path_length`, verifier, `inferReturnTypes`, pass branches, samples, dialect test.
3. Lowering, `NLProgram` base extraction, translator, executor, including the hop filter,
   the expansion ops and the output expansion.
4. Frontend: grammar (inline WHERE, parenthesized quantified pattern), `EdgeMetadata`,
   dependency graph, codegen factoring, `walkExplorePaths` with the `hop` region and the
   use-site conversions, analyzer scopes, Cypher test.
5. Tier 2: `end_labels` attribute and fusion pass; bounded multi-source BFS; pruning index
   with the cost gate; tests proving pruned and unpruned explorations return identical rows.
6. Tier 3 and Tier 4 as separate changes, each with its own plan.

Steps 1 to 5 and the Tier 3 change are done (see Status); Tier 4 and the deferred Tier 3
items are next, in the order Status gives.

A measurement harness is needed from step 1 onwards to set `walkerCount`, the candidate
lookahead and the Tier 2 cost gate: a generated out-of-cache graph (tens of millions of
edges) timed through `QueryInterpreterV3`, in the shape of `samples/query_bench`. It is
`samples/path_bench` (see Status); its numbers are recorded there with what they changed.

## Verification
- `make -j8` from `build/`, then `test_storage_path_explorator`,
  `test_query_ir_explore_paths_dialect`, `test_query_ir_explore_paths_cypher`, the whole
  `ctest -R query_ir` suite (hop tests must be unaffected by the `NLExpansionLoopData` and
  `collectHopCarrySet` factoring), `ctest -R query_analyzer`, and `make run_regress` for
  the sample parse test and v2's untouched behaviour.
- Unit tests force small `fill(maxCount)` (1 and 2) to cover mid-frame resume within and
  across seeds, lookahead 0 and 1 giving identical sorted rows (a
  frame with a single candidate exercises the lookahead bound), `min = 0`, unbounded max
  on a cyclic graph terminating with every trail exactly once, `{2,3}`, BACKWARD/BOTH, type
  filter with `min = 0`, `maxHops = 0`, null targets/paths, a hop filter rejecting a whole
  frame (frame popped, no row), patch edges, tombstoned edges, a
  2-cycle yielding `[a,b]`/`[b,a]` but never `[a,b,a]`, a self-loop emitted twice in BOTH,
  and a signature collision (two edges hashing to the same bit on one path) still accepted
  by the exact scan.
- Tier 2: every constrained query returns the same sorted rows with the pruning index forced
  on and forced off; a fixture where a dead-end region dominates shows the pruned run
  touching a fraction of the edges (count candidate checks through a test hook).
- `PathExploratorCyclicTest` carries the shapes a hand-written fixture cannot: a seeded
  generator whose arcs are drawn uniformly over a small node set, so self-loops, parallel
  edges and short cycles arise together, swept against the reference over every direction,
  every `min` to `max` up to three, the type and hop filters, chunk
  1/2/65536 against lookahead 0/1, end labels with the distance index forced on and off, bound
  ends with the target index forced on and off, the distinct mode, and tombstones. Alongside
  it the structural extremes with counts derived by hand: the complete digraph on five nodes,
  a seventy-edge cycle where every signature bit is set past the sixty-fourth hop so only the
  exact scan can reject a repeated edge, parallel edges both ways between one pair, and two
  three-cycles meeting at a node yielding exactly two six-hop trails. The sweep asserts the
  rows it weighed and the structure it generated, so it cannot pass by comparing two empty
  enumerations. Mutation-checked: pruning one hop early is caught by the end-label test
  alone, and dropping the path's first edge from the exact scan by eleven of the thirteen.
  A part sorts its nodes by label set, so the fixtures with hand-derived counts create the
  nodes in one commit and add the arcs over the assigned IDs in the next - numbering arcs by
  insertion order builds a random permutation once the tied label sets outnumber a handful.

## Risks to check while implementing
- The literal `type` keyword in the declarative assembly format; fall back to `of_type`.
- Generated builder signatures for the two ops (attr/operand interleaving).
- `RETURN DISTINCT e` / `ORDER BY e` run on the expanded list chunk: compare has a
  `ListType` branch, key-append may not; add a Cypher test either way.
- `type(e)`/`id(e)` on a quantified `e`: measured void, neither function exists in the
  engine - `type` does not parse and `id` is rejected as unknown - so nothing reaches codegen
  with a path column. The risk returns the day either is implemented.
- The trie held every descended prefix until the query ended - 3.7 GB for one seed's
  2,481,686 closed trails at bound 32, 7.7 GB after 60 s unbounded - fixed by the per-walk
  arenas (Status). What remains is one chunk's chains until the next fill; the list buffer
  only grows where a path is expanded and, at output, per chunk.
- The `hop` region is the first region on a db op outside `db.cross_product`; `db.yield`'s
  parent constraint, the verifier, `lowerFactor` and the translator's block binding must
  generalize, and `TrimUnreadColumns`/`PushDownFilters` must treat the region as opaque.
- The hop filter runs its statements once per frame; on low-degree frames the per-call
  overhead may dominate. If it does, batch several frames' candidates into one call.
- Longer inner patterns in the parenthesized form (`((a)-[e1]->(b)-[e2]->(c)){1,3}`) repeat
  a fixed sub-pattern and need their own op; today the grammar stops them first, with a bare
  "unexpected TAIL_BRACKET, expecting CPAREN", so the limitation is a gap to close and the
  message names nothing.
- The per-frame prefetch is wasted when the descended child's subtree runs long enough to
  evict the line before the frame resumes, and it costs an owner-part resolution plus one
  prefetch per descent; measured with lookahead 0 and 1 on the out-of-cache graph, 1 kept.
  (The cross-seed interleaving this risk was written beside is gone - see Status.)
- The pruning index is a lower bound only if the BFS runs over the exact reverse of the
  exploration direction and honours the same edge type filter; tombstoned edges must be
  excluded from it too.

## Sources
- PathEnum (Sun, Chen, He, Hooi, SIGMOD 2021): https://arxiv.org/abs/2103.11137 — query-time
  distance index, IDX-DFS/IDX-JOIN, cost-based cut; ~100× fewer edges than BC-DFS.
- ReCAP (2026): https://arxiv.org/abs/2604.02553 — early filtering of doomed path prefixes,
  up to 400,000× over Neo4j/Memgraph/Kùzu.
- Asynchronous Memory Access Chaining (Kocberber et al., VLDB 2015):
  https://dl.acm.org/doi/10.14778/2856318.2856321; coroutine interleaving (Psaropoulos et al.):
  https://www.researchgate.net/publication/329670086; GastCoCo (2023):
  https://arxiv.org/abs/2312.14396.
- MS-BFS (Then et al., VLDB 2014): https://www.vldb.org/pvldb/vol8/p449-then.pdf; DuckPGQ's
  SIMD MS-BFS: https://www.vldb.org/pvldb/vol16/p4034-wolde.pdf.
- Batch HC-s-t path processing (ICDE 2024): https://arxiv.org/abs/2312.01424.
- Robust recursive query parallelism (Kùzu, 2025): https://arxiv.org/abs/2508.19379; Kùzu
  recursive join TODOs: https://github.com/kuzudb/kuzu/issues/4285.
- PathFinder / MillenniumDB path modes: https://arxiv.org/abs/2306.02194.
- Neo4j operators: https://neo4j.com/docs/cypher-manual/4.1/execution-plans/operators/;
  Memgraph deep path traversal: https://memgraph.com/docs/advanced-algorithms/deep-path-traversal.
