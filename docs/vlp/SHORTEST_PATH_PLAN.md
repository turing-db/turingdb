# Shortest paths in v3: `PathShortestSearch`

## Status (2026-09-28)

Planned; no code yet. The research behind every choice here is `SHORTEST_PATH_RESEARCH.md`;
section numbers below prefixed with "research" point into it.

## Context

v3 runs variable-length paths through `db.explore_paths` and `PathExplorator` (`PLAN.md`): trail
enumeration over the immutable parts, the `hop` region, end labels, bound ends and end sets
fused in by passes, a bit-parallel distinct search, and path handles in a per-query `PathTrie`
that `db.expand_path` and `db.make_path` turn into lists and named paths. The only shortest path
it has is TuringDB's own `SHORTESTPATH(a, b, prop, dist, path)`: a weighted Dijkstra from one
node set to another that returns one row for the whole query (`docs/ShortestPath/Guide.md`).

What is missing:

- Neo4j's `shortestPath((a)-[*]-(b))` and `allShortestPaths(...)`. The lexer is
  `%option caseless`, so the statement above owns the keyword, and its rule accepts only
  `SHORTESTPATH ( symbol , symbol , name , symbol , symbol )` before RETURN.
- The GQL selectors (`ANY SHORTEST`, `ALL SHORTEST`, `SHORTEST k`, `SHORTEST k GROUPS`, `ANY k`),
  the match modes and `ACYCLIC`.
- `length(p)`, `nodes(p)` and `relationships(p)` on a named path. A named path is typed
  `GraphPath` and only `count(p)` accepts one; the named-path tests read `p`, `count(p)` and
  `p IS NULL` and nothing else. Legacy per-hop predicates (`all(r IN relationships(p) WHERE ...)`)
  and most fixtures need these functions.

Goal: Neo4j's shortest-path semantics (research Section 1) on the exploration machinery, with
the searches the research recommends (research Sections 5 to 7 and 10), in phases that each land
with their tests.

## Decisions

Product decisions (Remy, 2026-09-28):

- **A legacy shortest path whose start and end are the same node, with a minimum of 1, gives no
  row.** Neo4j's default raises 51N23; its documented
  `dbms.cypher.forbid_shortestpath_common_nodes=false` mode drops the row, and that is the
  behaviour here. A minimum of 0 gives the zero-length path. Queries written for Neo4j with
  `WHERE a <> b` return the same rows under either rule.
- **The exhaustive fallback is always allowed.** No setting raises 51N22.
- **`SHORTESTPATH(a, b, prop, dist, path)` stays** beside the Neo4j forms through every phase;
  its syntax is decided afterwards.

Design decisions:

- **One op.** `db.explore_paths` and `nl.explore_paths` gain a selector rather than a new op
  being added. Selection is per (seed row, end node), and every end constraint the passes fuse
  into an exploration (`end_labels`, `end_column`, `end_nodes`, the end factor, the end set)
  restricts which of those partitions exist without changing what is selected inside one. So the
  fusions, the `hop` region, `hop_imports`, the carry set, the trie, OPTIONAL MATCH and reversed
  seeding all apply unchanged.
- **A separate runtime.** `PathShortestSearch` is a new chunk writer with `PathExplorator`'s
  contract. `runEdgeLoopSteps` is templated on the writer and drives either one. `PathExplorator`
  stays an enumerator; the search owns one for its deepening fallback.
- **The executor chooses the search.** A row whose end is bound runs a bidirectional BFS; a row
  whose end is open runs a BFS from its seed. Codegen emits the plain form and passes fuse, as
  CLAUDE.md requires.
- **A legacy `WHERE` is a pre-filter.** The conjuncts that read the path or its relationship list
  go into a `path_filter` region inside the op. A pass moves the ones that can be checked hop by
  hop into the `hop` region: the placement is the semantics, the motion is the optimisation.
- **A GQL MATCH-level `WHERE` is a post-filter.** It stays an ordinary filter after the op.
  `PushDownFilters` already stops path and target predicates at an exploration (`PLAN.md`
  Section 5), so nothing moves it inside.
- **New keywords reserve no names.** `SHORTEST`, `GROUP`, `GROUPS`, `PATH`, `PATHS`, `REPEATABLE`,
  `ELEMENTS`, `DIFFERENT`, `RELATIONSHIPS`, `ACYCLIC` and `ALLSHORTESTPATHS` join `symbol`, with
  no new bison conflict, so labels and properties with those names keep working.

## Semantics

Fixed by Neo4j (research Sections 1.2 to 1.4, fixtures in 1.5) and by the decisions above.

**Selection.** A selective exploration emits, for each seed row and each end node it reaches, the
paths its selector keeps among the candidate paths from that seed to that end:

- `any_shortest`: one path of the smallest length; which one is unspecified;
- `all_shortest`: every path of the smallest length;
- `shortest_k`: the first k paths by length, ties at the k-th length unspecified;
- `shortest_groups`: every path of the k smallest lengths.

Rows are independent: two input rows with the same seed and end each get their own paths. A path
runs from the pattern's left node to its right node whichever side the walk seeded from. Under
OPTIONAL MATCH a row with no path survives with its path null.

**Candidates.**

- Every relationship satisfies the edge types, the direction and the `hop` region, and every
  node the hop node constraints; the whole path satisfies `path_filter`.
- TRAIL: no relationship repeats (legacy, and GQL by default). WALK under `REPEATABLE ELEMENTS`.
  ACYCLIC: no node repeats.
- The length lies between `min_hops` and `max_hops`.
- With `legacy`, a path of one hop or more never ends on its own seed. Without it (GQL), a seed
  that is its own end asks for the shortest closed path.

**Legacy.** One relationship, a minimum of 0 or 1 (analyzer). The `WHERE` conjuncts that read `p`
or the relationship list are pre-filters (`path_filter`); the others are ordinary filters, which
commute with selection because they read only the endpoints and other variables.

**GQL.** Inline predicates, element-pattern and QPP `WHERE` are pre-filters (`hop` region); so is
the `WHERE` inside a parenthesised selective pattern (`path_filter`). The MATCH-level `WHERE` is a
post-filter. A fixed-length pattern under `ALL SHORTEST` or `GROUPS` is a plain match.

## Design

### 1. The op

`StorageEnums.td`: `PathSelector {any_shortest, all_shortest, shortest_k, shortest_groups}`
(`I64EnumAttr`, like `PathDirection`), shared by both dialects.

`db.explore_paths` gains:

- `OptionalAttr<PathSelector>:$selector`; absent is today's enumeration;
- `OptionalAttr<UI64Attr>:$selector_count`, the k of `shortest_k` and `shortest_groups`;
- `UnitAttr:$legacy`, the same-node rule above;
- `Variadic<Column>:$path_imports`, the columns the path predicate reads from outside the path,
  row-aligned with `input_nodes` like `hop_imports`;
- a second region, `path_filter` (`MaxSizedRegion<1>`), whose block arguments are a candidate's
  srcids, tgtids and path handle, then one per `path_imports` operand, yielding one
  `column<bool>`.

```
%s, %t, %p, %b2 = db.explore_paths(%a, {%b}) both hops 1 edge_types ["LINK"] end_column 0
                    selector any_shortest legacy
%s, %t, %p = db.explore_paths(%a, {}) forward hops 1 to 10 selector all_shortest legacy
               path_filter({ ^bb0(%src, %tgt, %path): ... db.yield %mask })
```

Verifier: `selector_count` is present exactly for `shortest_k` and `shortest_groups` and is not
zero; `legacy`, `path_imports` and `path_filter` appear only with a selector; `distinct` never
appears with one; `legacy` requires `min_hops` of at most 1. `nl.explore_paths` mirrors the
attributes, the operands and the region.

### 2. Codegen

- A legacy element, `p = shortestPath((a)-[r*..10]-(b))`, is generated like
  `p = (a)-[r*..10]-(b)` with `selector` and `legacy` set. The far end is bound as today, by an
  equality or label filter over `tgtids` that the end fusions absorb.
- The MATCH's `WHERE` conjuncts that read `p` or `r` are generated into `path_filter`, over
  `make_path` of the region's seed and handle, the way `generateHopRegion` builds the `hop`
  region; the other conjuncts go through `applyPredicateFilters` as today.
- GQL (Phase 3): inline predicates, element-pattern and QPP `WHERE` into the `hop` region as
  today; the parenthesised selective pattern's `WHERE` into `path_filter`; the MATCH-level
  `WHERE` as ordinary filters after the op.
- Codegen chooses no algorithm and moves no predicate beyond where the semantics place it.

### 3. Passes

- `fuse_shortest_step_predicates`, before `fuse_explore_hop_labels`. From a `path_filter` it
  moves `all(x IN relationships(p) WHERE f)` and `none(...)` (as `NOT f`), the same two over the
  relationship list, and the same two over `nodes(p)`, into the `hop` region, provided `f` reads
  neither the path nor the list. The `nodes(p)` forms also become a filter on the seed, since
  `nodes(p)` includes it. An emptied `path_filter` is removed. `dbPassCount` grows by one.
- `fuse_explore_distinct_ends` skips selective explorations. The distinct search answers walk
  reachability, and a seed reaching itself by a closed walk has not reached itself by a closed
  trail (`PLAN.md`, open items); the shortest search already expands each node once.
- Every other fusion keeps the selector: `fuse_explore_end_constraint`, `fuse_explore_end_nodes`,
  `fuse_explore_end_factor` and `fuse_explore_end_set` narrow the ends;
  `fuse_explore_hop_labels` rewrites the `hop` region; `trim_unread_columns` and
  `count_path_rows` do not depend on selection. One EXPLAIN test per fusion pins it, and a new
  fusion over explorations must say whether it commutes with per-(seed, end) selection.

### 4. Lowering, translation, execution

- `DBLowering::lowerExplorePaths` passes the new attributes and operands through and lowers
  `path_filter` the way `lowerHopRegion` lowers `hop`.
- `NLExplorePathsLoopData` gains the selector, its count, `legacy`, and the path filter's
  statements and bound columns; `NLTranslator::translateExplorePathsLoop` translates the region.
- `NLExecutor::runExplorePaths` builds a `PathShortestSearch` when the loop data carries a
  selector, sets the inputs it sets on `PathExplorator`, and hands it to `runEdgeLoopSteps`.
- `NLPathFilter`, beside `NLHopFilter`, runs the `path_filter` statements over a chunk of
  candidate rows (seed row, end, handle) and compacts the survivors.
- `PathHopFilter` gains a reversed entry point for the backward side of a search, whose frames
  hold hop ends: `NLHopFilter` fills the candidates as the hop's sources and the frame's node as
  its end, and `PathLabelHopFilter` checks the frame's node.

### 5. `PathShortestSearch` (`storage/iterators/PathShortestSearch.{h,cpp}`)

**Contract.** The setters `PathExplorator` has (indices, targets, paths and trie, hop filter, edge
type filter, end labels, end nodes, end node set, pending adjacency), plus the selector, its count
and `legacy`; then `reset`, `fill(maxCount)` and `isValid`. A fill emits up to `maxCount` rows and
the next one resumes where it stopped.

**Candidates.** The explorator's candidate generation (owner and patch parts, type words,
tombstones, pending edges, hop filter) moves into `storage/iterators/PathCandidates.{h,cpp}` for
both classes to use, in its own commit, with the explorator's behaviour unchanged. It gains the
reversed direction the backward side needs.

**State**, per side, reused across rows:

- `SearchTable`: open addressing keyed by node, with epoch stamps so a new row costs nothing to
  clear. A slot holds the depth (`uint32_t`), then one parent edge and node (ANY) or the head of
  the node's predecessor list (ALL);
- a predecessor arena of (edge, node, next) entries;
- the current and next frontiers.

A dense stamped array replaces the table when a search reaches a large share of the graph; the
threshold is measured in Phase 2.

**Bidirectional search**, for a row whose end is bound (`end_column`) and differs from its seed:

- expand one whole level of the side whose frontier has the smaller degree sum, a node's degree
  being its span sizes over the parts that hold it plus its pending edges;
- a candidate the other side has reached closes a meeting edge. ANY stops at the first; ALL
  records every meeting edge of that level and finishes it (research Section 5.3);
- the depths of the two sides add up to at most `max_hops`;
- a frontier that empties ends the row without a path.

**BFS from the seed**, for a row whose end is open, labelled or a set: level by level up to
`max_hops`, every reached node that passes the end constraints being an end. An end set stops the
search once all its members are reached.

**Listing.**

- ANY: the parent chains, appended to the trie in walk order.
- ALL: one sweep back from the meeting edges (or from the ends reached) marks the nodes that lie
  on shortest paths and links their successor edges. A depth-first walk over the marked
  successors, across a meeting edge and down the backward side's predecessor edges, emits one row
  per path. Every step has a continuation, so the walk never backs out of a dead end, and it
  resumes across fills from its stack (research Section 10.2).
- The trie's arenas are pinned, truncated and retained per fill exactly as the explorator does.

**Same node.** With `legacy`, a seed is never emitted as its own end unless `min_hops` is 0, which
emits the zero-length path.

**`path_filter`.** Candidates pass through `NLPathFilter` in length order. ANY keeps the first that
passes per (row, end); ALL keeps every passing path of the first length that has one. When the
shortest length has none, the search deepens with its own `PathExplorator`: minimum and maximum
L, end bound to the row's end, pruned by the target index, for L = d + 1 up to `max_hops`, until a
length yields a passing path. The index's distances are lower bounds on trail lengths, so the
pruning drops no valid trail (research Section 10.5).

### 6. Parser and AST

Phase 1:

- the token `ALLSHORTESTPATHS`;
- `patternPart` and `patternAlias` accept `(SHORTESTPATH | ALLSHORTESTPATHS) ( patternElem )`,
  with and without `symbol =`; the existing `shortestPathSt` rule keeps the weighted statement,
  told apart by the token after `SHORTESTPATH (`;
- `PatternElement` records a selector, its count and a legacy flag: `shortestPath` is
  `any_shortest` legacy, `allShortestPaths` `all_shortest` legacy.

Phase 3:

- the tokens `SHORTEST`, `GROUP`, `GROUPS`, `PATH`, `PATHS`, `REPEATABLE`, `ELEMENTS`,
  `DIFFERENT`, `RELATIONSHIPS` and `ACYCLIC`, all in `symbol`;
- `ANY SHORTEST`, `ALL SHORTEST`, `SHORTEST k [PATH|PATHS]`, `SHORTEST [k] GROUP|GROUPS` and
  `ANY [k]` before a path pattern, after `symbol =` too; k a literal or a parameter. `ANY` and
  `ANY k` are `shortest_k`, as Neo4j plans them; `ALL SHORTEST` and `SHORTEST 1 GROUPS` are
  `all_shortest`;
- the parenthesised selective pattern with its own `WHERE`;
- `MATCH REPEATABLE ELEMENTS` and `MATCH DIFFERENT RELATIONSHIPS` on `MatchStmt`, and `ACYCLIC`
  after a selector.

### 7. Analyzer

- Phase 0: `length`, `nodes` and `relationships` signatures over `GraphPath`.
- Phase 1, the legacy rules with Neo4j's messages (research Section 1.2): one relationship; a
  minimum of 0 or 1; no property map on the relationship; no QPP inside; no relationship
  variable bound earlier; not in CREATE or MERGE; not mixed with a selector or a mode. `[r:T]` is
  rewritten to `{1,1}`, so `r` is a list. Each `WHERE` conjunct is marked as reading the path (or
  its list) or not.
- Phase 3, the GQL rules: k greater than 0; under `DIFFERENT RELATIONSHIPS` a selective pattern
  is the only path pattern in its MATCH; it references only variables bound by earlier clauses;
  `REPEATABLE ELEMENTS` needs bounded quantifiers under a selector; `ACYCLIC` is used with
  neither `-[*]-` nor `REPEATABLE ELEMENTS`. A fixed-length pattern under `ALL SHORTEST` or
  `GROUPS` drops its selector.

### 8. The expression form

`RETURN shortestPath((a)-[*]-(b))` gives a path or null, and `allShortestPaths(...)` a list of
paths, with both endpoints bound. Both are pattern comprehensions over a legacy selective
pattern, `head` of the list for `shortestPath` and the list itself for `allShortestPaths`, built
on `translatePatternComprehensionExpr`.

## Phases

| Phase | What | Size |
|---|---|---|
| 0 | `length`, `nodes` and `relationships` of a named path | small |
| 1 | legacy `shortestPath` and `allShortestPaths`, end to end | large |
| 2 | batches and speed, each change gated by a measurement | medium |
| 3 | GQL selectors over one quantified relationship | medium to large |
| 4 | patterns whose next step depends on the position | large |
| 5 | optional: direction-optimising BFS, highway labels, weighted fixes | per item |

Phases 2 and 3 are independent once Phase 1 has landed.

### Phase 0: path functions

- The analyzer signatures of Section 7.
- Codegen: a named path that is one quantified relationship answers from its handles,
  `db.path_length` for `length` and `db.expand_path` for `relationships` and, after the seed, for
  `nodes`. An entity-list path gets one new op, `db.path_elements` with kind `nodes`,
  `relationships` or `length`, lowered one for one.
- Tests: a new Cypher test file on simpledb covering both path shapes, OPTIONAL MATCH nulls and a
  zero-length path.

### Phase 1: legacy `shortestPath` and `allShortestPaths`

Design Sections 1 to 8 for the legacy forms. Done when:

- the storage tests, the sweep and the Cypher tests of the Verification section pass under
  `ctest`;
- the research Section 1.5 fixtures marked "docs" pass, and the "derived" ones once a Neo4j run
  has confirmed them;
- a `shortest` group in `bench/vlp` on reactome and the fraud graph matches Neo4j's row counts and
  path lengths;
- a point-to-point query between nodes at most six hops apart runs within about twice v3's fixed
  per-query cost of about 0.4 ms.

### Phase 2: batches and speed

Each change lands with the measurement that justified it:

- rows of one chunk with the same seed and end share one search;
- rows that share a seed share one BFS from it;
- the 64-lane `PathTargetIndex` with descent (research Section 10.3) where the pair set is dense,
  chosen by the gate of research Section 6.2 with the existing samplers. At first only with no
  hop predicate, no pending edge and every distance within a byte, which meets the exactness
  obligations of research Section 10.4 by restriction;
- `count(*)` and `count(p)` over `all_shortest` from path counts on the DAG, without listing;
- measured before anything is built: degree-sum against node-count balancing, the dense-table
  threshold, a component-ID check for pairs with no path.

### Phase 3: GQL selectors over one quantified relationship

Design Sections 6 and 7 for the GQL forms, then in the search:

- `any_shortest` and `all_shortest` without `legacy`: the searches of Phase 1;
- `shortest_k` and `shortest_groups`: the BFS gives the first length and the deepening gives the
  next, trails of length d + 1, d + 2 and on, until k paths or k lengths;
- a minimum of 2 or more: deepening from max(min, d), since the BFS distance is then a lower bound
  and not an answer (research Section 2.1);
- a seed that is its own end: the shortest closed trail, by deepening with `ends_on_seed`;
- `REPEATABLE ELEMENTS`: deepening without the trail check, bounded by the required upper bound;
- `ACYCLIC`: a node-uniqueness check in the explorator beside its trail check;
- `SHORTEST k` over a fixed-length pattern: a new `db.limit_per_group` that keeps the first k
  rows per (first node, last node) of an ordinary match.

Done when the GQL fixtures of research Section 1.5 pass, the post-filter and pre-filter pair
first (0 rows against 1).

### Phase 4: patterns whose next step depends on the position

- The parser accepts QPP bodies of more than one relationship, which enumeration needs as well.
- Codegen compiles a selective pattern with fixed segments and quantifiers into one exploration
  carrying a position table: for each position the edge types, direction, hop predicate and node
  predicates allowed, and the position reached.
- The search keys its tables by (node, position) (research Section 10.6).
- Trails are validated by position-aware deepening; Neo4j's validate-and-propagate only if
  measurements call for it.

Done when the trail-propagation fixture of research Section 1.5 passes.

### Phase 5: optional

Direction-optimising BFS for searches from one seed that reach most of the graph; highway-cover
labels per compacted version, if hub-heavy graphs need millisecond answers; the weighted
statement's fixes (one answer per row, bidirectional Dijkstra, a 4-ary heap or buckets, direct
span reads).

## Files

Phase 0:

- New: `test/query/ir/NamedPathFunctionsTest.cpp`.
- Modified: `FunctionDecls.cpp`, `ExprAnalyzer.cpp`, `DBProgramGenerator.h/.cpp`, `DBOps.td/.cpp`
  (`db.path_elements`), `NLOps.td/.cpp`, `DBLowering.h/.cpp`, `NLTranslator.h/.cpp`,
  `NLExecutor.h/.cpp`, `test/query/ir/CMakeLists.txt`.

Phase 1:

- New: `storage/iterators/PathCandidates.h/.cpp`, `storage/iterators/PathShortestSearch.h/.cpp`;
  `test/storage/iterators/PathShortestSearchTest.cpp` and
  `test/storage/iterators/PathShortestSearchSweepTest.cpp` (`add_storage_tests`);
  `test/query/ir/LegacyShortestPathTest.cpp`, `test/query/ir/LegacyShortestPathFallbackTest.cpp`
  and `test/query/ir/ShortestPathStationTest.cpp`; `test/query-test-suite/tests/shortest-path-*.json`
  on simpledb.
- Modified: `CypherLexer.l`, `CypherParser.y`, `PatternElement.h/.cpp`, `ReadStmtAnalyzer.cpp`,
  `ExprAnalyzer.cpp`, `StorageEnums.td`, `DBOps.td/.cpp`, `NLOps.td/.cpp`, `DBPasses.td/.cpp`,
  `DBProgramGenerator.h/.cpp` (codegen and the pass table), `DBLowering.h/.cpp`, `NLProgram.h`,
  `NLTranslator.h/.cpp`, `NLExecutor.h/.cpp`, `PathHopFilter.h`, `PathLabelHopFilter.h/.cpp`,
  `PathExplorator.h/.cpp` (the candidate extraction), `storage/CMakeLists.txt`,
  `test/storage/CMakeLists.txt`, `test/query/ir/CMakeLists.txt`.

Later phases list their files in their own changes.

## Implementation order

Each step is a commit, with its failing test written first.

1. Phase 0: the signatures, codegen, and `db.path_elements` through lowering and execution, with
   their tests.
2. `PathCandidates` extracted from `PathExplorator`, the reversed direction included; every
   explorator test unchanged and green.
3. `PathShortestSearch` with ANY and ALL, bidirectional and from the seed, `legacy` and a minimum
   of 0; the storage test and the sweep against the reference.
4. The op: `PathSelector`, the attributes, `path_imports` and `path_filter`, the verifier, the nl
   mirror, lowering, translation, `NLPathFilter`, the reversed hop filter and the dispatch in the
   executor; a dialect test and a hand-written IR test on simpledb.
5. The deepening fallback behind `path_filter`.
6. The legacy grammar and AST; the analyzer rules and messages; codegen; the pass and the distinct
   exclusion; one EXPLAIN test per fusion; the Cypher tests and the Station fixtures.
7. The expression form.
8. The `bench/vlp` shortest group, its Phase 1 numbers recorded in this document.
9. Phases 2 to 5, each a change of its own.

## Verification

- Rebuild first: the binary on the development machine predates named paths. `make -j8` from
  `build/`, then the new targets and `ctest -R query_ir`. Not `make run_regress`, which does not
  exercise v3.
- The sweep: random small graphs as in `PathExploratorCyclicTest` (self-loops, parallel edges and
  short cycles arising together), every direction, a minimum of 0 and 1, bounded and unbounded
  maxima, type and hop filters, tombstones, pending edges, chunk sizes 1, 2 and 65,536. The
  reference enumerates every trail with `PathExplorationReference` and keeps the shortest per
  (seed, end): `all_shortest` must equal it as a multiset, and `any_shortest` must return one of
  its paths per (seed, end). The sweep asserts the rows it weighed, so two empty results cannot
  pass.
- Structural cases with counts derived by hand: a chain of n diamonds (2^n shortest paths), a hub,
  two components, a shortest path through a node with a self-loop, parallel edges on a shortest
  path, a pair joined only by pending edges, a shortest path cut by a tombstone.
- Cypher tests: every error message; OPTIONAL MATCH nulls; each direction; the fallback (research
  Section 1.5, WSH and BMV bound); `WHERE a <> b`; the Station fixtures, whose graph a change
  builds before the queries run, since the V3 suite runner builds only simpledb.
- Ties: `any_shortest` tests compare lengths, counts and sets, never which path came back;
  `all_shortest` tests compare multisets.

## Risks

- Filter placement. A legacy pre-filter applied after the search, or a GQL post-filter moved
  inside it, returns plausible wrong rows. The Station fixtures pin both, 0 rows against 1.
- A fusion that changes selection: covered by the audit in Design Section 3 and one EXPLAIN test
  per fusion.
- New keywords: zero new bison conflicts and a full `ctest` run before landing.
- Exponential `all_shortest` output is the query's own output; the listing is chunked and the trie
  reclaims its arenas per fill.
- The deepening fallback is exponential in the worst case, as Neo4j's is. It is measured on the
  `precedingEvent` hub of reactome (`PLAN.md`, Status) before Phase 1 closes.
- Distances past a byte: the target index stops at 254 levels, so Phase 2 uses it only below that.
- The backward side's hop predicate. A missed role swap passes every forward-only test, so the
  sweep runs backward and undirected patterns with a hop predicate that reads the end node.

## Sources

- `SHORTEST_PATH_RESEARCH.md`: the semantics and fixtures (Section 1), what a BFS can and cannot
  answer (Section 2), Neo4j's implementation (Section 3), the searches and their costs (Sections
  5 to 8), the mapping onto v3 (Section 10).
- `VLP_RESEARCH.md` and `PLAN.md`: the exploration machinery this plan builds on.
