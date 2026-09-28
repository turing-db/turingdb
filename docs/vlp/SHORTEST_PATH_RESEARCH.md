# Shortest paths: a research synthesis

This document collects what is known about answering shortest-path queries in a graph
database: Cypher's `shortestPath` and `allShortestPaths`, and the GQL path selectors
(`ANY SHORTEST`, `ALL SHORTEST`, `SHORTEST k`, `SHORTEST k GROUPS`, `ANY k`) over fixed-length
and quantified patterns. It is the companion of `VLP_RESEARCH.md`, which covers
variable-length path enumeration, pruning indexes, multi-source BFS and hub labelling in
general; this document assumes it and does not repeat it. Section 10 maps the findings onto
the `PathExplorator` machinery that `PLAN.md` describes.

The synthesis is organised as a sequence of questions:

1. What exactly does Neo4j compute? (the legacy functions, the GQL selectors, fixtures)
2. Why is a shortest path cheap when an enumeration is not, and where does that stop?
3. How does Neo4j execute it? (bidirectional BFS, the exhaustive fallback, PPBFS)
4. What do the other engines do?
5. How is one pair made fast? (bidirectional search, balancing, termination, state)
6. How is a batch of pairs made fast? (pair graphs, multi-source BFS)
7. What can be precomputed or laid out better? (reachability, labels, storage)
8. What changes with weights, and with k paths instead of one?
9. What do the benchmarks ask for?
10. What does this mean for TuringDB?

Neo4j's behaviour was read from the Cypher manual (Cypher 25, 2026.09) and from the source of
`neo4j/neo4j` at branch `2026.09` (commit `54a7dcf7c`). The central claims were checked
against both directly: the pre-filter and post-filter wording, the minimum length rule,
`ANY SHORTEST` = `SHORTEST 1`, the partitioning rule, the side choice and depth check in
`BiDirectionalBFSImpl.java`, and the benchmark comment in
`StatefulShortestToFindShortestRewriter.scala`. Numbers from papers are quoted with the paper.
Statements marked *(inference)* are reasoning about TuringDB, not results from a source. Every
source is annotated in the reading list at the end.

---

## 1. What Neo4j computes

### 1.1 Two syntaxes

Neo4j has two shortest-path syntaxes with different semantics:

- The legacy functions: `MATCH p = shortestPath((a)-[:T*..10]-(b))` and
  `allShortestPaths(...)`.
- The GQL path selectors, added in Neo4j 5.21: `MATCH p = SHORTEST 1 (a)-[:T]-+(b)`,
  `ALL SHORTEST`, `SHORTEST k`, `SHORTEST k GROUPS`, `ANY`, `ANY k`, over any path pattern,
  including quantified path patterns (QPPs) and fixed-length patterns.

They differ in where `WHERE` applies, in what happens when both ends are the same node, and in
which patterns they accept (Section 1.4). Both count length in relationships.

### 1.2 Legacy `shortestPath` and `allShortestPaths`

**Accepted patterns.** The compile-time rules (`SemanticPatternCheck.scala`,
`SemanticError.scala`):

| Query | Outcome |
|---|---|
| `[:T*]`, `[*..5]`, `[*0..5]`, `[:A\|B*1..3]`, directed or undirected | accepted |
| an unbounded pattern such as `[*]` | accepted with warning 03N91: "Using shortest path with an unbounded pattern will likely result in long execution times. It is recommended to use an upper limit to the number of node hops in your pattern." |
| `shortestPath((a)-[r:T]->(b))` | accepted with deprecation warning 01N01, rewritten to `[r:T*1..1]` before semantic analysis, so `r` is a list |
| `[*3]`, `[*2..5]` | "shortestPath(...) does not support a minimal length different from 0 or 1" |
| `(a)-[*]-(b)-->(c)`, `shortestPath((:A))` | "shortestPath(...) requires a pattern containing a single relationship" |
| `[*1..5 {since: 2020}]` | "shortestPath(...) contains properties {since: 2020}. This is currently not supported." |
| a QPP or `-->+` inside | "shortestPath(...) contains quantified pattern. This is currently not supported." |
| a relationship variable bound earlier | "Bound relationships not allowed in shortestPath(...)" |
| in CREATE or MERGE | "shortestPath(...) cannot be used in a MERGE clause, but only in a MATCH clause." |
| with a selector, a match mode or a path mode | "Mixing shortestPath/allShortestPaths with path selectors (e.g. `ANY SHORTEST`), explicit match modes (e.g. `DIFFERENT RELATIONSHIPS`) or explicit path modes (e.g. `ACYCLIC`) is not allowed." |
| expression form, `RETURN shortestPath((a)-[*]-(b))`, with an anonymous endpoint | "A shortestPath(...) requires bound nodes when not part of a MATCH clause." The expression gives a path or null; `allShortestPaths` a list of paths |

**Endpoints.** The search is planned as a selection once both endpoints are bound
(`isFindableFrom` in `SelectionPlanner.scala`). Endpoints no earlier clause binds come from the
rest of the MATCH, typically two label scans and a cartesian product, and the search runs once
per row. The manual's example: "Return a single shortest path for each distinct pair of nodes
matching (:A) and (:B)". A null endpoint gives no row; under OPTIONAL MATCH, a null `p`.

**The same node at both ends** (`LogicalPlan.scala`):

- With the default `dbms.cypher.forbid_shortestpath_common_nodes=true`, such a row raises
  51N23: "The shortest path algorithm does not work when the start and end nodes are the same.
  This can happen if you perform a shortestPath search after a cartesian product..."
- With `false`, the row gives nothing.
- With a minimum length of 0, it gives the zero-length path `[a]` and nothing is raised.

So `MATCH p = shortestPath((a:X)-[*]-(b:X))` fails on any graph with an `X` node unless the
query adds `WHERE a <> b`. That predicate reads only the endpoints and runs before the search.

**Ties.** `shortestPath` returns one of the shortest paths, "non-deterministically".
`allShortestPaths` returns all of them. Two parallel relationships give two paths (`BFS.java`
records every same-level predecessor).

**Uniqueness against the rest of the MATCH.** The rewriter that adds relationship-uniqueness
predicates skips the shortest-path part (`AddElementUniquenessPredicates.scala`), so in
`MATCH (a)-[r]->(b), p = shortestPath((a)-[*]-(b))` the path may reuse `r` *(read from the
rewriter, not tested)*.

**`WHERE` is a pre-filter.** The manual: "If the `MATCH` clause of the `shortestPath()`
function includes a `WHERE` clause, this condition will act as a pre-filter".
`extractShortestPathPredicates` (`extractPredicates.scala`) splits the predicates that mention
`p` or its relationship variable:

- Evaluated during the search, per step: `all(x IN relationships(p) WHERE f)` and
  `none(x IN relationships(p) WHERE f)` (as `NOT f`), the same two over `nodes(p)`, and the
  same two over the relationship list, provided `f` mentions neither `p` nor the list. `f` may
  read other bound variables. Node predicates are also checked on both endpoints
  (`ShortestPathSlottedPipe.scala`).
- Everything else that mentions `p` or the list is a *path predicate*: `length(p) > 3`,
  `size(r) = 2`, `any(...)`, `r[0].x`, an `EXISTS { }` over `p`. Path predicates trigger the
  fallback.
- Predicates that read only the endpoints are ordinary filters before the search.

**The exhaustive fallback** (`planShortestRelationships.scala`; the 4.4 manual prints the same
plan):

```
AntiConditionalApply(p)
├─ Apply(input, Optional(a, b) ← ShortestPath(fallback, per-step predicates, path predicates) ← Argument)
└─ Top(1, length(p) ASC)                          allShortestPaths: Top1WithTies
   ← Projection(length(p)) ← Filter(path predicates) ← Projection(p) ← VarLengthExpand(Into) ← Argument(a, b)
```

The BFS runs first, and its shortest paths are filtered lazily by the path predicates. Only
when none pass does the right branch run: every trail between the endpoints (`VarLengthExpand`
uses trail semantics here), filtered, then the shortest kept. The planner emits notification
03N92: "Using shortest path with an exhaustive search fallback might cause query slow down ...
It is recommended to introduce a WITH to separate the MATCH containing the shortest path from
the existential predicates on that path." With `dbms.cypher.forbid_exhaustive_shortestpath=true`
the right branch becomes error 51N22, raised only when the fast search found nothing.

A predicate after a `WITH` is a post-filter and never selects the fallback:
`WITH p WHERE length(p) > 2` filters the one shortest path already chosen.

### 1.3 GQL path selectors

**Selectors.** `SHORTEST k [PATH|PATHS]`, `ALL SHORTEST`, `ANY SHORTEST`,
`SHORTEST [k] GROUP|GROUPS`, `ANY [k]`. `k` is a literal, or a parameter since 2025.06; `k ≤ 0`
is an error ("The path count needs to be greater than 0."). The manual: "`ANY SHORTEST` is
equivalent to `SHORTEST 1`." `ANY k` is documented as any k paths and implemented as
`SHORTEST k` (`expandSolverStep.scala`: "for now we will implement ANY via SHORTEST.").
`ALL SHORTEST` is planned as `SHORTEST 1 GROUPS`.

**Order of evaluation.** The manual's four steps:

1. All paths matching the path pattern are found.
2. Paths failing the predicates inside the path pattern are removed: a *pre-filter*.
3. The selector selects paths.
4. Paths failing the MATCH-level `WHERE` are removed: "This is a *post*-filter."

Pre-filters are inline node and relationship predicates, labels and types, a QPP's `WHERE`, and
a `WHERE` inside a parenthesised selective pattern: `SHORTEST 1 (p = (a)--+(b) WHERE ...)`. A
path variable declared outside the parentheses cannot be read in a pre-filter. Predicates on the
endpoints alone give the same result either way, and the planner moves them out of the pattern
(`MoveBoundaryNodePredicates`).

**Partitions.** "When a selective path selector is specified, the paths matched by the path
pattern are partitioned by distinct pairs of start and end nodes." Each partition has its own
shortest length, and the result is the union of the partitions' selections:

- `SHORTEST k`: the first k paths by length. Ties at the k-th length are broken arbitrarily.
- `SHORTEST k GROUPS`: every path of the k smallest distinct lengths.
- `ALL SHORTEST`: every path of the smallest length.

**Path modes.** The default match mode, `DIFFERENT RELATIONSHIPS`, gives TRAIL semantics within
a path: no relationship repeats, nodes may. `REPEATABLE ELEMENTS` (2025.06) gives WALK semantics
and requires bounded quantifiers under SHORTEST (error 42N53). `ACYCLIC` (2026.03, combinable
with selectors since 2026.05) forbids repeated nodes; it cannot be used with the `-[*]-` syntax
or together with `REPEATABLE ELEMENTS`. Plans show `StatefulShortestPath(All, Trail)` and
`StatefulShortestPath(Into, Trail)`.

**Start equals end** is allowed. The result is the shortest closed trail through the node; with
a minimum of 0, the zero-length path.

**Fixed-length patterns** (`FixedLengthShortestToAllRewriter.scala`). `ALL SHORTEST` and
`SHORTEST k GROUPS` over a pattern with no quantifier are rewritten to plain matching, since
every match has the same length; so is any selector over a single node pattern. `SHORTEST k`
and `ANY k` over a fixed-length pattern still keep k paths per partition. So
`MATCH SHORTEST 1 (a)-[:R]->(b)` returns one row per (a, b) even with parallel `R` edges, and
`SHORTEST 1 (a:User)-[r1]->(b)-[r2]->(c)-[r3]->(d)` plans as a three-transition
`StatefulShortestPath` *(both from planner tests, not the manual)*.

**Restrictions.** Under `DIFFERENT RELATIONSHIPS` a selective path pattern must be the only path
pattern in its MATCH: "Multiple path patterns cannot be used in the same clause in combination
with a selective path selector." Variables referenced inside a selective pattern must be bound
by an earlier MATCH. Selectors are not allowed in CREATE or MERGE.

### 1.4 Side by side

| | Legacy `shortestPath` | GQL `SHORTEST` |
|---|---|---|
| MATCH-level `WHERE` | pre-filter; path predicates select the fallback | post-filter |
| same start and end | error 51N23, no row, or `[a]` with a minimum of 0 | shortest closed trail |
| patterns | one relationship, minimum 0 or 1 | any path pattern, QPPs, fixed segments |
| paths per pair | one, or all shortest | k, k length groups, all shortest |
| path mode | trail, which costs nothing here (Section 2.1) | trail by default; walk or acyclic by mode |

### 1.5 Fixtures

The Station graph is the `CREATE` on the manual's shortest-paths page: 9 stations and 12 `LINK`
relationships carrying a `distance`. "docs" results are printed in the manual. "derived"
results come from an exhaustive enumeration of trails and walks over the same graph. That
enumerator reproduces the manual's `SHORTEST 8 GROUPS` histogram exactly, so it applies the same
semantics, but Neo4j did not produce the derived numbers: confirm them against a Neo4j server
(the `bench/vlp` harness has a client) before they become `expect.result` files. WSH is
Worcester Shrub Hill, BMV Bromsgrove. Patterns are undirected `-[:LINK]-+` unless noted.

```
docs     SHORTEST 1 WSH→BMV                        1 row, length 2, through either of two stations
docs     SHORTEST 2 WSH→BMV                        [WSH, Droitwich Spa, BMV] and [WSH, Worcestershire Parkway, BMV]
docs     SHORTEST 5 WSH→BMV                        those two and three paths of length 3
docs     ALL SHORTEST WSH→BMV                      2 rows
docs     SHORTEST 2 GROUPS WSH→BMV                 5 rows: 2 of length 2, 3 of length 3
docs     SHORTEST 8 GROUPS WSH→BMV                 length:count 2:2 3:3 4:1 5:4 6:8 7:10 8:6
docs     ANY (Pershore)-[l:LINK WHERE l.distance < 10]-+(Bromsgrove)
                                                   distances [4.16, 3.71, 5.76, 6.16]
docs     SHORTEST 1 Hartlebury→Cheltenham Spa      interior stops [Droitwich Spa, Bromsgrove]
           MATCH-level WHERE none(stop IN n[..-1] WHERE stop.name = 'Bromsgrove')
                                                   0 rows (post-filter)
           the same predicate inline or in parentheses
                                                   [Droitwich Spa, Worcester Shrub Hill, Ashchurch]
docs     SHORTEST 1 Hartlebury→(b:Station)         8 rows
           post-filter length(p) % 2 = 0            4 rows
           pre-filter, inside the parentheses       8 rows; Droitwich Spa at length 4 through a
                                                    repeated node, so trail, not acyclic
derived  SHORTEST 1 (a:Station)-[:LINK]-+(b:Station)
                                                   79 rows: 72 ordered pairs and 7 closed trails
                                                   (Ashchurch 3, Bromsgrove 4, Cheltenham Spa 4,
                                                   Droitwich Spa 3, Worcester Foregate Street 3,
                                                   Worcestershire Parkway 3, WSH 3; none for
                                                   Hartlebury and Pershore)
derived  ALL SHORTEST, same pattern                108 rows
derived  shortestPath((a:Station)-[:LINK*]-(b:Station))
                                                   error 51N23
           with WHERE a <> b                        72 rows; allShortestPaths 90
derived  Hartlebury, Cheltenham Spa bound:
         shortestPath((h)-[:LINK*]-(c)) WHERE none(n IN nodes(p) WHERE n.name = 'Bromsgrove')
                                                   [Hartlebury, Droitwich Spa, WSH, Ashchurch, Cheltenham Spa]
derived  WSH, BMV bound:
           allShortestPaths(...) WHERE length(p) > 2
                                                   3 rows of length 3, notification 03N92
           shortestPath(...) WHERE length(p) > 2   1 row of length 3
           shortestPath(...) WITH p WHERE length(p) > 2
                                                   0 rows
           allShortestPaths(...) WHERE length(p) = 5
                                                   4 rows
derived  shortestPath((wsh)-[:LINK*]->(bmv))       no row; null under OPTIONAL MATCH
           (wsh)<-[:LINK*]-(bmv)                    length 2; allShortestPaths 2 rows
derived  WSH→BMV over -[:LINK]-{1,6}, SHORTEST 3 GROUPS
                                                   trail 2:2 3:3 4:1; walk 2:2 3:3 4:19; acyclic 2:2 3:3 4:1
           SHORTEST 4 GROUPS                        trail adds 5:4; acyclic has no fourth group
derived  MATCH REPEATABLE ELEMENTS p = SHORTEST 1 (h)-[:LINK]-{1,4}(h), h = Hartlebury
                                                   [Hartlebury, Droitwich Spa, Hartlebury]; default mode 0 rows
```

Two fixtures from Neo4j's runtime tests (`runtime-spec-suite`):

```
StatefulShortestPathTestBase, trail propagation
  graph    (x0:S:T)-[e1:R1]->(x1), (x0)-[e2:R2]->(x2), (x2)-[e3:R1]->(x3), (x3)-[e4:R2]->(x1)
  query    MATCH p = SHORTEST 1 (s:S)((n1)-[r1]->(n2)-[r2:R1]-(n3))+(t:T)
  trail    one path, x0 → x2 → x3 → x1 → x0, length 4, r1 = [e2, e4], r2 = [e3, e1];
           the same single path with SHORTEST 9999 GROUPS
  walk     with {1,5}: length 2, x0 -e1-> x1 -e1- x0
  acyclic  0 rows

ShortestPathTestBase, self-loops
  graph    one node with two self-loops
  allShortestPaths((x)-[*1..]->(y)) over all x, y     error: same start and end
  allShortestPaths((x)-[*0..]->(y))                   only [n]
```

The trail-propagation fixture is the one every design must get right. The shortest walk from
`s` back to `t` uses `e1` twice, so under trail semantics the answer is a path twice as long,
which a BFS never reports.

---

## 2. Why shortest paths are cheap, and where that stops

### 2.1 A shortest walk is a trail, under three conditions

`VLP_RESEARCH.md` Section 1 explains why trail semantics make enumeration expensive: trail
counts grow exponentially with the hop bound, and counting them is #P-complete. Shortest paths
escape this through one observation.

Take a walk from `s` to `t`, `s ≠ t`, whose every node and relationship satisfies the pattern's
per-element predicates. If it repeats a node, cutting out the cycle between the two visits
leaves a shorter walk from `s` to `t`. Every element of the shorter walk belongs to the longer
one, so it still satisfies the predicates, and its length is still at least 1 because `s ≠ t`.
So a *shortest* walk never repeats a node. It is a simple path, and therefore a trail. The
shortest walks between two distinct nodes are exactly the shortest trails, and a BFS, which
finds shortest walks, answers `shortestPath` and `allShortestPaths` exactly under Cypher's trail
semantics.

The argument needs three conditions, and each legacy restriction protects one:

- **Cutting a cycle must keep the walk in the pattern.** It does for `-[:T*]-`, `-[:A|B*]-`,
  and any predicate applied to every element alike; the theory calls such patterns *downward
  closed*. It does not for a pattern with fixed segments, `(a)-->(x)-[:T]-+(b)`, where the cut
  can remove the fixed hop, or for a two-relationship body, `((x)-[:A]->(y)-[:B]->(z))+`, where
  the cut can break the alternation. Legacy accepts a single relationship only.
- **The minimum must be at most 1.** With a minimum of 2, the cut can drop the length below
  it. Take `VLP_RESEARCH.md`'s two-node graph, `e1: A→B` and `e2: B→A`, and `(A)-[*2..]->(B)`:
  the shortest qualifying walk is A→B→A→B, length 3, which uses `e1` twice. No trail from A to B
  has length 2 or more, so the correct answer is no path, where a BFS over walks answers 3.
  Legacy caps the minimum at 1.
- **The endpoints must differ.** A closed walk from `s` back to `s` is itself the cycle, and
  cutting it leaves nothing. In an undirected graph `s -e- x -e- s` is a closed walk of length
  2 over one relationship; the shortest closed *trail* is a different and harder question.
  Legacy refuses it (error 51N23); GQL answers it.

When all three hold, trail semantics cost nothing. When one fails, the shortest trail can be
longer than the shortest walk, as the trail-propagation fixture shows.

### 2.2 The complexity map

The theory of regular path queries (RPQs) makes this precise. |A| is the size of the pattern's
automaton, |G| the size of the graph; the bounds are in data complexity (pattern fixed).

| Selector and mode | Cost | Algorithm |
|---|---|---|
| ANY SHORTEST, WALK | O(\|A\|·\|G\|) per source | BFS over the product of graph and automaton, one back-pointer per product node (PathFinder, Algorithm 1) |
| ALL SHORTEST, WALK | O(\|A\|·\|G\|), then output-linear delay | BFS keeping every predecessor at depth − 1: a DAG of all shortest paths (PMR paper, Theorem 5.6; PathFinder, Algorithm 2). Correct multiplicities need an unambiguous automaton |
| SHORTEST k, WALK | O(k·\|A\|·\|G\|) | layered BFS expanding each product node at most k times (PathFinder, Algorithm 3); Eppstein-style k shortest walks |
| SHORTEST k GROUPS, WALK | O(k·\|A\|·\|G\|) | at most k lengths per product node (PathFinder, Algorithm 4) |
| any selector, TRAIL / ACYCLIC / SIMPLE | NP-complete to decide existence, already for fixed patterns such as `(aa)*` or `a*ba*` | pruned exhaustive search |
| TRAIL, patterns in T_tract | shortest trail in polynomial time; polynomial-delay enumeration if and only if the pattern is in T_tract | Martens, Niewerth and Popp (Trautner) |

The tractable classes are nested: downward-closed languages ⊊ SP_tract ⊊ T_tract (Bagan,
Bonifati and Groz define SP_tract, there called C_tract; Martens, Niewerth and Popp define
T_tract). Every single-occurrence regular expression is in T_tract, and more than 99.8 % of 50
million logged RPQs are single-occurrence. The algorithms for those classes enumerate summaries
whose polynomial degree grows with the automaton: they settle complexity, and systems use
pruned search instead. GQL as a whole has P^NP[log]-complete data complexity, and NL-complete
without restrictors (Figueira, Lin, Peterfreund).

For Cypher the table collapses to three cases *(inference)*:

- Legacy `shortestPath` and `allShortestPaths`, and GQL `ANY SHORTEST` and `ALL SHORTEST` over
  a single quantified relationship with a minimum of at most 1 and distinct endpoints: a BFS,
  exact.
- Under the default TRAIL mode, `SHORTEST k` with k > 1, `GROUPS`, closed paths, minimums of 2
  or more, fixed segments and multi-hop bodies: a search that validates trails and may go past
  the BFS level.
- The same under `REPEATABLE ELEMENTS`: walks, polynomial again by the layered BFS.

### 2.3 Representing all shortest paths

The number of shortest paths between two nodes can be exponential in their distance: a chain of
n diamonds holds 2^n shortest paths of length 2n. The BFS *predecessor DAG* holds them without
listing them: every node reached at depth d keeps its edges from neighbours at depth d − 1.
Martens, Niewerth, Popp, Rojas, Vansummeren and Vrgoč (VLDB 2023) formalise such structures as
*path multiset representations* (PMRs). The PMR of all shortest matching paths is built in time
linear in the product graph; paths are enumerated from it with output-linear delay; counting and
uniform sampling are linear. Their headline: a union of path queries using only the shortest
selector "can be evaluated in linear time combined complexity when using PMRs". On WDBench
(364.6M nodes, 1.257B edges), Neo4j 4.4.12 timed out on all but 2 of 315 path-returning queries.

Counting is the other half. TigerGraph's GSQL treats a Kleene star as all shortest paths and
never lists them: "one need not enumerate the shortest paths as it suffices to count them in
order to maintain the multiplicities of bindings". The count is a dynamic programme over the
DAG, σ(v) = Σ σ(u) over its predecessors u. A `count(p)`, or an aggregate over rows that do not
read `p`, needs σ(t) only.

Enumeration still costs its output. LDBC SNB Interactive v2 dropped the old IC14, which
enumerated every shortest path between two persons and scored each, because enumeration "can be
prohibitively expensive on large scale factors", and replaced it with a cheapest-path query.

---

## 3. How Neo4j executes it

### 3.1 Legacy: `BiDirectionalBFS`

The interpreted and slotted runtimes call `ShortestPathBFSFactory.create`. The pipelined runtime
is closed source, but `BiDirectionalBFS` has a `@CalledFromGeneratedCode` factory, so it
presumably runs the same code. The old `org.neo4j.graphalgo.impl.path.ShortestPath`, which
"starts from both ends and goes one relationship at the time, alternating side", is no longer
used by Cypher.

The algorithm (`BiDirectionalBFSImpl.java`, `BFS.java`):

- Two BFSs, one per endpoint. The target side walks the reversed direction with a reversed
  relationship predicate, so predicates on `startNode(r)` and `endNode(r)` stay correct.
- Each side keeps `currentLevel` and `nextLevel`, hash sets of node IDs, and `pathTraceData`, a
  map from node ID to one `(relationship, previous node)` step or a list of them. A node is
  visited at most once per side.
- **Side choice.** "As we want to explore as few nodes/relationships as possible, we always
  expand the BFS that sees the fewest amount of nodes." The code is
  `bfs1.currentLevel.size() > bfs2.currentLevel.size() ? bfs2 : bfs1`, ties to the source side.
  One call expands one full level. Degrees are never read.
- **Depth.** `if (depth++ == maxDepth)` counts levels over both sides, so the pattern's upper
  bound limits the total length.
- **Meeting.** A newly discovered node in the other side's current frontier. That frontier is
  the other side's deepest level, which is where the first meeting must fall (Section 5.3).
- **Variants.** `SinglePathBFS` (shortestPath without fallback) keeps one predecessor per node
  and stops at the first meeting. `EagerBFS` (allShortestPaths) finishes the level, intersects
  the two frontiers and records every same-level predecessor. `LazyBFS` (shortestPath with a
  fallback) stops at the first meeting and resumes only if asked for more paths, because the
  path predicate may reject the first.
- **Output.** `PathTracingIterator` emits, for each meeting node, the cartesian product of the
  source-side and target-side predecessor chains.
- Node and relationship filters apply during expansion, endpoint filters before the search. The
  BFS object is reused across input rows (`resetForNewRow`).

Special cases (`ShortestPathBFSFactory.java`): source = target with a minimum of 0 gives the
zero-length path; source = target undirected goes to dedicated `shortestloop` cursors (the
shortest closed trail; for walks, a self-loop or a back-and-forth hop; nothing for acyclic);
source = target directed runs the ordinary bidirectional BFS, which finds the shortest directed
cycle.

### 3.2 GQL: planning

1. Uniqueness predicates are inserted inside the selective pattern and converted to a path mode:
   node uniqueness → Acyclic, relationship uniqueness → Trail, none → Walk
   (`TraversalPathMode.scala`).
2. `expandSolverStep.produceStatefulShortestLogicalPlan` chooses `Into` when the far endpoint is
   bound and `All` otherwise; under `All`, predicates on the end node are inlined into the final
   state. The default `cardinality_heuristic` prefers `Into` when the input cardinality is at
   most 1. ANY maps to Shortest(k), ALL SHORTEST to ShortestGroups(1).
3. `LimitRangesOnSelectivePathPattern` turns quantifier bounds of 100 or more into an unbounded
   loop plus `size(group)` predicates, which caps the automaton's size.
4. `ConvertToNFA` builds the automaton: one state per node variable, anonymous states for
   variable-length hops. Node predicates sit on states and are checked on entry; relationship
   transitions carry types, direction and a relationship predicate. A QPP unrolls its body `min`
   times, then loops back (unbounded) or unrolls up to `max`, with a skip edge when `min` is 0.
   What cannot be inlined, such as whole-path predicates and subqueries, becomes
   `nonInlinedPreFilters`.
5. `StatefulShortestToFindShortestRewriter`: "Re-writes StatefulShortestPath to FindShortestPath
   where possible, as benchmarks have shown the latter is faster than StatefulShortestPath."
   Possible when k = 1 (`SHORTEST 1` or `ALL SHORTEST`), the target is bound, the pattern is one
   variable-length relationship or one-relationship QPP with a minimum of 0 or 1, every
   predicate inlines per step, and there are no node group variables.

The manual's operator table: `ShortestPath` for the legacy functions and the rewritten case;
`StatefulShortestPath(Into)` when both endpoints are known and k > 1, GROUPS or several
relationship patterns are involved; `StatefulShortestPath(All)`, a "unidirectional BFS ... from
a source node to all nodes matching the target node conditions".

### 3.3 GQL: the product-graph search (PPBFS)

The code is under `runtime-util/.../traversal/ppbfs/`. The javadoc calls `PGPathPropagatingBFS`
"the root of the product graph PPBFS algorithm" and points to an internal page. No public paper
or talk describes it; the code is the only source.

**State.**

- A product node is a `NodeState(nodeId, NFA state)`, unique per pair, held in `FoundNodes` as
  one map per BFS level from node ID to states. The code notes the lookup is "linear w.r.t the
  depth of the bfs"; issue neo4j#13966 measures the cost on deep searches.
- A product edge is a `TwoWaySignpost`: a `RelSignpost` (one relationship, length 1) or a
  `NodeSignpost` (a juxtaposition, length 0). It carries a bitset of the lengths at which its
  forward node is reached through it from the source, with a validated bit in trail and acyclic
  modes, and `minTargetDistance`, the distance from its previous node to a target.
- A `NodeState` holds its source and target signposts, its lengths, a `sourceDistance` ("not
  necessarily a trail length"), `remainingTargetCount` = k, and `isTarget`.

**Main loop.**

1. Level 0 floods zero-length juxtapositions from (source, start state); in Into mode it also
   floods backwards from (target, final state).
2. Each level stops at the pattern's maximum length, expands one frontier (the one with fewer
   distinct data nodes, ties forward), and runs the propagator.
3. Once a target has gained a source length equal to the current depth, `PathTracer` emits its
   paths of exactly that length.
4. `SHORTEST k` decrements k per path that passes the non-inlined pre-filters; GROUPS decrements
   once per length that yielded a path.
5. Into stops when the target is saturated; All when every known target is saturated and the
   frontier is empty.

**Expansion.** A product node is expanded at most once per direction, and one adjacency scan per
data node serves all its NFA states. Every traversal adds a source signpost with the current
length, even to nodes already found, so nodes accumulate lengths without being re-expanded.
Adjacency and predicate results are cached for the whole search.

**Why lengths propagate.** BFS depths are walk distances. Under trail semantics the shortest
walk to an intermediate product node may reuse a relationship, so the shortest trail can reach
it at a non-BFS depth. When a node is found to reach a target in t hops, its other source
lengths l are queued as (l, t) pairs, ordered by (l + t, l, node, state); at depth d each pair
with l + t ≤ d is pushed along target signposts to the target, which becomes a target at depth
d. Longer lengths are enumerated only over the part of the graph already known to reach a
target.

**Tracing.** A DFS from the target back to the source over signposts with matching lengths. In
trail mode a `DepthPresenceTracker` maps each relationship ID to the stack depths holding it;
`validate()` rejects repeats and marks lengths validated; on backtrack, lengths never validated
are pruned; `canAbandonTraceBranch` cuts branches holding a duplicate relationship once the
nodes involved are validated. Acyclic mode does the same with node IDs; walk mode tracks nothing.
The tracer runs to exhaustion even after k paths, since it also does bookkeeping.

Memory is charged to a scoped tracker. The number of traced paths can grow exponentially on
adversarial graphs *(inference from the algorithm)*.

### 3.4 Adjacent rewrites

`pruningVarExpander.scala` turns a variable-length expand whose relationship variable is unused,
under a *distinct horizon* (a `DISTINCT`, or an aggregation whose aggregates are all distinct or
`min(length(p))` / `min(size(r))`), into `BFSPruningVarExpand` when the minimum is at most 1, or
into `PruningVarExpand` (a DFS with pruning) when the maximum is bounded. `BFSPruningVarExpand`
"will not find all paths but is guaranteed to find all distinct end-nodes" and can emit the
depth, which rewrites `min(length(p))` into it: a shortest distance computed without any
shortest-path syntax. `VLP_RESEARCH.md` Section 4.1 covers the rest.

### 3.5 Weighted paths live outside Cypher

Cypher has no weighted shortest path. The Graph Data Science library runs Dijkstra
(source-target and single-source) on a projected graph with a binary heap
(`HugeLongPriorityQueue`), a predecessor map and a visited bitset, single-threaded. A* is the
same Dijkstra ordered by cost plus haversine distance. Yen's k shortest paths uses Dijkstra with
node and relationship filters for spur paths, parallel spur tasks and "Lawler's modification".
Delta-Stepping is parallel, with atomic tentative distances and bucket fusion; Bellman-Ford
covers negative weights. APOC's `apoc.algo.dijkstra` calls the traversal framework's Dijkstra
(`DijkstraBidirectional` when no path count is given) on the live graph. The LDBC BI reference
implementation for Neo4j calls `gds.shortestPath.dijkstra` for Q19.

---

## 4. What the other engines do

| Engine | Bound pair | Many pairs, unbound end | Paths held as | Notes |
|---|---|---|---|---|
| Memgraph | bidirectional BFS (`STShortestPathCursor`) | single-source BFS; `*WSHORTEST` priority queue keyed by (vertex, depth) when bounded; `*KSHORTEST` "lazy-evaluated Yen's algorithm" | hash maps, row at a time | filter lambdas `(r, n [, p, w])` per expansion; no `shortestPath()` function |
| FalkorDB, Rust rewrite (unreleased) | bidirectional BFS: "Expands the smaller frontier one full level at a time, alternating" | `allShortestPaths`: BFS with predecessor maps, then DFS | hash maps | `shortestPath` only in WITH/RETURN; minimum 0 or 1; the shipped C engine uses a GraphBLAS masked BFS |
| Kùzu, forks (Ladybug) | one-directional BFS per source | levels split into frontier morsels; sparse frontier until 1,000 nodes are visited, then dense atomic arrays; early stop when ≤ 100 masked destinations are all reached | block-allocated parent lists; factorized output; per-node counts for count-only queries | default maximum depth 30; relationship predicates on neighbour chunks; Roaring-bitmap source and destination masks; weighted variant is Bellman-Ford-style |
| DuckPGQ | 512-lane MS-BFS over the (src, dst) pairs of a vector | same | two \|V\|×512 int64 parent arrays per call | CSR built per query by UDFs; ANY SHORTEST only; cheapest path by batched Bellman-Ford |
| NebulaGraph | bidirectional BFS | `BatchShortestPath` splits the start and end sets into a grid of blocks, one bidirectional BFS each, across threads | | GetNeighbors RPCs to storage |
| TigerGraph | frontier relaxation in supersteps | accumulators, e.g. a per-vertex `MapAccum<source, MinAccum>` in BI Q19, with threshold pruning | counts | a Kleene star means all shortest paths, counted, not listed |
| Umbra (LDBC BI) | bidirectional batched Dijkstra in recursive SQL | | | expands the 1,000 cheapest open entries per round; stops expanding an entry past half the best meeting cost |
| Apache AGE (June 2026) | BFS with a predecessor multiset | | DAG, enumeration capped at 1,000,000 paths | functions `age_shortest_path` and `age_all_shortest_paths` |
| SQL Server | repeated joins over tempdb worktables | | graph-path aggregates | `SHORTEST_PATH` returns "any one" unweighted path; the end-node filter applies after paths to all nodes are built |
| Spanner Graph, Oracle PGX | not documented | | | ANY SHORTEST and ANY CHEAPEST (Spanner); ANY, ALL SHORTEST, SHORTEST k, CHEAPEST k (PGX) |
| Amazon Neptune | Database: `shortestPath()` "not currently supported" | Analytics: BFS, Bellman-Ford and delta-stepping procedures | | |

Four lessons.

**Bidirectional search is what makes bound pairs fast.** Neo4j, Memgraph, FalkorDB's rewrite,
NebulaGraph and Umbra use it; Kùzu and DuckPGQ do not. DuckPGQ's CIDR paper says Neo4j
"completes the workload ... thanks to its bi-directional path-finding algorithm", and that its
own "SIMD-friendly MS-BFS ... is uni-directional, but is able to generally beat Neo4j still" on
16K pairs over LDBC Person–Knows.

**Dense per-search state is the common failure.** Kùzu allocates and initialises state for every
node per source: on SF100 a 2-hop query spent about 400 ms of its 900 ms there (kuzu#4941), and a
path query exhausted 150 GB (kuzu#4459). DuckPGQ's parent arrays are about 8 KiB per vertex per
call. v3's `PathReachTable` already applies the fix to the distinct search: state sized to the
ball, not the graph.

**Predicates on intermediate nodes belong in the search.** In July 2026 Ladybug fixed all four
of Kùzu's SHORTEST modes: "The filter was only checked during the output/path-reconstruction
phase, not during the BFS traversal itself". A filter applied after the BFS drops the shortest
path and reports nothing, where the correct answer is a longer path avoiding the filtered nodes.

**A frontier framework beats row-at-a-time traversal by an order of magnitude.** Kùzu 0.7.0
replaced its sparse, single-threaded recursive join with a design that "mimics those in parallel
graph analytics systems, such as Ligra, Pregel, or GraphChi". Single-source shortest-path lengths
on LDBC1000 Person–Knows (3.2M nodes, 202M edges) went from 56.4 s to 3.33 s on one thread, and
0.32 s on 32.

---

## 5. Making one pair fast

### 5.1 Bidirectional search is sublinear on real graphs

A BFS from `s` to `t` at distance d explores the ball of radius d around `s`. A bidirectional BFS
explores two balls of radius about d/2. Where balls grow exponentially, b^d against 2·b^(d/2) is
a square root.

The evidence that real graphs behave this way:

- Borassi and Natale (KADABRA, ESA 2016) prove that a balanced bidirectional BFS touches
  m^(1/2 + o(1)) edges with high probability on the configuration model and on rank-1
  inhomogeneous random graphs (Chung–Lu, Norros–Reittu) whose degree distribution has a finite
  second moment, and m^((4 − β)/2 + o(1)) for power laws with 2 < β < 3.
- Bläsius, Freiberger, Friedrich, Katzmann et al. (ICALP 2018) prove a sublinear bound on
  hyperbolic random graphs.
- Bläsius and Fischbeck (ESA 2022, TALG 2024) measured 2,740 real networks, 100 random pairs
  each, balancing sides by the degree sum of each layer. The cost exponent is about 0.5, so about
  √m edges, unless the network is both *local* and *homogeneous*; then it is about 1. Road
  networks and meshes are local and homogeneous. Social, web, citation and biological networks
  are not.
- Haeupler, Hladík, Rozhoň, Tarjan and Tětek (SOSA 2025) show that bidirectional Dijkstra,
  alternating one relaxation per side and stopping on the μ rule, is instance-optimal in edges
  examined, and optimal for unweighted BFS up to an O(Δ) factor, which is tight.
- On roads the gain is a constant: bidirectional Dijkstra scans 4.9M vertices against 9.3M for
  Dijkstra on Western Europe (route-planning survey, Table 1).

Side by side, from different papers and machines: bidirectional BFS takes 7.0 ms on Hollywood
(1.1M nodes, 114M edges) and 9.7 ms on Indochina (7.4M, 194M), where plain BFS takes 1.2 s and
1.5 s.

### 5.2 Which side to expand

Expanding a level costs the sum of its frontier's degrees, not its node count. Bläsius and
Fischbeck balance by that sum, and KADABRA's analysis assumes it. Neo4j balances by node count
(Section 3.1). They differ on hubs: a 3-node frontier holding a node of degree 10^6 costs a
million edge reads, and a node-count rule expands it before a 100-node frontier of degree 10.
The degree sum costs one read per frontier node, which in v3 is the size of the node's span in
each part that holds it (Section 10.1).

Balancing after every node instead of every level does better still in theory: Cerf et al.
(2024) reach n^(1/2 + o(1)) on Chung–Lu graphs and geometric inhomogeneous random graphs with
power-law exponent τ between 2 and 3, where level balancing reaches n^((4 − τ)/2 + o(1)). It
breaks the first-meeting stopping rule and needs the μ rule instead (Section 5.3).

### 5.3 When to stop, and how many paths there are

**One shortest path.** Expand whole levels, one side at a time. Suppose the forward side has
completed depth k and the backward side depth J, and no node has been reached by both. Then the
distance d is at least k + J + 1: on a path of length k + J or less, the node at position
max(0, d − J) would be within k of `s` and within J of `t`, so both sides would have reached it.
While expanding forward level k + 1, any edge from a forward frontier node to a node the
backward side has reached closes a path of length at most k + 1 + J, which equals the lower
bound. That path is shortest, so the search stops at the first such edge. The node it reaches
sits exactly J from `t`, in the backward side's deepest level, which is why Neo4j tests only the
other side's current frontier.

**All shortest paths.** Finish the level of the first meeting and collect every meeting edge
(v, w), with v at forward depth k and w at backward depth L − k − 1 = J. Every shortest path
crosses from forward depth k to backward depth J through exactly one such edge. So the number of
shortest paths is Σ σ(s, v)·σ(w, t) over the meeting edges, where σ counts shortest paths
(KADABRA samples paths this way), and the paths are, per meeting edge, every forward chain from
`s` to v times every backward chain from w to `t`. Neo4j decomposes by meeting *nodes* instead,
which is equivalent; mixing the two decompositions double counts.

**Pitfalls.**

- Alternating one node at a time breaks the first-meeting rule: the approximate variant of Cerf
  et al. returns paths up to one hop too long. Keep the best length μ found and stop when the
  completed depths satisfy k + J + 1 ≥ μ.
- A weighted search cannot stop at the first meeting at all (Section 8.2).
- A directed pattern's backward side reads in-edges; an undirected pattern reads both lists on
  both sides. Parallel relationships give distinct `allShortestPaths` rows.
- A pair with no path ends when either frontier empties, so with degree-sum balancing the work is
  about the size of the smaller component *(inference)*. LDBC BI shows the effect: Umbra answers
  Q20a (no path) 2 to 42× faster than Q20b by exhausting the smaller component.

### 5.4 Per-search state

A point query that costs √m cannot afford O(n) setup: a parent array for 100M nodes is 800 MB,
and writing it costs more than the search it serves. Two engines in Section 4 pay it anyway. The
known pattern:

- hash-based visited and parent tables sized to the ball, reused across rows;
- epoch stamps instead of clearing, so reuse costs nothing (`PathExplorator::KeySet` and
  `PathReachTable` already work this way);
- a dense layout only once a frontier is large: Ligra switches at |E|/20 edges, Kùzu at 1,000
  visited nodes.

### 5.5 Direction optimisation, SIMD, prefetching

Beamer, Asanović and Patterson (SC 2012) switch a BFS from top-down (each frontier node pushes to
its neighbours) to bottom-up (each unvisited node scans its in-edges and stops at the first
parent in the frontier) when the frontier's edges exceed the unvisited edges divided by α, and
back when the frontier falls below n/β. The paper uses α = 14 and β = 24, GAPBS 15 and 18, and a
poorly chosen α still runs within 15-20 % of peak. They report 3.3-7.8× on synthetic graphs and
2.4-4.6× on social graphs against a strong baseline. In a bidirectional point search the
frontiers meet while small, so it rarely triggers; it pays for one-to-all searches, batches, and
pairs with no path inside a giant component *(inference)*. `VLP_RESEARCH.md` Section 4.3 covers
sparse and dense frontiers.

SIMD helps a single-source BFS little: SlimSell is up to 33 % faster than a tuned Graph500 code,
a vectorised hybrid BFS on Xeon Phi 33 %. Software prefetching of indirect accesses gives 1.3× on
average (Ainsworth and Jones, CGO 2017). The large data-parallel win is the bit-parallel
multi-source search of Section 6.2.

### 5.6 What one query costs at scale

Without an index, on one thread, bidirectional BFS answers a distance query in 427 ms on Twitter
(42M nodes, 1.5B edges), 535 ms on Friendster (66M, 1.8B) and 356 ms on uk2007 (106M, 3.7B)
(highway cover labelling paper, Table 2). It computes all shortest paths in 4.8 s, 3.6 s and
5.3 s (QbS paper, Table 2). At LDBC scale the bar is lower and tighter. The audited IC13 query,
the unweighted shortest-path length between two persons, averages 254 µs at SF100 and 287 µs at
SF1000 on GraphScope Flex (2024, P99 802 µs), and 177 µs and 349 µs on Huawei GES (2025). The
SF100 Person–Knows graph has 448K persons and 19.9M edges. Neither audit describes the algorithm.

---

## 6. Making a batch fast

### 6.1 The pair graph

Joins produce batches. `MATCH (a:X), (b:Y) MATCH p = shortestPath((a)-[*]-(b))` asks for every
pair, and a legacy query over bound variables asks for one pair per row. Dong, Li, Gu and Sun
(Orionet, SPAA 2025) model a batch as a graph over the queried vertices, with an edge per
requested pair. A single-source search from each vertex of a *vertex cover* of that graph
answers every pair; it was up to 2.55× faster than searching from every source. For X × Y the
pair graph is complete bipartite and the cover is the smaller side: 1,000 × 1,000 pairs become
1,000 searches instead of a million.

The same paper warns against a single strategy. Batched bidirectional search won about 80 % of
their tests and was 2.20× faster on average on clique-shaped batches, but on social and web
graphs bidirectional search between distant pairs could lose to one optimised full SSSP. Neo4j
decides by cardinality: `ShortestPath` or `StatefulShortestPath(Into)` for few estimated pairs,
`StatefulShortestPath(All)` for many.

### 6.2 Multi-source BFS against per-pair search

`VLP_RESEARCH.md` Section 4.2 explains MS-BFS (Then et al., VLDB 2014): ω searches share one scan
of each adjacency list, their state held in ω-bit words per node. The paper reports 12.1-88.5×
over running textbook or direction-optimising BFSs side by side on 60 cores. MS-PBFS (Kaufmann et
al., EDBT 2017) saturates 60 cores with one 64-source batch, where sequential MS-BFS needs 7,680
sources. Kùzu's multi-source morsels lost to its per-source policy below 32 sources and won by
1.4-4.4× once the 64 lanes were full.

The 12-88× is against independent *full* BFSs. For point-to-point shortest paths the competitor
is a bidirectional search per pair, and the comparison changes *(inference, rough numbers)*. A
bidirectional search costs about 2√m edge reads on a small-world graph (Section 5.1). A 64-lane
pass costs up to m word operations, because a one-directional search out to a small-world
distance reaches most of the graph. On a 10M-edge graph:

| Batch | Per-pair bidirectional | 64-lane MS-BFS from one side | Cheaper |
|---|---|---|---|
| 16,000 random pairs, 16,000 distinct sources | 16,000 × 6,300 ≈ 1.0 × 10^8 | 250 passes × 10^7 ≈ 2.5 × 10^9 | per pair |
| 1,000 × 1,000, every pair | 10^6 × 6,300 ≈ 6.3 × 10^9 | 16 passes × 10^7 ≈ 1.6 × 10^8 | MS-BFS |

What decides is the pair graph's density: pairs per distinct source, or per distinct target.

### 6.3 Lengths first, paths after

Paths are what make batches expensive. Kùzu's multi-source morsels preallocate 536 bytes per node
per morsel to return paths: 128 GB for two morsels on a 120M-node graph, which ran out of memory,
against 21 GB for lengths only. The alternative *(inference)*: compute distances bit-parallel,
mark a lane done when its target is reached, retire the batch when every lane is done, then
rebuild each requested path from the distances (Section 10.3) or with a bidirectional search
bounded by the known length. SAMS (Then et al., VLDB 2017) runs the same lanes over several
snapshots at once, up to 100× faster than one snapshot at a time, which is the shape of a path
query asked across commits.

### 6.4 Threads

v3 runs a query on one thread, so this is future work. For one source, thin levels cap
parallelism: Kùzu measured 11.9× on the widest level but 4.8× overall on 32 threads. Its hybrid
policy (nTkS: up to k sources active, threads take work from any of their frontiers) reached
11.0-14.0× with 8 sources and 11.5-15.5× with 64, at 95-99 % CPU. DuckPGQ's UDF design ties
parallelism to the number of pair vectors; below about 60 calls, "MS-BFS actually runs in
single-threaded mode" (Ren, 2024 thesis). For millisecond point queries, the literature runs one
thread per query and parallelises across queries.

---

## 7. What can be precomputed or laid out better

### 7.1 Pairs with no path

A search for an unreachable pair explores a whole component. Connected-component IDs answer it
in O(1). They can be computed over all edges and still serve every pattern: a typed or directed
pattern walks a subgraph, so two nodes in different weak components have no path under any
pattern, though pairs connected only through other types slip through. ConnectIt computes the
connectivity of Hyperlink2012 (3.5B nodes, 128B edges) in 8.2 s on 72 cores. For directed
patterns, a strongly-connected-component condensation plus a reachability index answers most
queries in constant time (O'Reach; `VLP_RESEARCH.md` Section 5.5 covers the family). A component
labelling stays a sound "no path" test across deletions, which can only split components, and
union-find keeps it current under insertions at almost no cost *(inference)*.

### 7.2 Distance labels

`VLP_RESEARCH.md` Section 5.2 describes pruned landmark labelling (PLL): 15.6 µs per query on
Hollywood, but 15,164 s to build and 12 GB of labels for a graph the highway-labelling paper
lists at 430 MB. PLL did not finish within a day, or ran out of memory, on Orkut, Twitter and
Friendster.

Highway cover labelling (Farhan, Wang, Lin and McKay, EDBT 2019) scales. It picks about 20
landmarks and stores each node's distances to them. A query takes the upper bound through the
landmarks, then runs a bidirectional BFS on the graph *with the landmarks removed*, cut off at
that bound. Removing the hubs is what keeps the residual search small.

| Graph | Build (parallel) | Labels | Query | Bidirectional BFS |
|---|---|---|---|---|
| Twitter, 42M nodes, 1.5B edges | 1,380 s (133 s) | 2.8 GB | 1.42 ms | 427 ms |
| Friendster, 66M, 1.8B | 2,229 s (135 s) | 5.2 GB | 1.09 ms | 535 ms |
| ClueWeb09, 2B, 8B | 28,124 s (4,236 s) | 9 GB | 0.31 ms | |
| uk2007, 106M, 3.7B | | | 11.8 ms | 356 ms |

QbS (SIGMOD 2021) applies the idea to all shortest paths. It returns the shortest-path graph, the
DAG of Section 2.3, in 164 ms against 4,818 ms on Twitter and 12 ms against 3,600 ms on
Friendster, and in 480 ms on ClueWeb09 where bidirectional BFS did not finish. Its labels take
0.78 GB and 31.4 GB, built in parallel in 200 s and 1,819 s. Answers are exact, and paths come
from the landmarks' BFS trees.

Landmark *lower* bounds (ALT) prune little on small-world graphs: distances are 3 to 8 hops and
the bounds 0 to 2 *(inference)*. Even on roads, ALT gains over bidirectional Dijkstra are minor
for a noticeable fraction of queries (route-planning survey, Section 2.2). Highway labels use the
upper bound, which is the useful direction.

### 7.3 Indexes on a versioned graph

Inserting edges can only shorten distances, and deleting them can only lengthen them. An index
built at commit V gives valid *upper* bounds on a later commit that only inserted edges, and
valid *lower* bounds on one that only deleted edges *(inference; `VLP_RESEARCH.md` Section 5.2
makes the pruning half of this argument)*. After mixed changes it can order a search but not cut
it, and a rebuild belongs with compaction. Incremental PLL (Akiba, Iwata, Yoshida, WWW 2014)
absorbs insertions in milliseconds and answers distances at past times from one index. BatchHL
(SIGMOD 2022) maintains highway labels under batched insertions and deletions at billion-edge
scale.

### 7.4 Layout

A traversal's cost per expanded node is its random reads. In v3 a node's edges live in its owner
part and in every later part that patched it (`VLP_RESEARCH.md` Section 6.3), so an expansion or
a degree read costs one lookup per part holding the node *(inference)*. A merged CSR per
committed version, or compaction that keeps the part count low, removes that factor. DuckPGQ
builds a CSR per query, which is cheap next to heavy path work and significant for one pair. In
BYO, an updatable B-tree container runs 1.22× slower on average than an optimised static CSR
across 10 algorithms.

Reordering node IDs by degree helps traversals on skewed graphs. Degree-based grouping (DBG,
IISWC 2019) averages 16.8 % over the original order excluding the reordering cost (Gorder 18.6 %,
HubCluster 11.6 %, Sort 8.4 %) and 6.2 % net of it, where Gorder loses 41.3 % on PageRank. DBG
pays for itself after 8 SSSP traversals. Balaji and Lucia (IISWC 2018) show the gain depends on
how scattered the hubs are in the input order. Immutable parts make reordering a build cost
shared by every later query, at the price of a map between internal and user-visible IDs
*(inference)*. Ligra+ byte codes halve the space and run 14 % faster in parallel.

---

## 8. Weights, and k paths

### 8.1 Dijkstra engineering

Lazy Dijkstra pushes a new entry instead of decreasing a key and skips stale entries on pop; it
beat every decrease-key variant on road networks (Chen et al., UTCS TR-07-54). Implicit 4-ary
heaps do well. Fibonacci heaps run about 3× slower on the USA road graph, and pairing heaps are
the only Fibonacci relative that competes (Larkin, Sen and Tarjan). For integer weights at most
C, bucket queues run in O(m + n log C) and work well in practice. The LDBC weights are small
integers: IC14 v2 uses max(round(40 − √x), 1), BI Q19 round(40 − √x) floored at 1, BI Q20
|Δ classYear| + 1, so one 41-bucket queue serves all three. BI Q15's 1/(score + 1) needs a heap
*(inference from the specifications)*.

Universal optimality (Haeupler, Hladík, Rozhoň, Tarjan and Tětek, FOCS 2024 best paper): Dijkstra
with a working-set heap orders vertices by distance within a constant factor of optimal on every
graph. Tune the heap; keep the algorithm.

### 8.2 Bidirectional Dijkstra and hop bounds

Bidirectional Dijkstra keeps μ, the best s–t distance through any edge joining the two searches,
and stops when the smallest keys of the two queues add up to at least μ. On Western Europe it
takes 1.2 s against 2.2 s unidirectional. In parallel, Orionet's bidirectional search was 2.9×
faster than GraphIt and 6.8× faster than MBQ over 14 graphs, and bidirectional A* 4.4× and 6.2×.

A weighted search under a hop bound needs state per (vertex, depth): the cheapest path with at
most 5 hops can reach an intermediate node by a costlier route with fewer hops than the cheapest
route to it. Memgraph's `*WSHORTEST` keys its queue by (vertex, depth) when an upper bound is set.

### 8.3 Preprocessing for road networks

On Western Europe (route-planning survey, Table 1): contraction hierarchies build in 5 minutes,
take 0.4 GiB and answer in 110 µs; customizable route planning takes an hour of
metric-independent preprocessing, answers in 1.65 ms and applies a new metric in 0.37 s on 12
cores; hub labels take 37 minutes and 18.8 GiB and answer in 0.56 µs; PHAST computes one-to-all
more than 10× faster than Dijkstra sequentially and up to 1,000× on a GPU. All of them rely on
small separators, which social graphs lack, and weights computed per query (BI Q15) defeat any
metric-dependent preprocessing *(inference)*. They matter only for road or logistics graphs.

### 8.4 Parallel single-source search

Δ-stepping buckets tentative distances by width Δ and relaxes a bucket's light edges in parallel.
ρ-stepping (Dong, Gu, Sun and Zhang, SPAA 2021) is 1.3-2.5× faster than prior implementations on
five social and web graphs, and Δ*-stepping at least 14 % faster on two road graphs. GBBS runs
weighted BFS on Hyperlink2012 in 58.1 s on 72 cores. Bellman-Ford-style rounds (Kùzu's weighted
variant, TigerGraph, DuckPGQ's cheapest path) parallelise without a priority queue but relax the
same node many times. A 2026 study warns that synthetic uniform weights, the usual benchmarking
practice, can invert algorithm rankings.

### 8.5 k shortest paths

Eppstein represents the k shortest *walks* implicitly in O(m + n log n + k). Yen finds the k
shortest *simple* paths with one spur search per vertex of each path found, O(Kn(m + n log n)).
Hershberger, Maxel and Suri gain up to a factor Θ(n) with replacement paths. Kurz and Mutzel's
sidetrack-based algorithm is about an order of magnitude faster on road networks. PeeK (Feng et
al., SC 2023) observes that "top K shortest paths only cover a meager portion of the original
graph, e.g., less than 0.001% on a Twitter graph for K = 128". It prunes every vertex and edge
whose distance from `s` plus distance to `t` exceeds a bound, compacts the graph and runs Yen on
the rest: 5.1× faster than the previous best at K = 8 and 28.8× at K = 128 on 32 threads, with
runtime growing only 1.1× from K = 2 to K = 128. Under regular constraints, Yen over the product
graph works for downward-closed patterns and, with derivative automata, for T_tract trails.

PathFinder's layered BFS answers `SHORTEST k` and `GROUPS` over walks in O(k·|A|·|G|). It can be
slower than `ALL SHORTEST` because it explores longer paths.

### 8.6 Recent theory

Duan, Mao, Mao, Shu and Yin (STOC 2025 best paper) solve directed single-source shortest paths in
O(m log^(2/3) n), breaking the sorting barrier by not sorting vertices; a February 2026 paper
improves this to O(m √(log n · log log n)) on sparse graphs. An implementation study found
Dijkstra 3-4× faster on graphs up to 10^7 vertices and put the crossover far beyond any real
graph. Negative weights reached O(m log^8 n log W) (Bernstein, Nanongkai, Wulff-Nilsen), about
six log factors less in a follow-up, and m^(1 + o(1)) for real weights in 2026; no
implementation is known to beat Bellman-Ford-style code, and Cypher's `shortestPath` has no
weights. None of this changes what an engine should build today.

---

## 9. What the benchmarks ask for

**LDBC SNB Interactive.** IC13 asks for the unweighted shortest-path length between two persons
over Person–knows–Person; audited means are 177-349 µs at SF100-SF1000 (Section 5.6). IC14 v1
enumerated all shortest paths and scored each; v2 replaced it with "any cheapest path". Audited
IC14 means: 8.5, 17.9 and 22.4 ms at SF100, 300 and 1000 on GraphScope Flex; 6.1 and 23.7 ms at
SF100 and SF1000 on Huawei GES.

**LDBC SNB BI** (Szárnyas et al., PVLDB 16(4)) has three weighted path queries. Q15 weights an
edge 1/(interaction score + 1), counting only forums created in a date range, so its weights
depend on the parameters. Q19 weights come from interaction counts between persons of two cities,
Q20 weights from the class years of universities two persons attended. Precomputed weights are
allowed if maintained under updates, and TigerGraph and Umbra precomputed Q19's and Q20's. The
paper names bidirectional search, multi-source batching (Q19) and landmark labelling as remedies.

- Umbra used "a sophisticated bidirectional, sampling variant of Dijkstra's algorithm" in
  recursive SQL, 3,000 characters for Q15. Its Q20 checks reachability with a BFS, then expands
  the 1,000 smallest tentative distances per round from both endpoint sets and drops entries
  past half the best path found. At SF100 it ran Q19 in 0.03 s and Q20b in 0.08 s.
- TigerGraph's audited SF10000 run (48 machines, April 2023) took 39.1 / 110.4 s for Q15a/b,
  9.1 / 9.7 s for Q19a/b and 1.8 / 2.0 s for Q20a/b, after 915 s precomputing Q19's weights. Its
  GSQL is frontier-based label correcting with an upper-bound cutoff. TuGraph's SF30000 audit
  (72 machines, December 2023) took 61.5 / 110.7 s, 20.8 / 20.5 s and 7.7 / 7.3 s.
- Q15, whose weights cannot be precomputed, is 3 to 11× slower than Q19 in both audits. Building
  the weighted graph costs more than the search *(inference)*.

**Graph500.** The June 2026 BFS list is led at 2-4 × 10^5 GTEPS by machines with thousands of
nodes. Fugaku's team credits a partitioned, compressed graph, skipping unnecessary traversal and
dropping irrelevant vertices in preprocessing. None of it applies to millisecond point queries.

---

## 10. Consequences for TuringDB

### 10.1 What v3 already has

- `db.explore_paths` and `PathExplorator` (`PLAN.md`): a depth-first trail enumerator over the
  immutable parts, with `end_column` (a per-row bound end), `end_nodes` (an end set shared by
  every seed), `ends_on_seed` (a cycle back to the seed), `distinct` (a bit-parallel
  reachability search over 64 seeds, exact up to a minimum of 1), the `hop` region and
  `hop_labels`, `edge_types`, direction and the carry set. It writes `PathTrie` handles, which
  `db.expand_path` and `db.make_path` (with `reversed_paths`) turn into lists and named paths. It
  reads the change's pending edges and skips tombstoned ones.
- `PathTargetIndex`: a multi-source BFS from up to 64 targets per word over the reverse of the
  walk's direction, stored as an open-addressing table or dense per-level words chosen by
  `planBatch`, with a uint8 hop count per (node, target): 255 unreached, 254 the farthest level.
  `PathDistanceIndex` runs the same search from a label set or a list of ends, one byte per node.
  `PathReachTable` is the distinct search's ball-sized table.
- The run-time gates (`sampleSeedExpansion`, `isWorthBuilding`, `estimatedSearchChecks`) sample
  the walk's own seeds to choose between walking and building an index.
- Both indexes ignore hop predicates and trail uniqueness by design: they are lower bounds for
  pruning, not distances. The executor also stops using them when the change holds pending edges
  (`walksPendingEdges`), since they do not read the write buffer.
- `SHORTESTPATH(a, b, prop, dist, path)` (`docs/ShortestPath/Guide.md`) is a set-to-set weighted
  Dijkstra. It returns one row for the whole query, follows out-edges only, keeps a
  `std::priority_queue` with lazy deletion and a `std::unordered_map`, and runs a
  `GetOutEdgesChunkWriter` fill and a property fill per popped node. Neo4j's Cypher has no
  weighted shortest path (Section 3.5), so this statement is outside the compatibility target.
- The lexer is `%option caseless`, so that statement owns the keyword `shortestPath`. Its rule
  accepts only `SHORTESTPATH ( symbol , symbol , name , symbol , symbol )`, between the match
  statements and RETURN. Neo4j's `p = shortestPath((a)-[*]-(b))` belongs in `patternAlias`, and
  the token after `SHORTESTPATH (` separates the two: a symbol, or the `(` of a node pattern.
  `allShortestPaths` and the GQL selectors have no tokens yet.
- The executor runs a query on one thread. Of the techniques above, bit lanes and prefetching
  apply now; threads (Section 6.4) are future work.

### 10.2 One pair: find with a bidirectional BFS, list from its DAG

*(inference)* A shortest-path query has two phases: *finding*, which is the BFS, and *listing*,
which turns what the BFS found into rows. For one pair the finding is a bidirectional BFS:
balance by frontier degree sum (Section 5.2), expand one level per step, and keep epoch-stamped
sparse tables across rows (Section 5.4).

**`shortestPath` (ANY) has no listing phase.** Each side keeps one parent edge per node. At the
first meeting edge (v, w), the path is v's parent chain back to `s`, reversed, then (v, w), then
w's parent chain to `t`: O(L) to read off. This is Neo4j's `SinglePathBFS`.

**`allShortestPaths` (ALL) lists from the DAG the BFS leaves.**

- While expanding a level, the BFS scans every frontier node's edges anyway. Each edge that
  reaches a node at the next depth, new or already found at that depth, is recorded as one of
  that node's predecessor edges. The recorded edges are the DAG of Section 2.3, one per side,
  and cost no extra scan. This is Neo4j's `EagerBFS` and the PMR construction.
- The BFS finishes the level of the first meeting and collects every meeting edge (Section 5.3).
- One sweep back from the meeting edges, through the forward side's predecessor edges, marks the
  forward nodes that lie on a shortest path and records each marked node's successor edges
  within them. Forward nodes that reach no meeting edge drop out here.
- Listing is a depth-first walk: from `s` along recorded successor edges, across a meeting edge,
  then along the backward side's recorded edges down to `t`. Every step has a continuation: a
  marked forward node has a successor toward a meeting edge by construction, and every backward
  node has a recorded edge one hop closer to `t`, at least its BFS parent. The walk never backs
  out of a dead end, and each step costs O(1) over edges already recorded.

**Why the listing is depth-first.** There can be 2^n rows, as on the diamond chain of Section
2.3. A depth-first walk holds one path, O(L) state; it fills a chunk of rows, returns, and
resumes from the same stack on the next call; and it stops after 10 rows under `LIMIT 10`. A
breadth-first listing holds every partial path of a level: 2^i of them at level 2i of the
diamond chain. Neo4j's PPBFS lists the same way: its `PathTracer` "runs a DFS from a given
target … back towards the source".

**Why store the DAG rather than re-derive it.** The alternative keeps only distances and, at
each step of the listing, scans the node's whole adjacency for the neighbours one hop closer.
The walk passes a node v once per shortest prefix ending there, σ(s, v) times, so that costs
σ(s, v) × deg(v) at v. Shortest paths in small-world graphs run through hubs: a hub with 10^6
edges on 1,000 prefixes costs 10^9 checks, where the stored DAG costs 1,000 times the hub's DAG
successors. The DAG's memory is bounded by the edges the BFS scanned.

**Output.** The listing writes rows into the `PathTrie` in walk order, so `make_path`,
`length(p)`, `nodes(p)`, `relationships(p)`, `UNWIND` and OPTIONAL MATCH work unchanged. It can
be a mode of the explorator that walks the stored DAG's successor edges instead of a node's
adjacency, which keeps its chunked output, carry columns and reversed seeding. That reuses the
explorator's output, not its search. Every listed path is a shortest walk between distinct
nodes, hence simple (Section 2.1), so the trail check always passes and can be skipped.

This is also the only strategy for an exploration with `hop_imports`, which the explorator
already walks seed by seed: a predicate reading the seed row cannot be shared across the 64
lanes of an index.

### 10.3 Many pairs: read paths out of a distance field

*(inference)* When many rows share few targets, a search per row repeats work; Section 6.2
prices the crossover. `PathTargetIndex` over the chunk's distinct targets gives dist(v, t) for
every node within reach of each target, 64 targets per multi-source BFS. Read as distances
rather than bounds, it must apply everything Section 10.4 lists, which it does not today. When
the distinct sources are fewer, build it from the sources over the forward direction and walk
from the targets with `reversed_paths`. A batch should stop once every pair it serves is
resolved, rather than at the pattern's bound, which is often unbounded here.

No search runs from `s`, so there are no parent edges and no DAG. A distance field is read by
*descent*. Let d = dist(s, t) and run the explorator from `s` with `end_column` bound to `t`,
minimum and maximum both d, and its existing prune rule: at depth i, extend to v' only if
dist(v') ≤ d − i − 1.

- Every prefix is a prefix of a shortest path. At depth i the walk stands on a node v with
  dist(v) = d − i. A neighbour v' has dist(v') ≥ d − i − 1, so it passes only with
  dist(v') = d − i − 1, and the extended prefix still completes to total length d.
- Every step has a surviving candidate, the next node of any shortest path from v, so the walk
  never backs out of a dead end and every leaf is an output row.
- Every emitted path is a shortest walk between distinct nodes, hence simple; the trail check can
  be skipped.
- `ANY` takes the first candidate that passes at each step, which reads each adjacency only up
  to the first neighbour one hop closer. `ALL` takes every passing candidate.

For `ALL`, descent pays the adjacency scan that 10.2 avoids, σ(s, v) × deg(v) at v. That is the
price of sharing one BFS across 64 targets: a DAG per target would multiply the index's memory
by its lanes. When a row asks for all paths and they run through hubs, a bidirectional BFS for
that row (10.2) is the better plan.

**An unbound end** (GQL's `StatefulShortestPath(All)` shape, or a legacy cartesian product with
many rows per source). The `distinct` search already reaches each (seed, end) pair first at its
shortest distance, one level at a time; it reports neither the distance nor a path today.
Reporting the level gives `length(p)`. Paths need a per-seed distance, the target batch layout
run forward from the seeds, and descent back from each end.

**The gate** compares the pair set's cost under each strategy: the rows times a sampled
bidirectional ball, against the distinct targets (or sources) divided by 64 times a sampled
one-directional ball (Section 6.2). The samplers exist.

### 10.4 What exact means

A shortest-path search defines the answer, so it must apply everything a pruning index may skip:

- edge types, direction and tombstones, as the indexes already do;
- the hop predicate, on the frames of candidates the search generates. `PathHopFilter::filter`
  takes the frame's node as the hop's source; a backward frame holds hop ends, so the backward
  side needs the predicate with source and end swapped, as Neo4j builds a reversed predicate for
  its target side;
- node predicates from `all` / `none` over `nodes(p)` on every node, both endpoints included;
- the change's pending edges, which the explorator reads and the indexes do not;
- distances wider than a byte: legacy `[*]` is unbounded, and the indexes stop at 254 levels.

### 10.5 The legacy fallback and the GQL selectors

- **Path predicates** (`length(p) > 3`, `any(...)`). Filter the shortest paths lazily by the
  predicate and stop at the first that passes (`shortestPath`), or keep every passing path of
  the first length that has one (`allShortestPaths`). If none pass, deepen: explore trails of
  length exactly d + 1, then d + 2, up to the bound, with the explorator bound to `t` and pruned
  the way it already prunes a bound end, and stop at the first length with a passing path. The
  `PathTargetIndex` distances it prunes by are lower bounds on the hops a trail still needs, so
  the pruning never drops a valid trail. That returns the rows of Neo4j's enumerate-then-`Top`
  plan without enumerating every trail up to the bound.
- **`SHORTEST k` and `SHORTEST k GROUPS`**, one relationship, TRAIL: the same deepening,
  stopping after k paths or k lengths. Under `REPEATABLE ELEMENTS`, PathFinder's layered BFS
  instead.
- **Start equals end** (GQL): `ends_on_seed` with the same deepening. The shortest closed trail
  is not a BFS question (Section 2.1).
- **General patterns** (fixed segments with a quantifier, multi-hop bodies, several
  quantifiers): a product-graph search that validates trails, as PPBFS does. They are valid
  Cypher, so CLAUDE.md's rule applies: implement them, do not reject them in the analyzer.
- **Fixed-length patterns.** `ALL SHORTEST` and `GROUPS` become plain matching, as Neo4j
  rewrites them. `SHORTEST k` needs an operator that keeps the first k rows per
  (first node, last node) after ordinary matching.

### 10.6 Traps

- **Filter placement.** A legacy MATCH-level `WHERE` is a pre-filter: its per-step parts must
  reach the search, and its path predicates select the fallback. A GQL MATCH-level `WHERE` is a
  post-filter and must stay above the selection. `PushDownFilters` must tell them apart.
- **Same start and end.** Neo4j's default raises 51N23; its setting's alternative drops the row.
  GQL returns the shortest closed trail. For the legacy form this is a product decision.
- **Ties.** `shortestPath`, `SHORTEST k` and `ANY k` choose arbitrarily. Tests compare lengths,
  counts and sets, or use graphs whose shortest paths are unique. `ALL SHORTEST` and `GROUPS`
  are deterministic and compare as multisets.
- **The relationship variable is a list**, also for the deprecated `[r:T]` form.
- **Rows are independent.** Two input rows with the same (a, b) each get their own result.
- **The depth bound counts total length**, not length per side.

### 10.7 What to measure

In order: (a) bidirectional against unidirectional search on reactome and the fraud graph, for
random pairs and for pairs through hubs, and degree-sum against node-count balancing; (b)
per-pair search against the 64-lane index as pairs per distinct target grow, to place the gate;
(c) the stored DAG's memory for `ALL SHORTEST` on graphs with many shortest paths through hubs,
and a DAG per row against descent through the index when rows share a target and ask for all
paths; (d) the same queries through `bench/vlp` against Neo4j, Memgraph, FalkorDB and
Ladybug, which also cross-checks row counts; (e) if hub-heavy graphs need millisecond answers,
highway-cover labels per compacted version.

---

## Glossary

- **Selector**: `ANY SHORTEST`, `ALL SHORTEST`, `SHORTEST k`, `SHORTEST k GROUPS`, `ANY k`;
  picks finitely many paths per partition.
- **Partition**: the paths sharing one (start node, end node) pair; selection happens per
  partition.
- **Path mode (restrictor)**: WALK (no restriction), TRAIL (no repeated relationship), ACYCLIC
  (no repeated node), SIMPLE (no repeated node except first = last). Neo4j's default is TRAIL.
- **Pre-filter / post-filter**: a predicate applied before selection / after it.
- **Path predicate**: in legacy `shortestPath`, a predicate on `p` that cannot be checked one
  step at a time; it selects the exhaustive fallback.
- **Downward closed**: a pattern that still matches after a cycle is cut out of a walk; shortest
  walks for such patterns are simple.
- **Product graph**: pairs (graph node, automaton state), with an edge wherever a graph edge and
  an automaton transition agree; a BFS on it finds shortest matching walks.
- **Predecessor DAG / PMR**: every node reached at depth d with its edges from depth d − 1;
  holds all shortest paths in space linear in the graph.
- **Descent**: reading paths out of distances to a target by stepping, at each node, to a
  neighbour one hop closer; the only way to list paths when no search ran from the source.
- **Meeting edge**: in a bidirectional BFS, an edge from one side's frontier to a node the other
  side has reached.
- **Degree-sum balancing**: expanding the side whose frontier has the smaller sum of degrees.
- **μ rule**: stop a bidirectional search once no unexplored path can beat the best found:
  completed depths k + J + 1 ≥ μ unweighted, smallest queue keys summing to at least μ weighted.
- **Pair graph**: the requested (source, target) pairs of a batch, as a graph; a vertex cover of
  it is a set of sources that answers every pair.
- **Highway cover labelling**: distances from each node to about 20 landmarks, plus a
  bidirectional BFS on the graph without them, cut off at the landmark bound.
- **Epoch stamp**: a per-entry generation number that empties a table by incrementing one
  counter.

## Reading list

Neo4j
- Cypher manual, *Shortest paths* and its reference page, current version (2026.09).
  https://neo4j.com/docs/cypher-manual/current/patterns/shortest-paths/ and
  https://neo4j.com/docs/cypher-manual/current/patterns/reference/shortest-paths/ — selectors,
  pre-filters and post-filters, partitions, the operator table, the Station graph.
- Cypher manual 4.4, *Shortest path planning*.
  https://neo4j.com/docs/cypher-manual/4.4/execution-plans/shortestpath-planning/ — the
  fallback plan.
- Cypher manual, *Match modes and path modes*.
  https://neo4j.com/docs/cypher-manual/current/patterns/reference/match-modes-and-path-modes/
- `neo4j/neo4j` at branch `2026.09`, https://github.com/neo4j/neo4j —
  `community/cypher/runtime-util/.../traversal/BiDirectionalBFSImpl.java` and `BFS.java` (the
  legacy search), `.../traversal/ppbfs/` (PPBFS), `cypher-planner/.../ConvertToNFA.scala`,
  `.../plans/rewriter/StatefulShortestToFindShortestRewriter.scala`,
  `.../steps/planShortestRelationships.scala` (the fallback),
  `runtime-spec-suite/.../StatefulShortestPathTestBase.scala` and `ShortestPathTestBase.scala`
  (the fixtures).

Semantics and complexity
- Deutsch et al. *Graph Pattern Matching in GQL and SQL/PGQ.* SIGMOD 2022. arXiv:2112.06217 —
  path modes, selectors, partitions, pre- and post-filters.
- Francis et al. *A Researcher's Digest of GQL.* ICDT 2023. doi:10.4230/LIPIcs.ICDT.2023.1 —
  well-formedness rules; selectors evaluated before the implicit join.
- Figueira, Lin, Peterfreund. *Complexity of Evaluating GQL Queries.* arXiv:2407.06766.
- Martens, Niewerth, Popp, Rojas, Vansummeren, Vrgoč. *Representing Paths in Graph Database
  Pattern Matching.* PVLDB 16(7), 2023. arXiv:2207.13541 — PMRs, output-linear enumeration of
  shortest paths, the WDBench timeouts.
- Farías, Martens, Rojas, Vrgoč. *PathFinder: A unified approach for handling paths in graph
  query languages.* arXiv:2306.02194 (v4, 2026); ISWC 2024 — algorithms for every GQL mode.
- David, Francis, Marsault. *Distinct Shortest Walk Enumeration for RPQs.* PODS 2024.
  arXiv:2312.05505 — shortest walks with NFAs and multi-labels, O(λ·|A|) delay.
- Bagan, Bonifati, Groz. *A Trichotomy for Regular Simple Path Queries on Graphs.* PODS 2013.
  arXiv:1212.6857.
- Martens, Niewerth, Popp (Trautner). *A Trichotomy for Regular Trail Queries.* STACS 2020,
  LMCS 2023. arXiv:1903.00226 — T_tract, the single-occurrence statistics.
- Martens, Trautner. *Evaluation and Enumeration Problems for Regular Path Queries.* ICDT 2018.
- Mendelzon, Wood. *Finding Regular Simple Paths in Graph Databases.* VLDB 1989.
- Deutsch, Xu, Wu, Lee. *TigerGraph: A Native MPP Graph Database.* arXiv:1901.08248 — counting
  all shortest paths instead of enumerating them.

Engines
- Chakraborty, Salihoğlu. *Robust Recursive Query Parallelism in Graph Database Management
  Systems.* PVLDB 18, 2025. arXiv:2508.19379 — Kùzu's scheduling policies and path memory.
- Kùzu 0.7.0 release post.
  https://github.com/kuzudb/blog/blob/master/src/content/post/2024-11-13-kuzu-v-0.7.0.md — the
  frontier rewrite and its numbers; kuzudb/kuzu issues #4941 and #4459.
- ten Wolde, Singh, Szárnyas, Boncz. *DuckPGQ: Efficient Property Graph Queries in an analytical
  RDBMS.* CIDR 2023. https://www.cidrdb.org/cidr2023/papers/p66-wolde.pdf — 512-lane MS-BFS,
  the Neo4j comparison.
- Ren. MSc thesis, CWI, 2024. https://homepages.cwi.nl/~boncz/msc/2024-PinganRen.pdf — DuckPGQ's
  UDF bottleneck and a parallel operator.
- Memgraph, `src/query/plan/operator.cpp`, and *Deep path traversal*.
  https://memgraph.com/docs/advanced-algorithms/deep-path-traversal
- FalkorDB, `graph/src/runtime/eval.rs` — the bidirectional `shortestPath`.
- NebulaGraph, `ShortestPathExecutor.cpp` and `BatchShortestPath.cpp`.
- Ladybug commit `bf2a81b2` — the SHORTEST filter fix.

Bidirectional search
- Borassi, Natale. *KADABRA is an ADaptive Algorithm for Betweenness via Random Approximation.*
  ESA 2016. arXiv:1604.08553 — the m^(1/2 + o(1)) bound; path sampling through meeting edges.
- Bläsius, Freiberger, Friedrich, Katzmann et al. *Efficient Shortest Paths in Scale-Free
  Networks with Underlying Hyperbolic Geometry.* ICALP 2018.
- Bläsius, Fischbeck. *On the External Validity of Average-Case Analyses of Graph Algorithms.*
  ESA 2022, TALG 2024. arXiv:2205.15066 — the 2,740-network measurement.
- Cerf et al. arXiv:2410.22186 — balancing after every node.
- Haeupler, Hladík, Rozhoň, Tarjan, Tětek. *Bidirectional Dijkstra is Instance-Optimal.* SOSA
  2025. arXiv:2410.14638.

BFS engineering and batches
- Beamer, Asanović, Patterson. *Direction-Optimizing Breadth-First Search.* SC 2012.
- Then et al. *The More the Merrier: Efficient Multi-Source Graph Traversal.* PVLDB 8(4), 2014.
- Kaufmann et al. *Parallel Array-Based Single- and Multi-Source Breadth First Searches on Large
  Dense Graphs* (MS-PBFS). EDBT 2017.
- Then et al. *Automatic Algorithm Transformation for Efficient Multi-Snapshot Analytics on
  Temporal Graphs* (SAMS). PVLDB 10(8), 2017.
- Dong, Li, Gu, Sun. *Orionet.* SPAA 2025. arXiv:2506.16488 — pair graphs, vertex covers,
  batched bidirectional search.
- Ainsworth, Jones. *Software Prefetching for Indirect Memory Accesses.* CGO 2017.

Indexes and layout
- Farhan, Wang, Lin, McKay. *A Highly Scalable Labelling Approach for Exact Distance Queries in
  Complex Networks.* EDBT 2019. arXiv:1812.02363 — highway cover labelling; the bidirectional
  BFS baselines.
- Wang, Wang, Koehler, Lin. *Query-by-Sketch: Scaling Shortest Path Graph Queries on Very Large
  Networks* (QbS). SIGMOD 2021. arXiv:2104.09733.
- Farhan, Wang, Koehler. *BatchHL: Answering Distance Queries on Batch-Dynamic Networks at
  Scale.* SIGMOD 2022. arXiv:2204.11012.
- Akiba, Iwata, Yoshida. *Fast Exact Shortest-Path Distance Queries on Large Networks by Pruned
  Landmark Labeling.* SIGMOD 2013. arXiv:1304.4661; the dynamic version, WWW 2014.
- Dhulipala, Hong, Shun. *ConnectIt: A Framework for Static and Incremental Parallel Graph
  Connectivity Algorithms.* PVLDB 14(4). arXiv:2008.03909.
- Hanauer, Schulz, Trummer. *O'Reach: Even Faster Reachability in Large Graphs.*
  arXiv:2008.10932.
- Faldu, Diamond, Grot. *A Closer Look at Lightweight Graph Reordering* (DBG). IISWC 2019.
  arXiv:2001.08448.
- Balaji, Lucia. *When is Graph Reordering an Optimization?* IISWC 2018.
- Shun, Dhulipala, Blelloch. *Smaller and Faster: Parallel Processing of Compressed Graphs with
  Ligra+.* DCC 2015.

Weighted and k shortest paths
- Bast et al. *Route Planning in Transportation Networks.* arXiv:1504.05140 — the Western Europe
  table: Dijkstra, bidirectional Dijkstra, CH, CRP, hub labels.
- Chen et al. *Priority Queues and Dijkstra's Algorithm.* UTCS TR-07-54.
- Larkin, Sen, Tarjan. *A Back-to-Basics Empirical Study of Priority Queues.* arXiv:1403.0252.
- Dong, Gu, Sun, Zhang. *Efficient Stepping Algorithms and Implementations for Parallel Shortest
  Paths.* SPAA 2021. arXiv:2105.06145.
- Eppstein. *Finding the k Shortest Paths.* SIAM J. Comput., 1998.
- Yen. *Finding the K Shortest Loopless Paths in a Network.* Management Science, 1971.
- Hershberger, Maxel, Suri. *Finding the k Shortest Simple Paths.* ACM TALG, 2007.
- Kurz, Mutzel. *A Sidetrack-Based Algorithm for Finding the k Shortest Simple Paths in a
  Directed Graph.* ISAAC 2016.
- Feng et al. *PeeK: A Prune-Centric Approach for K Shortest Path Computation.* SC 2023.

Recent theory
- Haeupler, Hladík, Rozhoň, Tarjan, Tětek. *Universal Optimality of Dijkstra via Beyond-Worst-Case
  Heaps.* FOCS 2024. arXiv:2311.11793.
- Duan, Mao, Mao, Shu, Yin. *Breaking the Sorting Barrier for Directed Single-Source Shortest
  Paths.* STOC 2025. arXiv:2504.17033; the improvement arXiv:2602.07868; the implementation
  study arXiv:2511.03007.
- Bernstein, Nanongkai, Wulff-Nilsen. *Negative-Weight Single-Source Shortest Paths in
  Near-linear Time.* FOCS 2022. arXiv:2203.03456.

Benchmarks
- Szárnyas et al. *The LDBC Social Network Benchmark: Business Intelligence Workload.* PVLDB
  16(4). https://www.vldb.org/pvldb/vol16/p877-szarnyas.pdf — Q15, Q19, Q20 and the audited
  systems.
- Püroja, Waudby, Boncz, Szárnyas. *The LDBC Social Network Benchmark Interactive Workload v2.*
  arXiv:2307.04820 — why IC14 became a cheapest-path query.
- LDBC audit reports, https://ldbcouncil.org — GraphScope Flex SNB Interactive (2024-05-14),
  Huawei GES SNB Interactive (2025-12-01), TigerGraph BI SF10000 (2023-04-06), TuGraph BI
  SF30000 (2023-12-03).
- `ldbc/ldbc_snb_bi`, https://github.com/ldbc/ldbc_snb_bi — the Umbra, TigerGraph and Neo4j
  implementations of the BI queries.
