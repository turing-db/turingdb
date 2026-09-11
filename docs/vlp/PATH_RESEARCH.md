# Variable-length path evaluation: a research synthesis

This document collects what is known about executing variable-length path patterns
(`MATCH (a)-[e]->{1,4}(b)`, `-[e]->*`, `shortestPath`, regular path queries) in graph
databases, and explains it from first principles. It is written to be read before the code:
the goal is that a reader who knows C++ and basic graph algorithms understands why the
`PathExplorator` design in `PLAN.md` looks the way it does, which alternatives were rejected
and why, and which results in the literature are worth trusting.

The synthesis is organised as a sequence of questions:

1. What exactly is being computed? (semantics, output size, complexity)
2. In which order should the search space be walked? (BFS with a path tree vs DFS with a stack)
3. How can the search space be made smaller without changing the answer? (pruning)
4. When does the problem stop being enumeration at all? (reachability semantics, bit-parallel BFS)
5. What can be precomputed once and reused across queries? (hubs, landmarks, labels)
6. How does the hardware want to be driven? (memory-level parallelism, layout)
7. What do the existing engines do, and what does TuringDB do differently?

Sources are listed at the end with one-line annotations. Where a number comes from a paper it
is quoted with the paper; where it is a typical hardware figure it is marked as such.

---

## 1. What is being computed

### 1.1 The pattern and the row

A variable-length pattern binds three things per result row: the start node, the end node,
and the sequence of edges in between. In Cypher, `MATCH (n)-[e]->{2,4}(m) RETURN n, e, m`
returns one row per path of two, three or four edges leaving `n`; `e` is bound to the list of
edge identities of that path (a list, not an edge), `m` to the node where the path ends. The
GQL-style quantifiers TuringDB parses are `+` (1 or more), `*` (0 or more), `{k}`, `{k,}`,
`{,k}` and `{k,l}`; the classic Cypher spelling `-[e*1..3]->` means the same thing.

Two consequences shape everything downstream:

- A pattern with `min = 0` produces a zero-length path per start node: the end node is the
  start node and the list is empty. That row exists even if the start node has no edges.
- The result is a relation, so if two different paths lead from the same `n` to the same
  `m`, there are two rows. The path is part of the row's identity even when the query does
  not project it. `MATCH (n)-[e]->{1,3}(m) RETURN m` returns `m` once per path, with
  duplicates. Only `DISTINCT`, `EXISTS`, or an aggregate that ignores multiplicity changes
  that.

### 1.2 Path semantics: walk, trail, simple path

A *walk* is any sequence of adjacent edges. A *trail* is a walk in which no edge repeats. A
*simple path* (GQL: `ACYCLIC`) is a walk in which no node repeats. Every simple path is a
trail and every trail is a walk; the reverse implications fail.

Query languages differ on the default:

| Language / system | Default semantics for `-[e*]->` | Notes |
|---|---|---|
| openCypher, Neo4j, Memgraph, TuringDB | trail (edge-distinct, "relationship isomorphism") | nodes may repeat |
| GQL, SQL/PGQ | walk, with `TRAIL`, `SIMPLE`, `ACYCLIC` path modes | plus `ANY`, `ALL SHORTEST` selectors |
| Kùzu | walk, with `TRAIL` and `ACYCLIC` opt-in | |
| SPARQL property paths | reachability (pairs), not paths | |

The choice matters more than it looks. Consider this graph:

```
        e1            e3            e4
   A ────────▶ B ────────▶ C ────────▶ D
   ◀────────                ◀──────────
        e2                       e5
```

(e2: B→A, e5: D→B.) Enumerating from A with at most 3 hops:

| length | walks | trails | simple paths |
|---|---|---|---|
| 1 | A·e1·B | A·e1·B | A·e1·B |
| 2 | A·e1·B·e2·A, A·e1·B·e3·C | both | A·e1·B·e3·C only (the other revisits A) |
| 3 | A·e1·B·e2·A·e1·B, A·e1·B·e3·C·e4·D | A·e1·B·e3·C·e4·D only (the other reuses e1) | A·e1·B·e3·C·e4·D |
| total ≤ 3 | 5 | 4 | 3 |

With 4 hops, A·e1·B·e3·C·e4·D·e5·B is a trail but not a simple path (B repeats), so the
trail count keeps growing where the simple-path count has stopped. Under walk semantics the
count never stops growing on a graph with a cycle, which is why walk semantics is only usable
with an explicit upper bound.

Trail semantics has one property that the rest of this document leans on: a trail cannot be
longer than the number of edges in the graph, so `*` (unbounded) is finite even on cyclic
graphs. It also has a property that hurts: reachability under trail semantics is not
reachability. In the two-node graph with only e1: A→B and e2: B→A, a walk of length exactly 3
from A to B exists (A→B→A→B) but no trail of length 3 does. Section 4 returns to this.

### 1.3 Output size and complexity

The number of trails of length ≤ k from a node grows like d^k on a graph of average degree d
(fewer on sparse graphs, far more from hubs). The output is exponential in k and any
algorithm must at least write it, so the interesting complexity measure is not total time but
*delay*: the time between two consecutive outputs, and the preprocessing before the first
one. An enumeration algorithm has *polynomial delay* if every gap is polynomial in the input
size. For hop-constrained simple paths between two given nodes, Rizzi, Sacomoto and Sagot
(2014) and Grossi, Marino and Versari (2018) gave polynomial-delay algorithms (T-DFS,
T-DFS2), and Peng et al. (VLDB 2019) gave BC-DFS with delay O(k·|E|). The catch, documented
by both Peng et al. and the PathEnum authors, is that the pruning needed to guarantee the
delay costs more than it saves on most real queries; Section 3 explains why and what replaced
it.

Counting is a different problem. Walks of a given length can be counted in polynomial time
(powers of the adjacency matrix; equivalently a DP over levels). Counting simple paths, and
counting trails, is #P-complete (Valiant 1979 for simple paths; trails reduce to it). So a
`RETURN count(*)` over a trail pattern cannot be answered by a clever closed form; it needs the
enumeration, or a pruning of it that preserves cardinality.

Existence is easy: a trail from `u` to `v` exists iff a walk does (shorten any walk to a
path), so reachability questions collapse to BFS. That gap between "does a path exist" (linear
time) and "list the paths" (exponential output) is the whole design space.

### 1.4 Four query shapes

The same syntax covers workloads with different asymptotics, and one algorithm does not fit
all:

1. **Source-only enumeration**: `MATCH (n:Person)-[e]->{1,3}(m) RETURN n, e, m`. Nothing
   constrains the end. Output is inherent; only constant factors can be improved.
2. **End-constrained enumeration**: `(m:Rare)`, `WHERE m.name = 'x'`, or `m` also matched by
   another pattern. Most partial paths never end on a valid node; pruning them changes the
   exponent.
3. **Bound source and target (s-t)**: `MATCH (a {id: 1})-[e]->{1,6}(b {id: 2})`. The classic
   hop-constrained s-t path enumeration (HcPE) problem of the literature: fraud rings, money
   laundering paths, knowledge-graph relation paths.
4. **Reachability-only**: `RETURN DISTINCT m`, `EXISTS { ... }`, `count(DISTINCT m)`, or a
   shortest-path selector. Multiplicity does not matter, so BFS with a visited set replaces
   enumeration, subject to the trail caveat of Section 4.1.

`PLAN.md` maps these onto tiers: Tier 1 handles shape 1 as well as it can be handled, Tier 2
handles shape 2, Tier 3 handles shapes 3 and 4.

---

## 2. Two ways to walk the search space

The search space of an enumeration is a tree: the root is the start node, each child is a
partial path extended by one edge, each node at depth d is a path of length d. Enumerating
all trails means visiting every tree node that passes the trail check, and emitting those at
depths in `[min, max]`. The two classical orders to visit a tree are breadth-first and
depth-first, and they have very different memory and locality profiles.

### 2.1 Level-synchronous BFS with a path tree

Breadth-first visits all depth-d partial paths before any depth-(d+1) one. Since each
partial path must be remembered until it is extended, and a path is a sequence of edges, the
natural compressed representation is a *parent-pointer tree*: each tree node stores the last
edge, the end node and the index of its parent; the path is recovered by walking parents to
the root. This is exactly what TuringDB's v2 `PathExplorerProcessor` does (`_allEntries`
with `parentIdx`), what MillenniumDB's PathFinder calls its *path index* (a DAG of parent
pointers in the visited dictionary), and what Kùzu's recursive join does with its frontier
structures.

```
depth 0:   [A]
depth 1:   [B ← A via e1]
depth 2:   [A ← B via e2]   [C ← B via e3]
depth 3:   [D ← C via e4]   ( [B ← A via e1] rejected: e1 already on the path )
```

Costs:

- **Memory**: every partial path of every depth stays alive, because deeper levels point
  into shallower ones. Memory is proportional to the number of partial paths, i.e. to the
  output itself (v2 documents this: "the memory usage is unbounded"). For the *result* of a
  shortest-path query this is the right structure, because shortest paths from one source form
  a DAG whose size is bounded by the graph, not by the number of paths.
- **Trail check**: to decide whether edge e may extend a partial path, walk the parent chain
  and compare: O(depth) dependent memory accesses per candidate edge, into a tree that grows
  beyond cache.
- **Path reconstruction**: another O(depth) parent walk per emitted row.
- **What it buys**: all partial paths ending at the same node at the same depth are visible
  together, so the adjacency of that node can be fetched once and applied to all of them
  (frontier deduplication), and the frontier can be sorted by node identifier to stream
  through the adjacency arrays in order. Graph-analytics BFS engines (Ligra, GAP, Graph500)
  live on exactly these properties. They help when many paths converge on the same nodes at
  the same depth, which is common in social graphs and rare in path-shaped ones.

The sharing argument is weaker than it sounds for enumeration. Fetching the adjacency of a
node costs one or two array reads; *processing* it (one candidate check per edge per partial
path) is the same whether the fetch was shared or not, because each partial path has its own
trail check and its own children. Sorting the frontier to get locality costs a radix sort
over all partial paths of the level, which is itself several passes over as many bytes as the
tree entries. Whether it pays depends on whether the adjacency arrays fit in cache; on a
cache-resident graph it is pure overhead.

### 2.2 Depth-first with an explicit stack

Depth-first extends the current path as far as it goes before backtracking. The current path
*is* the stack, so the representation needs no tree at all: a vector of the edges on the
path, one frame per depth holding the still-unexplored candidate children of that frame's
node, and that is the entire state. This is how Neo4j's `VarLengthExpand(All)` (a stack of
relationship iterators per input row), Memgraph's `ExpandVariable` (a stack of edge iterators
per frame) and PathEnum's `IDX-DFS` work.

```
path:    [e1, e3]                     ← A·e1·B·e3·C, depth 2
frames:  frame0 (children of A):  e1 ✓ taken
         frame1 (children of B):  e2 (pending), e3 ✓ taken
         frame2 (children of C):  e4 (pending)
```

Costs:

- **Memory**: O(max depth × max degree) per active start node, independent of the number of
  paths. The stack for a path of length 8 in a graph of degree 100 is a few kilobytes.
- **Trail check**: scan the current path's edge vector, which is contiguous and at most
  `depth` entries long: for typical depths one or two cache lines, always hot.
- **Path materialization**: copy the edge vector, a contiguous source, into the output list.
- **Streaming**: results appear as soon as the first leaf is reached; a `LIMIT 10` after the
  pattern stops after ten paths, where BFS would have computed whole levels first. The stack
  is also a natural resumable cursor: a chunk-at-a-time executor fills a chunk, returns, and
  continues from the same frames on the next call.
- **What it loses**: adjacency lookups are not shared between paths reaching the same node,
  and each descent is a dependent memory access (read the node's range, then its edges) with
  no natural batching. On a graph that does not fit in cache, a single DFS is latency-bound.
  Section 6 shows how to get the latency back without giving up the stack.

The comparison is not BFS versus DFS in the abstract; it is "what does the path tree pay for".
For enumeration under trail semantics, it pays for prefix sharing that the stack gives for
free, at the price of unbounded memory and pointer-chasing checks. For shortest-path and
reachability semantics, the tree (or its DAG form) is the right output structure and BFS the
right order. `PLAN.md` uses DFS for enumeration and keeps BFS for the reachability and index
work.

### 2.3 The trail check, done well

Whatever the order, every candidate edge must be tested against the edges already on the
path. Three implementations:

1. **Scan the path**: O(d) compares. Contiguous in the DFS design, pointer-chasing in the
   tree design. Fine for d ≤ 8, noticeable beyond.
2. **A per-path hash set**: O(1) expected, but a set per partial path is expensive to copy
   or maintain along a stack (insert on push, erase on pop is fine; the constant factor is
   not).
3. **A per-path signature**: a 64-bit word that is the OR of one hashed bit per edge on the
   path, a Bloom filter with one hash function. Testing a candidate is a multiply, a shift, an
   AND. A zero means "definitely not on the path" and admits the edge immediately; a one means
   "maybe" and falls back to the scan. The signature of the path at depth d+1 is the
   signature at depth d OR the new edge's bit, so it costs nothing to maintain along a stack
   (one word per depth).

The false-positive probability with j bits set among 64 is 1 − (63/64)^j. Excluding the
parent edge (which is compared exactly first, because the immediate back-edge is the most
common rejection in undirected exploration), a path of depth d sets at most d − 1 bits:

| depth d | 2 | 3 | 5 | 8 | 12 | 16 |
|---|---|---|---|---|---|---|
| P(fallback scan) | 1.6 % | 3.1 % | 6.1 % | 10.4 % | 15.9 % | 21.0 % |

The hash matters. Edge identifiers in TuringDB are dense and locally consecutive (a node's
edges have adjacent identifiers), so `id mod 64` would put the edges of one node onto
consecutive bits and edges 64 apart onto the same bit. A multiplicative (Fibonacci) hash
`(id × 0x9E3779B97F4A7C15) >> 58` spreads consecutive identifiers over the word. A second
hash function would lower the false-positive rate around d ≈ 10–40 and raise it beyond; for
the depths Cypher queries use it is not worth the second multiply.

The signature is a filter, never a decision: correctness rests on the exact scan taken on a
hit, so a collision costs time, not rows.

### 2.4 Materialization and factorized representations

When the query projects `e`, every row carries its list of edges, and writing those lists
costs Σ rows × depth × bytes-per-element, which for deep enumerations dominates everything
else. Two observations from the literature:

- **Factorized representations** (Olteanu and Závodný; Kùzu's factorized query processing):
  a set of paths sharing prefixes is representable as a trie whose size is the number of
  *distinct prefixes*, not the number of paths times their length. The parent-pointer tree of
  Section 2.1 is precisely a factorized representation of the output. Kùzu keeps its
  intermediate results factorized so that a downstream filter or aggregate never flattens
  them; PathFinder's path index enumerates paths from its DAG with output-linear delay.
- **Flat lists are what the client sees**, and a list type that guarantees contiguous
  elements (TuringDB's `ListView` over the query `ListBuffer`) cannot share prefixes. So the
  flattening cost is inherent *at the output*, but not at intermediate operators: a filter
  on `m`, a `SKIP`/`LIMIT`, or a sort by another column should not pay per-element for the
  paths of the rows they drop.

Two practical rules follow. First, materialize nothing the query does not read: a column
pruning pass (TuringDB's `TrimUnreadColumns`) tells the operator whether `e` is consumed, and
if not the path list is never built, which turns `RETURN m` and `RETURN count(*)` into pure
traversal. Second, when the paths are consumed by later operators before being output, a
factorized path column (a handle into a per-query prefix trie, expanded only at output or
`UNWIND`) removes the per-row copy from every intermediate step. `PLAN.md` puts the first rule
in Tier 1 and the second in Tier 4.

---

## 3. Pruning: changing the exponent

### 3.1 Intermediate cardinality is the cost

Every study of path enumeration that reports a breakdown arrives at the same conclusion: the
running time tracks the number of partial paths generated, not the speed at which each is
generated. The most striking recent data point is ReCAP (2026), which evaluates path queries
with label patterns and property constraints in DuckDB by pushing the constraints into the
recursion. On a fraud-detection query with hop bound 4, the compared engines (Neo4j, Memgraph,
Kùzu, a commercial RDBMS) generated about 3.5 million intermediate paths where ReCAP generated
324, and the speedup was 82,940×; across their queries the maximum was 400,000×, and ReCAP
still answered at hop bound 10 where the others timed out around 6. The technique is not
exotic: a prefix is *doomed* if no extension can satisfy the constraints (timestamps that must
be increasing and already are not; a budget already exceeded; an end label that can no longer
be reached), and a doomed prefix is dropped the moment it becomes doomed rather than after the
whole path is built. The abstraction ReCAP proposes is a per-path accumulator with an
`is_viable(state, next_edge)` predicate evaluated at every extension.

PathEnum (SIGMOD 2021) reports the same phenomenon from the other side: measured on 15 real
graphs, the dynamic-pruning baseline BC-DFS touched roughly 100× more edges than PathEnum's
index-based DFS for the same result set, and PathEnum was 1.9× to 240.7× faster in query time
and 14.2× to 358.5× faster to the first 1000 results.

So the question for shapes 2 and 3 is not how fast a candidate can be checked, but how early
a partial path can be known to be useless.

### 3.2 Barrier pruning (BC-DFS)

For s-t enumeration, the first idea is a lower bound on the hops still needed: if the shortest
distance from the current node to t exceeds the remaining budget, backtrack. Peng et al.
(VLDB 2019) maintain, for each node, a *barrier* initialised to the shortest distance to t and
raised whenever a search subtree under that node produced no result, which yields polynomial
delay O(k·|E|). The cost is the bookkeeping: each visit updates and checks barriers, and the
PathEnum authors measured that for most queries the pruning work exceeded the enumeration
work it saved. The lesson is general: pruning must be cheaper than what it prunes, so
precompute once per query rather than maintain per visit.

### 3.3 PathEnum's query-time index

PathEnum replaces dynamic barriers with a light-weight index built once per query:

1. Run a BFS from s over the graph and a BFS from t over the reversed graph, both bounded by
   k, giving `S(s,v)` and `S(v,t)` for the nodes within reach.
2. Keep only nodes with `S(s,v) + S(v,t) ≤ k`: a node further than that from the pair cannot
   lie on any path of length ≤ k. Organise them in a (k+1)×(k+1) grid by the two distances.
3. For each kept node, store its kept neighbours sorted by distance to t. Then "the neighbours
   of v from which t is still reachable within b hops" is a *prefix* of that list, retrieved
   in O(1) by an offset table.

Enumeration is then a plain DFS on the index: at depth i with the path ending at v, iterate the
neighbours v' with `S(v',t) ≤ k − i − 1`, skip those already on the path (simple-path
semantics in their setting), recurse. There is no barrier to maintain; the distance check is
an array bound. Index construction is O(|V| + |E|) and, on graphs where the k-hop balls around
s and t are small, far less than that in practice; the paper's Table 7 shows indexes of a few
megabytes for k up to 8 on million-edge graphs. On their two-billion-edge graph the two BFS
runs took tens of seconds and dominated short queries, which motivates a cost gate (Section
3.6).

The transfer to TuringDB is direct, with one generalisation. The s side of the index is
unnecessary when exploring by DFS from s, because depth already is `S(s,v)` restricted to the
current path. The t side becomes "distance to *any* valid end node": for `(m:Rare)` the target
set is every node carrying the label, for a bound `m` it is that node, for a per-row bound `m`
it is one target per row (Section 3.5). One bounded multi-source BFS over the reverse of the
exploration direction (in-edges for a forward pattern, out-edges for a backward one, both for
undirected) computes `dist(v)` for all nodes within `max` hops of the target set. The DFS rule
is then:

> at depth d with remaining budget r = max − d, extend to v' only if dist(v') ≤ r − 1, and emit
> v' only if dist(v') = 0 (it is a target) and d + 1 ≥ min.

Soundness under trail semantics needs one sentence: `dist` is the shortest *walk* distance,
and every trail is a walk, so `dist(v')` lower-bounds the length of any trail from v' to a
target; if the budget is below the lower bound, no completion exists. The pruning is
therefore exact (never drops a valid path) even though it was computed without any uniqueness
constraint. Two obligations come with it: the BFS must run over the same edge set as the
exploration (same type filter, same tombstone filtering), and with an unbounded `max` the
distance degenerates into plain reachability, which still prunes dead-end regions.

### 3.4 Join-based enumeration

PathEnum's second enumeration method treats the path as a chain join `R₁ ⋈ R₂ ⋈ … ⋈ R_k` and
chooses where to cut it. Enumerate all prefixes of length i* from s and all suffixes of length
k − i* from t (both by DFS on the index), then hash-join them on the middle node and reject
pairs that share a node (or, for trails, an edge). The cut position is chosen by a dynamic
programme over the index that estimates the number of walks of each length from s and to t
(`c^i_j(v)` counts propagated along the sorted neighbour lists), minimising |prefixes| +
|suffixes|. The cost model is the textbook `T(Q) = |Q| + T(Q₁) + T(Q₂)`, i.e. the total number
of intermediate results. Their Figure 9 shows the join winning by more than 2× on long queries
with large search spaces and losing on short ones where the optimiser's own cost exceeds the
enumeration; hence PathEnum first runs a cheap cardinality estimate and only optimises the join
order when the estimated search space is large.

The classic meet-in-the-middle argument explains the win: with degree d and length k, one-sided
enumeration explores d^k partial paths, two-sided enumeration explores 2·d^{k/2} and joins them.
The join output is the same either way; the intermediate work is the square root.

### 3.5 From s-t to end constraints and per-row targets

Three target shapes appear in Cypher queries, and they need slightly different index builds:

- **A label or predicate on the end node** (`(m:Rare)`, `WHERE m.x = 1`): the target set can
  be large. Distances from a large set are small almost everywhere (most nodes are within a
  hop or two of *some* `Rare` node), so the pruning power is proportional to the label's
  rarity. Enumerate the set from the label index rather than by scanning all nodes; TuringDB's
  per-part `LabelSetIndexer` gives node ranges per label set directly.
- **One bound target for the whole exploration**: PathEnum's original setting; the BFS from t
  is tiny for small k.
- **A different target per input row** (`MATCH (a)-[e]->{1,6}(b), (a)-->(b)`; or `b` bound
  by an earlier `WITH`): a BFS per distinct target is too expensive when there are thousands of
  rows. Multi-source BFS (Section 4.2) computes distances from up to 64 targets in one pass,
  storing per node a bit per target per level; each row's DFS then prunes against its own
  target's bit. Batch HC-s-t processing (Yuan et al., ICDE 2024) formalises a related sharing:
  queries with a common source share the "HC-s path" computation, and their two-phase detection
  finds the shareable groups in a batch.

### 3.6 A cost gate

Every pruning index has a fixed cost (a bounded BFS, at most O(|V| + |E|)) and a variable
benefit (the fraction of the search tree it cuts). For `MATCH (n {id: 1})-[e]->{1,2}(m:Person)`
on a small neighbourhood the index costs more than the enumeration; for `{1,6}` from a hub the
enumeration is astronomically larger than any BFS. The decision must be made at run time with
graph statistics that only the executor has: an estimate of the enumeration work such as
`seeds × avgDegree^min(max,4)` compared to the size of the target set's `max`-hop ball
(bounded by |V| + |E|). PathEnum makes an analogous two-stage decision with a preliminary
estimator (their Equation 5) before paying for the full-fledged one. `PLAN.md` puts this gate
in the executor, next to the decision of how many walkers to interleave.

---

## 4. When the problem is not enumeration

### 4.1 Reachability semantics and the trail caveat

`RETURN DISTINCT m`, `count(DISTINCT m)`, `EXISTS { (n)-[*1..3]->(m:Flagged) }` and `WHERE
EXISTS` ask whether an end node is reachable within the hop range, not how many ways. Neo4j's
planner recognises the pattern and replaces `VarLengthExpand(All)` by `VarLengthExpand(Pruning)`
(a DFS that remembers visited nodes and their remaining budget so that a node is not
re-expanded with a smaller budget) or `VarLengthExpand(Pruning,BFS)` when the lower bound is 0
or 1; Neo4j's documentation lists both conditions: an upper bound, and a `DISTINCT` (or a
compatible aggregation) downstream.

The reason for the "0 or 1" condition is the trail caveat from Section 1.2. Reachability by
BFS answers "is there a *walk* of length in [min, max]", and the set of nodes reachable by
walks of length in [min, max] equals the set reachable by trails of such lengths only when
min ≤ 1: any walk of length between 1 and max shortens to a path (a trail) of length between 1
and max, and a zero-length walk is a zero-length trail. For min ≥ 2 the two differ (A→B→A→B in
the two-edge graph is a walk with no trail of the same length), so a BFS would report B for
`{3,3}` where enumeration reports nothing. Any DISTINCT mode must therefore either check
`min ≤ 1` or fall back to enumeration with output deduplication.

### 4.2 Multi-source BFS

Then et al. (VLDB 2014, "The More the Merrier") observed that on small-world graphs, many
concurrent BFSs from different sources visit the same nodes at the same levels, and that a
set of up to ω concurrent BFSs can be represented by an ω-bit word per node: bit b of
`seen[v]` says whether BFS b has already discovered v, bit b of `visit[v]` whether BFS b has v
in its current frontier. One level of all ω searches is then:

```
for each v in frontier:
    for each u in N(v):
        visitNext[u] |= visit[v]              // every search at v discovers u
for each u touched:
    visitNext[u] &= ~seen[u]                  // drop what each search already knew
    seen[u]      |= visitNext[u]
    if visitNext[u] != 0: frontier' += u
```

Set union, difference and emptiness become OR, AND-NOT and a zero test on machine words, and
the adjacency of v is read once for ω searches instead of ω times. With ω = 64 on scalar
registers or 512 with AVX-512 (DuckPGQ's SIMD variant of MS-BFS), the shared traversal is
where the speedup comes from; on graphs without the small-world property the sharing is weaker
but the bit-parallelism still applies. Kùzu's 2025 work on recursive query parallelism shows
MS-BFS can be modelled as one morsel-dispatch policy among others (source-level, frontier-level,
hybrid) and that its benefit depends on having enough sources.

Two uses in the path setting:

- **DISTINCT mode** (Section 4.1): 64 start nodes per word, a visited word per node, one row
  per (start, end) bit that first appears at a level in [min, max].
- **Per-row target distances** (Section 3.5): 64 targets per word, BFS over the reverse
  direction, distance of target j to node v = first level whose word at v has bit j set.

The memory of a dense per-level word array is 8·(max+1)·|V| bytes, which is fine for millions
of nodes and prohibitive for billions; sparse per-level maps are the fallback when the reached
balls are small, which is precisely the case where the index is cheap.

### 4.3 Frontier engineering for the index BFS

The BFS that builds a pruning index is an ordinary single-source (or multi-source) BFS, and the
graph-analytics literature has settled how to make it fast: a sparse frontier (a queue of node
identifiers) while the frontier is small and a dense frontier (a bitmap over all nodes) once it
is a sizeable fraction of the graph (Ligra's automatic switch, Shun and Blelloch 2013);
direction-optimising BFS (Beamer, Asanović and Patterson 2012), which switches from
"top-down: each frontier node pushes to its neighbours" to "bottom-up: each unvisited node
looks for any frontier parent and stops at the first" when the frontier is large, saving most
edge examinations on low-diameter graphs. For a bounded BFS of a few hops from a small target
set the frontier never gets large and none of this matters; for label targets covering a
large fraction of the graph the dense representation is the difference between milliseconds
and seconds.

### 4.4 Output-sensitive evaluation of regular path queries

A variable-length pattern with a type is the regular path query `a*` or `a{1,k}`; the general
theory (Mendelzon and Wood 1995; Martens and Trautner 2018 on enumeration; Martens, Niewerth
and Trautner 2020 on trails) treats arbitrary regular expressions over edge labels and asks
for the *pairs* (s, t) connected by a matching walk. The workhorse is the product graph: pair
each graph node with an automaton state and run BFS. Recent work (Output-Sensitive Evaluation
of Regular Path Queries, 2024) refines this to a running time that depends on the output size,
O(|E|^{3/2} + min(OUT·√|E|, |V|·|E|)), by splitting nodes into light and heavy by degree
(threshold √|E|) and processing them differently. MillenniumDB/PathFinder implements the
product-graph BFS with a path index for `ANY`/`ALL SHORTEST` and adds constraint checking
during traversal for `TRAIL` and `SIMPLE`, and reports order-of-magnitude speedups over graph
engines on Wikidata path queries.

The lesson for a Cypher engine is that reachability-shaped questions over typed paths already
have near-linear algorithms; the expensive part of Cypher is that its default asks for every
path, and the design should make sure the engine only pays for enumeration when the query
really needs the multiplicity.

---

## 5. Hubs, landmarks and offline indexes

Everything in Sections 3 and 4 is computed per query. A second family of techniques
precomputes structure once, amortised over many queries, and almost all of it revolves around
one empirical fact about real graphs: a small set of high-degree nodes lies on nearly every
path. This section explains why that is true, what has been built on it, and what it would
cost to maintain such an index inside a versioned store.

### 5.1 Why hubs decide the cost

Two measurements make the point. The GraphS team at Alibaba (Qiu et al., VLDB 2018)
instrumented a production graph of 519 million vertices and 2.09 billion edges: 80 % of
vertices had degree below 10, the largest had degree above 78,000, and only 5,274 vertices
had degree above 4,000. Enumerating paths of length k from random vertices, 93 % of the
length-3 paths and 99 % of the length-5 paths passed through at least one vertex of degree
40 or more. Their baseline DFS showed latency spikes of hundreds of seconds exactly when the
search entered such a vertex; on a random graph with the same number of vertices and edges,
the number of k-paths grew far more slowly, because no vertex fans out that much.

Pruned Landmark Labeling (Akiba, Iwata and Yoshida, SIGMOD 2013) measured the same fact from
the shortest-path side: after running their pruned BFS from only the 1,000 highest-degree
vertices, the labels of fewer than 10 % of the vertices still received new entries, and after
10,000 fewer than 1 %; most vertex pairs have a shortest path through a few thousand central
vertices, and distant pairs are covered before close ones.

For enumeration the consequence is asymmetric. In a source-only query (shape 1) the paths
through hubs *are* the answer and nothing avoids writing them. In every constrained shape,
hubs are where the wasted work concentrates: a DFS that enters a hub with r hops of budget
left spends d_hub^r work, of which only the fraction leading to a valid end survives. Hub-aware
techniques therefore aim at one of three things: know the distance from a hub to the target
without exploring (distance labels), know what a hub can reach at all (reachability and
landmark labels), or never expand a hub during the search and instead stitch precomputed
hub-to-hub segments (hot-point indexes).

### 5.2 Distance labelings: 2-hop covers and pruned landmark labeling

A *2-hop cover* (Cohen, Halperin, Kaplan and Zwick, 2002) gives each vertex v a label
L(v) = {(h, d(v,h))} over a set of hub vertices such that for every pair (s,t) some hub h lies
on a shortest s–t path and appears in both labels. Then

    d(s,t) = min over h ∈ L(s) ∩ L(t) of d(s,h) + d(h,t),

computed by merging two sorted label lists in O(|L(s)| + |L(t)|), microseconds in practice.
The hard part is building small labels. Pruned Landmark Labeling does it with an algorithm
that fits in ten lines: order the vertices by decreasing degree, and run one BFS per vertex in
that order; when the BFS from v_k reaches u at distance δ, first query the labels built so far
for d(v_k, u); if that already returns a value ≤ δ, do not label u and do not expand it (an
earlier, more central hub already covers this pair); otherwise add (v_k, δ) to L(u) and
continue. The pruning makes later BFSs tiny (the paper's figures show the k-th BFS visiting a
vanishing fraction of the graph), the result is provably a minimal 2-hop cover for the chosen
order, and the order matters enormously: on the Gnutella graph the average label has 781
entries with degree ordering and 6,171 with random ordering.

Numbers from their Table 3 (single thread, 2013 hardware), with the distances stored as 8-bit
integers because small-world diameters fit in a byte:

| graph | vertices / edges | build | index | query | plain BFS |
|---|---|---|---|---|---|
| Epinions | 76 K / 509 K | 1.7 s | 32 MB | 0.5 µs | 7.4 ms |
| WikiTalk | 2.4 M / 4.7 M | 61 s | 1.0 GB | 0.6 µs | 197 ms |
| Skitter | 1.7 M / 11 M | 359 s | 2.7 GB | 2.3 µs | 190 ms |
| Flickr | 1.8 M / 23 M | 866 s | 4.0 GB | 2.6 µs | 361 ms |
| Hollywood | 1.1 M / 114 M | 15,164 s | 12 GB | 15.6 µs | 1.2 s |
| Indochina | 7.4 M / 194 M | 6,068 s | 22 GB | 4.1 µs | 1.5 s |

Two refinements matter for a database. *Bit-parallel labels*: the first few BFSs, from the
most central vertices, cover the most pairs, and each can be run together with up to 64 of its
neighbours by keeping per-vertex bit sets of "which of these sources are at distance δ, δ−1"
(the same bit-parallel idea as MS-BFS, Section 4.2), which the authors report speeds
preprocessing two to ten times. *Directed graphs*: two labels per vertex, one from pruned
BFSs over out-edges and one over in-edges. The same labels extended with counts answer
shortest-path *counting* (Zhang and Yu, SIGMOD 2020), and with parents they reconstruct one
shortest path.

Construction scales with parallelism and with exploiting structure: Parallel Shortest-distance
Labeling (Li et al., SIGMOD 2019) rebuilds the labels level by level so that all labels at
distance d are computed in parallel, and its core–periphery successor (SIGMOD 2020) indexes the
dense core with labels and the tree-like fringe with tree decompositions, reaching
billion-edge graphs. Maintenance is the weak spot and an active area: the incremental variant
(Akiba, Iwata and Yoshida, WWW 2014) handles edge insertions by a pruned BFS from the new
edge's endpoints; deletions were only solved later (D'Angelo, D'Emidio and Frigioni, "Fully
Dynamic 2-Hop Cover Labeling", 2019), and the current state of the art, M2HL (Zeng et al.,
VLDB 2025), maintains correctness *and* minimality under batches of insertions and deletions in
parallel, reporting up to four orders of magnitude over earlier maintenance methods; the same
paper notes that rebuilding the Orkut labels (29 M vertices, 106 M edges) from scratch with
the parallel builder takes over 10⁴ seconds on 40 cores, and that one earlier maintenance
method needed 1.5 TB of auxiliary memory on Pokec where M2HL needs 46 GB.

**What labels buy a path engine.** A distance label is a pruning oracle for the DFS of
Section 3.3 that costs nothing per query: `dist(v, t)` in a few microseconds instead of a
bounded BFS from t. For a single bound target that is a wash (the BFS from t is cheap too);
for thousands of per-row targets it replaces thousands of BFSs. For a *set* of targets (a
label class) it is the wrong tool, because the minimum over targets costs one merge per target.
There is also a soundness subtlety worth knowing when the index lags behind the data. The
pruning rule drops v when `dist(v,T) > r`. A deleted edge can only increase true distances, so
a label that has not seen the deletion returns a value ≤ the truth and prunes *less*: still
sound. An inserted edge can decrease true distances, so a stale label returns a value that
may be too large and would prune a valid path: unsound. In a versioned store where deletions
are tombstones and insertions arrive in commits, that means a distance index attached to a
commit stays a valid pruning oracle across deletions for free and must only be brought up to
date for insertions, which is the direction incremental PLL handles cheaply.

Approximate cousins are worth a sentence. Landmark embeddings (Potamias et al., CIKM 2009)
store distances to a few dozen high-centrality landmarks ℓ and bound the distance by the
triangle inequality, `|d(v,ℓ) − d(t,ℓ)| ≤ d(v,t) ≤ d(v,ℓ) + d(ℓ,t)`; the lower bound is the
ALT heuristic of A* on road networks (Goldberg and Harrelson, 2005). A lower bound is all the
DFS pruning rule needs, so landmark distances are a valid, much smaller, but much weaker oracle
than an exact 2-hop cover: they prune only paths whose end is far from every landmark's
perspective.

### 5.3 Landmark indexes for constrained reachability

Distance labels answer "how far"; a different landmark family answers "can reach at all" under
a constraint. Label-constrained reachability (LCR: is there a path from s to t using only edge
types in a set L?) is the reachability version of `EXISTS { (s)-[:A|B*]->(t) }`. Valstar,
Fletcher and Yoshida (SIGMOD 2017) build their index only for *landmark* vertices, the top
1250 + √n by degree, and for each landmark store the vertices it reaches together with the
*minimal* type sets that reach them (a set L is stored only if no strict subset also works,
which keeps the entry count far below the 2^|types| worst case). A query BFSes from s over
L-edges; the moment it touches a landmark, the landmark's entries answer it. Two extensions
turn this into a pruning device rather than a shortcut: every non-landmark stores a small
budget (20 in their experiments) of "I reach landmark v under L" entries, so the search hits an
indexed vertex early; and every landmark v keeps, for small type sets, the set of vertices
that *can reach it*, `R_L(v)`. The second gives a doomed-set rule: if the search reached
landmark v but v cannot reach t, then no vertex in `R_L(v)` can reach t either, and all of them
are marked dead for the rest of the search. On fourteen graphs up to 102 M edges their index
built in 0.1 s to seven hours and occupied 5 MB to 103 GB; true queries ran up to two orders of
magnitude faster than direction-optimising BFS (Youtube: 16 µs vs 2,000 µs; socPokec: 9 µs vs
1,290 µs), and false queries within the same order or better, while earlier LCR indexes did
not finish on graphs four orders of magnitude smaller. Later work extended the idea to dynamic
graphs (DLCR, VLDB 2022) and to concatenation patterns rather than type sets (pattern-
constrained reachability with a two-dimensional index, 2025), where the problem becomes
NP-hard and indexes trade completeness for filters.

The transferable idea is the doomed set, which is ReCAP's viability predicate (Section 3.1) in
precomputed form: a landmark that cannot reach the target under the query's constraints
condemns everything that can only reach the target through it.

### 5.4 Hot-point indexes: never expand a hub

GraphS attacks enumeration itself rather than reachability. Its HP-Index picks a degree
threshold t (tuned to about 40 for their graph) and calls vertices above it *hot points*. For
every ordered pair of hot points it stores every simple path of length ≤ k between them that
passes through no other hot point, organised as an *index graph* whose vertices are the hot
points and whose edges are those stored paths, weighted by length. The count stays manageable
precisely because hubs are few and segments between hubs are short: in their graph, the
5,274 vertices above degree 4,000 had 1,399,383 such paths of length below 6 between them. A
query for all k-paths from s to d then runs in three steps:

1. DFS from s that *suspends* whenever it reaches a hot point (recording the partial path
   to it) or exhausts the budget or reaches d.
2. The same DFS backwards from d on the reversed graph, suspending at hot points.
3. Combine: hot points found by both searches give paths through one hub by pairing the two
   partial paths; for paths through several hubs, a DFS on the index graph with the remaining
   budget connects a hub reached from s to a hub reached from d, and the stored segments are
   concatenated.

Maintenance is a by-product of the search: when a new edge (u,v) arrives, the partial paths
that steps 1 and 2 just computed are exactly the new hub-to-hub segments the edge creates,
so they are inserted into the index; expired edges are removed through an inverted index from
edge to stored paths; the hot-point set itself drifts by 1–5 % per hour and is adjusted by
re-running the suspended DFS from newly hot vertices. In production (six-hop cycle detection
over a 48-hour sliding window, 3,000 edge updates per second on average and 20,000 at peak)
the 99.9th-percentile latency dropped from spikes of hundreds of seconds to about 15–20 ms.
Their Figure 21 carries a warning that generalises: with a low threshold (many hubs) the time
to *combine* segments in step 3 dominates, so the threshold is a real tuning parameter, and
the index graph's memory grows with the number of hub pairs.

Two older ideas explain why suspending at hubs is principled. K-Reach (Cheng et al., VLDB
2012) answers k-hop reachability from an index over a *vertex cover*: every edge has an
endpoint in the cover, so every path of length ≥ 1 passes through a cover vertex within one
step, and storing k-hop information only for cover vertices suffices. Akiba, Yano and Mizuno
(CIKM 2016) generalise the cover to *k-path covers*, vertex sets that intersect every path of
k vertices, and build them hierarchically with dynamic maintenance. A hot-point set with a low
enough threshold is, in practice, an approximate path cover for the long paths of a scale-free
graph, which is exactly what makes "search until a hub, then look up" complete for the paths
that matter.

### 5.5 DFS indexes: interval labelings and approximate transitive closures

The oldest reachability indexes are built by a depth-first traversal (Agrawal, Borgida and
Jagadish, SIGMOD 1989). Number the vertices in DFS pre-order and post-order over a spanning forest; a tree descendant t of s then has its
interval nested inside s's, `[pre(s), post(s)] ⊇ [pre(t), post(t)]`, so "t is unreachable from
s" is certain whenever the intervals do not nest, in O(1). Reachability through non-tree edges
is not captured, so a nesting interval means "maybe" and the query falls back to a search.
GRAIL (Yildirim, Chaoji and Zaki, VLDB 2010) made this practical by taking several randomised
DFS orders: t is reachable only if its interval nests in s's under *every* order, which rules
out most unreachable pairs immediately, and the fallback DFS prunes with the same labels.
Index size and construction are linear in the graph. Successors sharpened the filter: FERRARI
(2013) mixes exact and approximate intervals under a size budget, IP+ (2014) uses independent
random permutations, BFL (Su et al., 2017) stores Bloom filters of reachable sets so that
negative answers are cheap and positive ones fall back to search, and O'Reach (2020) combines
a handful of *supportive vertices* whose forward and backward reachable sets are stored
exactly with topological-order tests to answer most queries in constant time, reporting that
it speeds up every prior index it is combined with. DAGGER (2013) and DBL (2021) maintain
such indexes under updates. The SIGMOD 2023 tutorial of Zhang, Bonifati and Özsu organises the
field as a spectrum between "full online BFS" and "full offline transitive closure", with
tree-cover (interval) indexes, 2-hop indexes and approximate closures as the three points on
it for plain graphs, and alternation-based (LCR) versus concatenation-based (RPQ) indexes for
edge-labelled ones.

For a path engine these are the oracle for the *unbounded* case, where a distance is
meaningless and the question is only whether a target (or anything carrying the end label) is
reachable from v at all: a negative answer in O(1) prunes an entire region of the `*`
exploration. Their labels are over plain reachability, so they ignore edge type filters and
tombstones; like distance labels they remain sound lower bounds across deletions and must be
refreshed for insertions.

### 5.6 What transfers to TuringDB, and what does not

Ordering by leverage per unit of complexity:

1. **Hub-suspended, bidirectional enumeration for bound targets** needs no offline index at
   all. Run the DFS from s and the reverse DFS from t of Section 3.4, but suspend both at
   hubs (degree above a threshold, known from the adjacency ranges) and join the two partial-path
   sets at hubs. Hubs are the natural cut points of PathEnum's join because almost every long
   path passes through one; the hub graph is small enough to search per query with the
   remaining budget. This is GraphS's algorithm minus its persistent index, and it composes
   with the distance pruning of Section 3.3.
2. **A persistent hub-to-hub segment cache** is the natural next step when the same bound
   queries repeat (fraud monitoring, cycle detection): GraphS's maintenance-as-by-product means
   the cache fills itself from queries. It is a per-commit structure in TuringDB's model; the
   threshold controls its memory and the cost of the combination step.
3. **Distance labels as a pruning oracle** pay off when three conditions meet: many bound
   or per-row targets, a graph too large for repeated bounded BFSs, and a mostly append-only
   history (insertions maintained incrementally, deletions tolerated as sound staleness). Build
   cost is minutes to hours per graph and index size a few times the edge array; this is a
   deliberate, per-graph decision, not a default.
4. **Landmark reachability with doomed sets** fits typed `EXISTS`/`DISTINCT` queries over
   `*` patterns, where the query is a constrained reachability question; its exponential worst
   case in the number of edge types is bounded by the minimal-set trick and by indexing only
   landmarks.
5. **DFS interval labels** are the cheapest oracle (linear build, linear size) and serve
   exactly one purpose here: killing unbounded `*` branches that can never reach the target
   set.

What does not transfer: full transitive closures (quadratic), hot-point indexes with low
thresholds on graphs whose hubs are not few (the combination step explodes), and any label
that ignores the query's edge type filter or the snapshot's tombstones without being checked
against them. None of the engines compared in Section 7 ships any of these indexes inside its
variable-length operator; they are the largest untapped lever after the per-query pruning of
Section 3.

---

## 6. Making the hardware work

### 6.1 Latency, not bandwidth

A traversal is a chain of dependent memory accesses: read a node's adjacency range, read the
edges in that range, pick an edge, read the neighbour's range, and so on. Typical figures on a
current server core (approximate): an L1 hit costs about 1 ns, L2 about 4 ns, L3 10–20 ns, DRAM
80–100 ns. A graph whose adjacency arrays exceed the cache turns every hop into a DRAM
round-trip, and a single depth-first search issues those round-trips one after another
because each address depends on the previous load. The core's out-of-order window (a few
hundred instructions) is far too small to find the next *independent* traversal on its own,
so most of the time the pipeline is stalled and memory bandwidth sits idle. A modern core can
have ten or more cache misses in flight; a naive DFS uses one.

### 6.2 Memory-level parallelism: prefetching, AMAC, coroutines

The database community solved this for hash joins and index lookups, and the techniques
transfer. *Group prefetching* and *software-pipelined prefetching* (Chen, Ailamaki, Gibbons and
Mowry, early 2000s) batch independent lookups and issue prefetches for stage i+1 of all of
them while computing stage i; they work when every lookup has the same number of stages.
*Asynchronous Memory Access Chaining* (Kocberber, Falsafi and Grot, VLDB 2015) generalises
this to irregular chains: keep a small ring of in-flight lookups, each with its own state
machine; on every step, run one lookup's next stage, which ends by prefetching the address the
following stage will need, then move to the next lookup in the ring; when a lookup finishes,
load a fresh one into its slot. Because slots are independent, chains of different lengths
and early exits cause no bubbles. AMAC matched the batched schemes on regular workloads and
delivered up to 2.3× on irregular ones. Psaropoulos et al. (VLDB 2017) then showed that
writing each lookup as a coroutine that suspends after each prefetch gives the same
interleaving with code that reads like the original, and CoroBase (VLDB 2021) and GastCoCo
(2023, for dynamic graph processing) built whole engines around C++20 stackless coroutines
and software prefetch.

A depth-first path enumeration from a chunk of start nodes is an ideal AMAC workload: the
walks from different start nodes are independent chains of irregular length. Interleave K of
them: each walker's descent becomes three stages, (A) resolve which storage segment owns the
node and prefetch its adjacency-range entry, (B) read the range and prefetch the first cache
lines of the edge span, (C) scan the span and generate the frame's candidates. A scheduler
advances the K walkers round-robin one stage at a time, so while walker 1 waits for its range
to arrive, walkers 2..K issue their own loads. The output rows of different walkers interleave,
which is harmless for a relational operator. With K = 1 the stages run back to back and the
code is the plain DFS; the value of K is a tuning knob: 1 when the adjacency arrays fit in
cache (interleaving would only add scheduling overhead), 8 or so when they do not, never so
many that the walkers' own state evicts what they prefetched.

No graph database engine, to my knowledge, applies AMAC or coroutine interleaving inside its
variable-length expansion operator; Neo4j and Memgraph expand row by row, Kùzu parallelises
across threads with morsels. Interleaving is orthogonal to multithreading (each thread can
run its own ring) and is where a single-threaded interpreter gets its memory-level parallelism.

### 6.3 Layout: what the traversal reads

The bytes a traversal touches per candidate edge are the second constant factor:

- **CSR** (compressed sparse row) is the reference layout: an offsets array indexed by node
  and a contiguous neighbour array. TuringDB's per-`DataPart` layout is CSR in substance: edges
  sorted by source with a `(first, count)` range per node, plus a mirrored in-edge array. The
  twist is versioning: a node's edges may live in its own part *and* in later parts that
  patched it, so a lookup consults the owner part and every later part that holds patch nodes.
  On a bulk-loaded graph that is one part; on a graph built by many small commits it is many,
  and merging parts (or a per-commit "has patch edges" bitmap) is the lever.
- **AoS vs SoA**: an edge record holding (edge id, this node, other node, type) in 32 bytes
  serves four fields when a traversal reads two or three; a structure-of-arrays layout would
  halve the bytes streamed per candidate. Analytics engines (GAP, Ligra) store only the
  neighbour array for exactly this reason.
- **Typed traversal**: `-[:KNOWS*1..3]->` on a node with edges of twenty types touches all
  twenty types' edges and compares each one's type. Neo4j's store groups a dense node's
  relationships by type and direction (relationship groups) so that a typed expansion reads
  only its type. The equivalent in a CSR layout is to sort each node's run by type and locate
  the type's sub-range by binary search, which also lets the single-hop typed iterators fill
  contiguously.
- **Prefetchable structure**: interleaving (6.2) needs computable addresses one stage ahead.
  An array of ranges indexed by node identifier provides that; a hash-map probe (TuringDB's
  patch nodes) does not, which is one more reason to keep patch parts rare.

---

## 7. The engines compared

| System | Enumeration (all paths) | Trail/uniqueness check | Reachability / DISTINCT | Pruning by end constraint | Offline index | Memory-level parallelism | Result representation |
|---|---|---|---|---|---|---|---|
| Neo4j | `VarLengthExpand(All)`: DFS per input row over relationship iterators; node/relationship predicates inlined; typed groups for dense nodes | scan of the path's relationships | `VarLengthExpand(Pruning)` (visited nodes + budget), `(Pruning,BFS)` for min ≤ 1 | predicates on inner nodes/relationships only; no distance index | none | none | path objects, row at a time |
| Memgraph | `ExpandVariable`: DFS with a stack of edge iterators per frame; filter lambdas; hop bounds | per-path check | separate `BFSExpand`, `wShortest`, `allShortest` operators | lambdas on inner nodes/edges | none | none | row at a time |
| Kùzu | recursive join: frontier BFS, morsel-driven parallel; WALK default, TRAIL/ACYCLIC checked during enumeration | per-path check during BFS | shortest-path variants with early stop; bidirectional and direction-optimised planned | planned | none | multithreading; per-thread scalar | factorized intermediate tables, paths written tuple at a time (their own TODO) |
| MillenniumDB / PathFinder | product-graph search with constraint checks for TRAIL/SIMPLE | during traversal | BFS with path index (DAG of parent pointers), output-linear delay | automaton states prune label mismatches | none | none reported | path index |
| DuckPGQ | shortest paths only | n/a | SIMD MS-BFS over CSR, bitsets, AVX-512 | n/a | none | SIMD bit-parallelism | pairs / lengths |
| GraphS (Alibaba, research/production) | hub-suspended bidirectional DFS for s-t cycles | simple paths, per-path check | n/a | hot-point index: hub-to-hub segments, self-maintaining | yes (per query, dynamic) | none | paths streamed |
| PathEnum (research) | IDX-DFS / IDX-JOIN on a query-time distance index | simple paths, per-path check | n/a | distance index from s and t, join cut by cost model | none (query-time only) | none | paths streamed |
| ReCAP (research, 2026) | recursive CTE with viability predicates | edge-id list in the accumulator | n/a | doomed-prefix elimination on any monotone constraint | none | none | rows |
| TuringDB v2 | level-synchronous BFS, parent-pointer tree, unbounded memory | O(depth) parent walk per candidate | none | none | none | none | `EntityList` per row |
| TuringDB v3 (`PLAN.md`) | Tier 1: stack DFS over immutable part spans, candidate frames, chunked emission; Tier 3: hub-suspended join enumeration for s-t | 64-bit signature + contiguous scan | Tier 3: MS-BFS, exact for min ≤ 1, dedup fallback | Tier 2: end-label fusion + reverse-distance index with cost gate; per-row targets via MS-BFS | Tier 4: optional per-commit distance labels / hub segment cache / interval labels as pruning oracles | Tier 1: AMAC-style interleaved walkers with software prefetch | flat `ListView` when read, nothing when unread; Tier 4: factorized path column |

Three patterns stand out. First, every production engine enumerates with a per-row DFS and
checks uniqueness by scanning; none of them uses a distance index, even though PathEnum showed
five years ago that it removes about two orders of magnitude of edge visits on s-t queries and
ReCAP just showed the same principle winning by five orders of magnitude on constrained paths.
Second, the reachability toolbox (bit-parallel BFS, direction optimisation, dense/sparse
frontiers) and the whole offline-index literature (2-hop labels, landmark indexes, hot points,
interval labels) live in analytics engines, DuckPGQ and research systems, and reach the graph
databases only as separate shortest-path operators, if at all. Third, nobody hides memory
latency inside the traversal operator. Those three gaps are what `PLAN.md`'s tiers target.

---

## 8. Consequences for TuringDB

Putting the pieces together, the design decisions in `PLAN.md` follow from the sections above:

1. **DFS with an explicit stack, no path tree** (Section 2). Enumeration needs prefix sharing,
   which the stack provides; the tree's extra services (frontier deduplication, sorted access)
   are worth less than their sorting and memory cost for enumeration, and are exactly what the
   BFS-based index build and DISTINCT mode use instead.
2. **Signature trail check** (2.3): O(1) expected, exact on fallback, one word per depth.
3. **Materialize only what is read** (2.4): the column-trimming pass decides; `RETURN m` never
   builds a list. Factorized paths are the Tier 4 answer for intermediate operators.
4. **Interleaved walkers** (6.2): the state machine is the same DFS; K is a runtime knob
   driven by whether the adjacency fits in cache; needs a measurement harness to set defaults.
5. **End-constraint fusion and a reverse-distance index** (3.3, 3.5): the same bounded
   multi-source BFS serves label targets, bound targets and (with bit words) per-row targets;
   soundness under trail semantics is the lower-bound argument; a cost gate decides per query.
6. **Join enumeration for long s-t queries** (3.4), cut at hubs where the search suspends
   (5.4, 5.6), and **MS-BFS for DISTINCT with min ≤ 1** (4.1, 4.2) are separate modes chosen by
   an optimizer pass plus runtime statistics, never a silent change of semantics.
7. **Offline oracles** (5.2–5.5) are per-graph opt-ins layered under the same DFS rule:
   distance labels for many bound targets on append-mostly graphs, landmark reachability for
   typed `EXISTS`, interval labels for unbounded `*`; all sound across deletions, refreshed on
   insertions.
8. **Storage follow-ups** (6.3): type-sorted runs, SoA edges, fewer patch probes.

What to measure, in order: (a) candidate checks per emitted row and rows per second on a
cache-resident graph, to validate the Tier 1 constants against v2; (b) the same on a
generated graph an order of magnitude larger than L3, with K ∈ {1, 4, 8, 16}, to set the
interleaving default; (c) for end-constrained queries, edges touched with and without the
index as the target label's selectivity varies, to calibrate the cost gate; (d) for s-t
queries, DFS versus join versus hub-suspended join as the hop bound grows, to reproduce
PathEnum's crossover on TuringDB's layout; (e) for a persistent oracle, label build time and
size on the largest customer graph against the query volume that would amortise it.

---

## Glossary

- **Walk / trail / simple path**: edge sequence with, respectively, no restriction, no
  repeated edge, no repeated node. Cypher's variable-length patterns enumerate trails.
- **Polynomial delay**: bounded polynomial time between consecutive outputs of an enumeration.
- **Frontier**: the set of partial paths (or nodes) at the current BFS level.
- **Path tree / path index**: parent-pointer structure compressing a set of paths by shared
  prefixes; a DAG when several parents are kept (all shortest paths).
- **Factorized representation**: a result stored as shared prefixes/products instead of flat
  tuples; size bounded by distinct prefixes, not by tuple count.
- **Signature (Bloom filter)**: fixed-width bit set with hashed membership bits; no false
  negatives, tunable false positives.
- **HcPE**: hop-constrained s-t path enumeration.
- **MS-BFS**: multi-source BFS with ω searches packed into ω-bit words per node.
- **2-hop cover / hub labeling**: per-vertex lists of (hub, distance) such that every pair
  shares a hub on a shortest path; distance = min over shared hubs.
- **Pruned landmark labeling (PLL)**: builds a minimal 2-hop cover by degree-ordered BFSs
  that stop wherever existing labels already answer the distance.
- **Hot point / hub**: vertex whose degree exceeds a threshold; hot-point indexes store paths
  between hubs and suspend searches at them.
- **k-path cover**: vertex set intersecting every path of k vertices; a vertex cover is the
  k = 2 case.
- **Interval labeling**: DFS pre/post-order numbers whose nesting certifies tree reachability;
  randomised over several orders (GRAIL) to filter unreachable pairs.
- **AMAC**: asynchronous memory access chaining; interleaving independent pointer chases with
  software prefetch to raise memory-level parallelism.
- **CSR**: compressed sparse row; offsets per node into a contiguous neighbour array.
- **Direction-optimising BFS**: switching between top-down and bottom-up frontier expansion by
  frontier size.

## Reading list

Path enumeration and pruning
- Sun, Chen, He, Hooi. *PathEnum: Towards Real-Time Hop-Constrained s-t Path Enumeration.*
  SIGMOD 2021. https://arxiv.org/abs/2103.11137 — query-time distance index, IDX-DFS,
  IDX-JOIN, cost-based cut; the ~100× edge-visit reduction and 1.9–240× speedups quoted above.
  Code: https://github.com/Xtra-Computing/PathEnum
- Peng, Lin, Zhang, Qin, Zhou. *Towards Bridging Theory and Practice: Hop-Constrained s-t
  Simple Path Enumeration.* VLDB 2019 — BC-DFS barrier pruning with O(k|E|) delay; the
  baseline PathEnum beats.
- Rizzi, Sacomoto, Sagot. *Efficiently listing bounded length st-paths.* 2014; Grossi, Marino,
  Versari 2018 — polynomial-delay T-DFS variants.
- *ReCAP: Efficient Path Query Processing in Relational Database Systems.* 2026.
  https://arxiv.org/abs/2604.02553 — viability predicates on path prefixes; up to 400,000×
  over Neo4j/Memgraph/Kùzu; the 3.5M-vs-324 intermediate-path measurement.
- Yuan, Hao, Lin, Zhang. *Batch Hop-Constrained s-t Simple Path Query Processing in Large
  Graphs.* ICDE 2024. https://arxiv.org/abs/2312.01424 — sharing computation across queries
  with a common source.
- *Hop-Constrained s-t Simple Path Enumeration on Large Dynamic Graphs.* ICDE 2023 — the
  dynamic-graph variant.

Hubs, landmarks and offline indexes
- Akiba, Iwata, Yoshida. *Fast Exact Shortest-Path Distance Queries on Large Networks by Pruned
  Landmark Labeling.* SIGMOD 2013. https://arxiv.org/abs/1304.4661 — the pruned BFS, degree
  ordering, bit-parallel labels, Table 3 numbers. Code: https://github.com/iwiwi/pruned-landmark-labeling
- Akiba, Iwata, Yoshida. *Dynamic and Historical Shortest-Path Distance Queries on Large
  Evolving Networks by Pruned Landmark Labeling.* WWW 2014 — incremental insertions.
- D'Angelo, D'Emidio, Frigioni. *Fully Dynamic 2-Hop Cover Labeling.* ACM JEA 2019.
  https://dl.acm.org/doi/10.1145/3299901 — first decremental algorithm.
- Zeng, Fang, Chen, Li, Ma. *Efficient Maintenance of 2-Hop Labeling Index on Dynamic
  Small-World Graphs.* VLDB 2025. https://www.vldb.org/pvldb/vol18/p2005-zeng.pdf — M2HL,
  parallel insert/delete maintenance preserving minimality.
- Li, Qiao, Qin, Zhang, Chang, Lin. *Scaling Distance Labeling on Small-World Networks.*
  SIGMOD 2019. https://dl.acm.org/doi/10.1145/3299869.3319877 — parallel label construction;
  core–periphery successor SIGMOD 2020: https://dl.acm.org/doi/abs/10.1145/3318464.3389748
- Zhang, Yu. *Hub Labeling for Shortest Path Counting.* SIGMOD 2020.
  https://dl.acm.org/doi/10.1145/3318464.3389737 — labels with counts.
- Cohen, Halperin, Kaplan, Zwick. *Reachability and Distance Queries via 2-Hop Labels.* SODA
  2002 — the 2-hop cover.
- Potamias, Bonchi, Castillo, Gionis. *Fast Shortest Path Distance Estimation in Large
  Networks.* CIKM 2009 — landmark selection by centrality; Goldberg, Harrelson. *Computing the
  Shortest Path: A* Search Meets Graph Theory.* SODA 2005 — ALT lower bounds.
- Valstar, Fletcher, Yoshida. *Landmark Indexing for Evaluation of Label-Constrained
  Reachability Queries.* SIGMOD 2017.
  https://www.semanticscholar.org/paper/e34ae7318201b7e903d6a75a558fe3eba4463729 — top-degree
  landmarks, minimal label sets, reachable-by doomed sets. Code: https://github.com/DeLaChance/LCR
- *DLCR: Efficient Indexing for Label-Constrained Reachability Queries on Large Dynamic Graphs.*
  VLDB 2022. https://dl.acm.org/doi/10.14778/3529337.3529348
- *Fast Answering Pattern-Constrained Reachability Queries with Two-Dimensional Reachability
  Index.* 2025. https://arxiv.org/abs/2511.01025
- Qiu, Cen, Qian, Peng, Zhang, Lin, Zhou. *Real-time Constrained Cycle Detection in Large
  Dynamic Graphs.* VLDB 2018. https://www.vldb.org/pvldb/vol11/p1876-qiu.pdf — GraphS, the
  hot-point index, hub-suspended bidirectional DFS, the 93 %/99 % hub statistics.
- Cheng, Shang, Cheng, Wang, Yu. *K-Reach: Who is in Your Small World.* VLDB 2012.
  https://arxiv.org/abs/1208.0090 — vertex-cover index for k-hop reachability.
- Akiba, Yano, Mizuno. *Hierarchical and Dynamic k-Path Covers.* CIKM 2016.
  https://dx.doi.org/10.1145/2983323.2983712
- Yildirim, Chaoji, Zaki. *GRAIL: Scalable Reachability Index for Large Graphs.* VLDB 2010.
  https://dl.acm.org/doi/10.14778/1920841.1920879; Su, Zhu, Wei, Yu. *Reachability Querying:
  Can It Be Even Faster?* TKDE 2017 (BFL); Hanauer, Schulz, Trummer. *O'Reach: Even Faster
  Reachability in Large Graphs.* 2020. https://arxiv.org/abs/2008.10932
- Zhang, Bonifati, Özsu. *An Overview of Reachability Indexes on Graphs.* SIGMOD 2023
  tutorial. https://dl.acm.org/doi/10.1145/3555041.3589408

Reachability, BFS and regular path queries
- Then, Kaufmann, Chirigati, Hoang-Vu, Pham, Kemper, Neumann, Vo. *The More the Merrier:
  Efficient Multi-Source Graph Traversal.* VLDB 2014.
  https://www.vldb.org/pvldb/vol8/p449-then.pdf — MS-BFS.
- ten Wolde et al. *DuckPGQ: Bringing SQL/PGQ to DuckDB.* VLDB 2023.
  https://www.vldb.org/pvldb/vol16/p4034-wolde.pdf — SIMD MS-BFS over CSR.
- Beamer, Asanović, Patterson. *Direction-Optimizing Breadth-First Search.* SC 2012.
- Shun, Blelloch. *Ligra: A Lightweight Graph Processing Framework for Shared Memory.* PPoPP
  2013 — dense/sparse frontier switching.
- *Output-Sensitive Evaluation of Regular Path Queries.* 2024. https://arxiv.org/html/2412.07729
- Farías, Martens, Rojas, Vrgoč. *PathFinder: A unified approach for handling paths in graph
  query languages.* https://arxiv.org/abs/2306.02194 — product graph, path index, TRAIL and
  SIMPLE modes in MillenniumDB.
- Martens, Trautner. *Enumeration Problems for Regular Path Queries.* 2018.
  https://arxiv.org/abs/1710.02317; Martens, Niewerth, Trautner. *A Trichotomy for Regular
  Trail Queries.* STACS 2020 — complexity landscape of simple-path and trail semantics.
- *Semantics for Querying Paths in Graph Databases: No-repeated-node or No-repeated-edge?* —
  the walk/trail/simple debate behind the GQL path modes.

Systems
- Jin, Feng, Chen, Salihoglu et al. *KÙZU Graph Database Management System.* CIDR 2023.
  https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf — factorized processing, recursive joins.
- *Robust Recursive Query Parallelism in Graph Database Management Systems.* 2025.
  https://arxiv.org/abs/2508.19379 — source/frontier/hybrid morsel dispatch, MS-BFS as a policy.
- Kùzu recursive join TODOs: https://github.com/kuzudb/kuzu/issues/4285 — the engine's own list
  of bottlenecks (tuple-at-a-time paths, virtual calls, bidirectional joins).
- Neo4j Cypher manual, execution plan operators:
  https://neo4j.com/docs/cypher-manual/4.1/execution-plans/operators/ — `VarLengthExpand(All)`,
  `(Pruning)`, `(Pruning,BFS)` and their conditions.
- Memgraph deep path traversal: https://memgraph.com/docs/advanced-algorithms/deep-path-traversal
- Olteanu, Závodný. *Size Bounds for Factorised Representations of Query Results.* TODS 2015.

Memory-level parallelism
- Kocberber, Falsafi, Grot. *Asynchronous Memory Access Chaining.* VLDB 2015.
  https://dl.acm.org/doi/10.14778/2856318.2856321 — AMAC; up to 2.3× on irregular chains.
- Psaropoulos, Legler, May, Ailamaki. *Interleaving with Coroutines: A Practical Approach for
  Robust Index Joins.* VLDB 2017 (journal version 2019).
- He, Lu, Wang. *CoroBase: Coroutine-Oriented Main-Memory Database Engine.* VLDB 2021.
- *GastCoCo: Graph Storage and Coroutine-Based Prefetch Co-Design for Dynamic Graph Processing.*
  2023. https://arxiv.org/abs/2312.14396
- Chen, Ailamaki, Gibbons, Mowry. *Improving Hash Join Performance through Prefetching.* ICDE
  2004 — group prefetching and software pipelining, the schemes AMAC generalises.

## Bibliography

Every work, standard and documentation page referenced in this document, in alphabetical
order of first author (title first where the author list could not be verified from the
source consulted).

1. Abraham, I., Delling, D., Goldberg, A. V., Werneck, R. F. *A Hub-Based Labeling Algorithm
   for Shortest Paths in Road Networks.* SEA 2011, LNCS 6630, pp. 230–241.
   https://dl.acm.org/doi/10.5555/2008623.2008645
2. Agrawal, R., Borgida, A., Jagadish, H. V. *Efficient Management of Transitive Relationships
   in Large Data and Knowledge Bases.* SIGMOD 1989, pp. 253–262.
3. Akiba, T., Iwata, Y., Yoshida, Y. *Fast Exact Shortest-Path Distance Queries on Large
   Networks by Pruned Landmark Labeling.* SIGMOD 2013, pp. 349–360.
   https://arxiv.org/abs/1304.4661 — code: https://github.com/iwiwi/pruned-landmark-labeling
4. Akiba, T., Iwata, Y., Yoshida, Y. *Dynamic and Historical Shortest-Path Distance Queries on
   Large Evolving Networks by Pruned Landmark Labeling.* WWW 2014, pp. 237–248.
5. Akiba, T., Yano, Y., Mizuno, N. *Hierarchical and Dynamic k-Path Covers.* CIKM 2016,
   pp. 1543–1552. https://dx.doi.org/10.1145/2983323.2983712
6. Beamer, S., Asanović, K., Patterson, D. *Direction-Optimizing Breadth-First Search.*
   SC 2012.
7. Beamer, S., Asanović, K., Patterson, D. *The GAP Benchmark Suite.* arXiv:1508.03619, 2015.
8. Chen, S., Ailamaki, A., Gibbons, P. B., Mowry, T. C. *Improving Hash Join Performance
   through Prefetching.* ICDE 2004; extended version ACM TODS 32(3), 2007.
9. Cheng, J., Shang, Z., Cheng, H., Wang, H., Yu, J. X. *K-Reach: Who is in Your Small World.*
   PVLDB 5(11), pp. 1292–1303, 2012. https://arxiv.org/abs/1208.0090
10. Cohen, E., Halperin, E., Kaplan, H., Zwick, U. *Reachability and Distance Queries via 2-Hop
    Labels.* SODA 2002; SIAM Journal on Computing 32(5), pp. 1338–1355, 2003.
11. D'Angelo, G., D'Emidio, M., Frigioni, D. *Fully Dynamic 2-Hop Cover Labeling.* ACM Journal
    of Experimental Algorithmics 24, 2019. https://dl.acm.org/doi/10.1145/3299901
12. *DLCR: Efficient Indexing for Label-Constrained Reachability Queries on Large Dynamic
    Graphs.* PVLDB 15(8), 2022. https://dl.acm.org/doi/10.14778/3529337.3529348
13. Farías, B., Martens, W., Rojas, C., Vrgoč, D. *PathFinder: A Unified Approach for Handling
    Paths in Graph Query Languages.* arXiv:2306.02194. https://arxiv.org/abs/2306.02194
14. *Fast Answering Pattern-Constrained Reachability Queries with Two-Dimensional Reachability
    Index.* arXiv:2511.01025, 2025. https://arxiv.org/abs/2511.01025
15. Feng, X., Jin, G., Chen, Z., Liu, C., Salihoğlu, S. *KÙZU Graph Database Management
    System.* CIDR 2023. https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf
16. *GastCoCo: Graph Storage and Coroutine-Based Prefetch Co-Design for Dynamic Graph
    Processing.* arXiv:2312.14396, 2023. https://arxiv.org/abs/2312.14396
17. Goldberg, A. V., Harrelson, C. *Computing the Shortest Path: A\* Search Meets Graph
    Theory.* SODA 2005, pp. 156–165.
18. Grossi, R., Marino, A., Versari, L. *Efficient Algorithms for Listing k Disjoint st-Paths in
    Graphs.* LATIN 2018, pp. 544–557.
19. Hanauer, K., Schulz, C., Trummer, J. *O'Reach: Even Faster Reachability in Large Graphs.*
    SEA 2020; ACM Journal of Experimental Algorithmics 27, 2022. https://arxiv.org/abs/2008.10932
20. He, Y., Lu, J., Wang, T. *CoroBase: Coroutine-Oriented Main-Memory Database Engine.*
    PVLDB 14(3), pp. 431–444, 2021. http://vldb.org/pvldb/vol14/p431-he.pdf
21. *Hop-Constrained s-t Simple Path Enumeration on Large Dynamic Graphs.* ICDE 2023.
    https://fanzhangcs.github.io/papers/2023_icde_simplepath.pdf
22. ISO/IEC 39075:2024. *Information technology — Database languages — GQL.* (path modes
    WALK, TRAIL, SIMPLE, ACYCLIC; selectors ANY, ALL SHORTEST.)
23. ISO/IEC 9075-16:2023. *Information technology — Database languages — SQL — Part 16:
    Property Graph Queries (SQL/PGQ).*
24. Jin, R., Hong, H., Wang, H., Ruan, N., Xiang, Y. *Computing Label-Constraint Reachability
    in Graph Databases.* SIGMOD 2010, pp. 123–134.
25. Kocberber, O., Falsafi, B., Grot, B. *Asynchronous Memory Access Chaining.* PVLDB 9(4),
    pp. 252–263, 2015. https://dl.acm.org/doi/10.14778/2856318.2856321
26. Kùzu documentation. *MATCH clause: recursive relationship patterns (WALK, TRAIL,
    ACYCLIC).* https://docs.kuzudb.com/cypher/query-clauses/match/
27. Kùzu issue #4285. *GDS and Recursive Joins TODOs.* https://github.com/kuzudb/kuzu/issues/4285
28. Li, W., Qiao, M., Qin, L., Zhang, Y., Chang, L., Lin, X. *Scaling Distance Labeling on
    Small-World Networks.* SIGMOD 2019, pp. 1060–1077.
    https://dl.acm.org/doi/10.1145/3299869.3319877
29. Li, W., Qiao, M., Qin, L., Zhang, Y., Chang, L., Lin, X. *Scaling Up Distance Labeling on
    Graphs with Core-Periphery Properties.* SIGMOD 2020.
    https://dl.acm.org/doi/abs/10.1145/3318464.3389748
30. Lyu, Q., Li, Y., He, B., Gong, B. *DBL: Efficient Reachability Queries on Dynamic Graphs.*
    DASFAA 2021; arXiv:2101.09441. https://arxiv.org/abs/2101.09441
31. Martens, W., Trautner, T. *Enumeration Problems for Regular Path Queries.* ICDT 2018.
    https://arxiv.org/abs/1710.02317
32. Martens, W., Niewerth, M., Trautner, T. *A Trichotomy for Regular Trail Queries.*
    STACS 2020.
33. Memgraph documentation. *Deep path traversal algorithms.*
    https://memgraph.com/docs/advanced-algorithms/deep-path-traversal
34. Mendelzon, A. O., Wood, P. T. *Finding Regular Simple Paths in Graph Databases.* SIAM
    Journal on Computing 24(6), pp. 1235–1258, 1995.
35. Murphy, R. C., Wheeler, K. B., Barrett, B. W., Ang, J. A. *Introducing the Graph 500.*
    Cray User Group 2010. https://graph500.org
36. Neo4j. *Cypher Manual: Execution plan operators* (`VarLengthExpand(All)`,
    `VarLengthExpand(Pruning)`, `VarLengthExpand(Pruning,BFS)`).
    https://neo4j.com/docs/cypher-manual/4.1/execution-plans/operators/
37. Olteanu, D., Závodný, J. *Size Bounds for Factorised Representations of Query Results.*
    ACM TODS 40(1), 2015.
38. openCypher. *Cypher Query Language Reference.* https://opencypher.org
39. *Output-Sensitive Evaluation of Regular Path Queries.* arXiv:2412.07729, 2024.
    https://arxiv.org/html/2412.07729
40. Peng, Y., Zhang, Y., Lin, X., Zhang, W., Qin, L., Zhou, J. *Towards Bridging Theory and
    Practice: Hop-Constrained s-t Simple Path Enumeration.* PVLDB 13(4), pp. 463–476, 2019.
    Extended version: *Efficient Hop-Constrained s-t Simple Path Enumeration.* The VLDB
    Journal, 2021. https://dl.acm.org/doi/10.1007/s00778-021-00674-5
41. Potamias, M., Bonchi, F., Castillo, C., Gionis, A. *Fast Shortest Path Distance Estimation
    in Large Networks.* CIKM 2009, pp. 867–876.
42. Psaropoulos, G., Legler, T., May, N., Ailamaki, A. *Interleaving with Coroutines: A
    Practical Approach for Robust Index Joins.* PVLDB 11(2), pp. 230–242, 2017. Journal
    version: *Interleaving with Coroutines: A Systematic and Practical Approach to Hide Memory
    Latency in Index Joins.* The VLDB Journal 28, 2019.
43. Qiu, X., Cen, W., Qian, Z., Peng, Y., Zhang, Y., Lin, X., Zhou, J. *Real-time Constrained
    Cycle Detection in Large Dynamic Graphs.* PVLDB 11(12), pp. 1876–1888, 2018.
    https://www.vldb.org/pvldb/vol11/p1876-qiu.pdf
44. *ReCAP: Efficient Path Query Processing in Relational Database Systems.* arXiv:2604.02553,
    2026. https://arxiv.org/abs/2604.02553
45. Rizzi, R., Sacomoto, G., Sagot, M.-F. *Efficiently Listing Bounded Length st-Paths.*
    IWOCA 2014, LNCS 8986, pp. 318–329.
46. *Robust Recursive Query Parallelism in Graph Database Management Systems.*
    arXiv:2508.19379, 2025. https://arxiv.org/abs/2508.19379
47. *Semantics for Querying Paths in Graph Databases: No-repeated-node or No-repeated-edge?*
    https://www.academia.edu/118456575/
48. Seufert, S., Anand, A., Bedathur, S., Weikum, G. *FERRARI: Flexible and Efficient
    Reachability Range Assignment for Graph Indexing.* ICDE 2013, pp. 1009–1020.
49. Shun, J., Blelloch, G. E. *Ligra: A Lightweight Graph Processing Framework for Shared
    Memory.* PPoPP 2013.
50. Su, J., Zhu, Q., Wei, H., Yu, J. X. *Reachability Querying: Can It Be Even Faster?* IEEE
    TKDE 29(3), pp. 683–697, 2017.
51. Sun, S., Chen, Y., He, B., Hooi, B. *PathEnum: Towards Real-Time Hop-Constrained s-t Path
    Enumeration.* SIGMOD 2021. https://arxiv.org/abs/2103.11137 — code:
    https://github.com/Xtra-Computing/PathEnum
52. ten Wolde, D., Szárnyas, G., Boncz, P. *DuckPGQ: Bringing SQL/PGQ to DuckDB.* PVLDB 16(12),
    pp. 4034–4037, 2023. https://www.vldb.org/pvldb/vol16/p4034-wolde.pdf
53. Then, M., Kaufmann, M., Chirigati, F., Hoang-Vu, T.-A., Pham, K., Kemper, A., Neumann, T.,
    Vo, H. T. *The More the Merrier: Efficient Multi-Source Graph Traversal.* PVLDB 8(4),
    pp. 449–460, 2014. https://www.vldb.org/pvldb/vol8/p449-then.pdf
54. Valiant, L. G. *The Complexity of Enumeration and Reliability Problems.* SIAM Journal on
    Computing 8(3), pp. 410–421, 1979.
55. Valstar, L. D. J., Fletcher, G. H. L., Yoshida, Y. *Landmark Indexing for Evaluation of
    Label-Constrained Reachability Queries.* SIGMOD 2017, pp. 345–358.
    https://www.semanticscholar.org/paper/e34ae7318201b7e903d6a75a558fe3eba4463729 — code:
    https://github.com/DeLaChance/LCR
56. Vrgoč, D., Rojas, C., Angles, R., Arenas, M., Arroyuelo, D., Buil-Aranda, C., Hogan, A.,
    Navarro, G., Riveros, C., Romero, J. *MillenniumDB: A Persistent, Open-Source, Graph
    Database.* arXiv:2111.01540. https://arxiv.org/abs/2111.01540
57. Wei, H., Yu, J. X., Lu, C., Jin, R. *Reachability Querying: An Independent Permutation
    Labeling Approach.* PVLDB 7(12), pp. 1191–1202, 2014.
58. Yildirim, H., Chaoji, V., Zaki, M. J. *GRAIL: Scalable Reachability Index for Large Graphs.*
    PVLDB 3(1), pp. 276–284, 2010; journal version The VLDB Journal 21, 2012.
    https://dl.acm.org/doi/10.14778/1920841.1920879
59. Yildirim, H., Chaoji, V., Zaki, M. J. *DAGGER: A Scalable Index for Reachability Queries in
    Large Dynamic Graphs.* arXiv:1301.0977, 2013. https://arxiv.org/abs/1301.0977
60. Yuan, L., Hao, K., Lin, X., Zhang, W. *Batch Hop-Constrained s-t Simple Path Query
    Processing in Large Graphs.* ICDE 2024, pp. 2557–2569. https://arxiv.org/abs/2312.01424
61. Zeng, Y., Fang, Y., Chen, K., Li, Y., Ma, C. *Efficient Maintenance of 2-Hop Labeling Index
    on Dynamic Small-World Graphs.* PVLDB 18(7), pp. 2005–2017, 2025.
    https://www.vldb.org/pvldb/vol18/p2005-zeng.pdf
62. Zhang, C., Bonifati, A., Özsu, M. T. *An Overview of Reachability Indexes on Graphs.*
    SIGMOD 2023 Companion (tutorial). https://dl.acm.org/doi/10.1145/3555041.3589408
63. Zhang, Y., Yu, J. X. *Hub Labeling for Shortest Path Counting.* SIGMOD 2020.
    https://dl.acm.org/doi/10.1145/3318464.3389737
64. Zou, L., Xu, K., Yu, J. X., Chen, L., Xiao, Y., Zhao, D. *Efficient Processing of
    Label-Constraint Reachability Queries Using Spanning Trees.* Information Systems 40,
    pp. 47–66, 2014.

Internal TuringDB documents referenced: `PLAN.md` (the implementation plan this synthesis
supports), `docs/subroutine_interpreter.md` (the nl-dialect interpreter design),
`query/pipeline/processors/PathExplorerProcessor.cpp` (the v2 BFS path explorer), and
`test/query-test-suite/tests/variable-length-paths-*.json` (the behavioural oracle).
