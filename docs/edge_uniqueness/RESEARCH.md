# Edge uniqueness: what the engines and the literature do

The companion of `PLAN.md`. Everything here was read on 2026-09-29 from the sources linked;
a claim marked *(unverified)* is one the source did not settle. `docs/vlp/PATH_RESEARCH.md`
covers the variable-length side (section 1.2 on walk, trail and simple path, 2.3 on the
trail check); this note covers the rule as it applies across a whole `MATCH` clause, and
how each engine pays for it.

## 1. The rule and its baseline

Francis et al., "Cypher: An Evolving Query Language for Property Graphs" (SIGMOD 2018,
https://homepages.inf.ed.ac.uk/libkin/papers/sigmod18.pdf, section 4.2): a path satisfies a
rigid pattern only if all its relationships are distinct, and a variable-length pattern is
the union of its rigid expansions, so the rule holds over the whole pattern of one clause,
not hop by hop. Angles et al., "Foundations of Modern Query Languages for Graph Databases"
(ACM CSUR 2017) name the four semantics - homomorphism, no repeated node, no repeated edge,
no repeated anything - and place Cypher on no repeated edge. openCypher issue 174
(https://github.com/opencypher/openCypher/issues/174) proposes making the choice
configurable; it has not been adopted.

Marton, Szárnyas and Varró, "Formalising openCypher Graph Queries in Relational Algebra"
(ADBIS 2017, https://arxiv.org/pdf/1705.02844) write the naive implementation as one
*all-different* operator appended to the clause: a selection on the conjunction of
`e_i <> e_j` over every pair. That is the filter-at-the-end this plan is measured against.

GQL and SQL/PGQ chose the other default. Deutsch et al., "Graph Pattern Matching in GQL and
SQL/PGQ" (SIGMOD 2022, https://arxiv.org/pdf/2112.06217) separate *restrictors* (`WALK`,
`TRAIL`, `SIMPLE`, `ACYCLIC`), which act during matching, from *selectors* (`ANY`, `ALL
SHORTEST`), which act after. The GQL default is match mode `REPEATABLE ELEMENTS` with path
mode `WALK`; `DIFFERENT EDGES` restores Cypher's rule and turns every `WALK` path of the
pattern into a `TRAIL` (https://learn.microsoft.com/en-us/fabric/graph/gql-graph-patterns,
which also states outright that "for SIMPLE and ACYCLIC, edge uniqueness follows from node
uniqueness"). Neo4j exposes both: `DIFFERENT RELATIONSHIPS` is its default, `REPEATABLE
ELEMENTS` arrived in 2025.06 and requires every quantifier bounded, `TRAIL` and `ACYCLIC`
path modes in 2026.03 on quantified path patterns only
(https://neo4j.com/docs/cypher-manual/25/patterns/reference/match-modes-and-path-modes/).

Complexity only bites unbounded paths. Regular *simple* path queries are NP-complete
already for `a*ba*` (Mendelzon and Wood 1995); Martens, Niewerth and Trautner, "A Trichotomy
for Regular Trail Queries" (STACS 2020, https://arxiv.org/pdf/1903.00226) show the
tractable class for trails is strictly larger than for simple paths, since a loop can be cut
from a trail without changing its label. Figueira et al., "Complexity of Evaluating GQL
Queries" (2024, https://arxiv.org/abs/2407.06766): P^NP[log]-complete in data complexity with
a restrictor, NL-complete without. A bounded `*1..k` and every fixed pattern are polynomial
in their output.

## 2. What the engines do

**Neo4j** rewrites the rule into ordinary predicates before planning. `AddUniquenessPredicates`
(front-end rewriter, community `dev` branch,
https://github.com/neo4j/neo4j/blob/dev/community/cypher/front-end/rewriting/src/main/scala/org/neo4j/cypher/internal/rewriting/rewriters/AddUniquenessPredicates.scala)
emits, for every pair of relationship positions of a `MATCH`, `MERGE` or quantified path
pattern: `DifferentRelationships(x, y)` for two single relationships, `Not(In(x, list))` for
a single one against a variable-length group, `Disjoint(xs, ys)` for two groups, and one
`Unique(list)` per group for the trail rule inside it; the same variable named twice becomes
the literal `False`. One static elimination exists: `SingleRelationship.isAlwaysDifferentFrom`
evaluates the two type expressions (`:A`, `:A|B`, `!A`, `&`) and drops the predicate when
the type sets cannot overlap. Nothing reads the schema, the direction or the data. The
predicates are then planned like any selection and land as a `Filter` after the expand that
binds the second relationship. Inside `VarLengthExpandPipe` the path holds a persistent
immutable long set (`RelationshipContainer`), O(1) append and contains with structural
sharing between sibling paths; `PruningVarLengthExpandPipe` keeps a plain `long[]` and scans
it; `BFSPruningVarExpandCursor` keeps a node-level seen set and documents that it "only
works for directed searches when we don't need to keep track of relationship uniqueness
along the path", which is the walk-equals-trail-for-reachability argument of
`PATH_RESEARCH.md` 4.1. `TrailPipe` (quantified path patterns) copies the running hash set
of seen relationships once per repetition and seeds it from the relationships bound earlier
in the clause. The Königsberg page
(https://neo4j.com/docs/cypher-manual/current/patterns/unique-relationship-paths/) is the
documented example: seven hops over seven bridges return no row.

**Memgraph** plans `EdgeUniquenessFilter(expand_symbol, previous_symbols)` directly after
every `Expand` and `ExpandVariable` (`RuleBasedPlanner::EnsureCyphermorphism`, documented as
"cyphermorphism", https://memgraph.com/docs/querying/query-plan). Its cursor compares the
new edge against each previously bound edge symbol of the clause, recursing into the lists
of variable-length edges: a per-row linear scan. `ExpandVariableCursor` tests each candidate
with `any_of` over the edges on the frame's path. No static elimination.

**Kùzu** defaults to `WALK` for fixed patterns as well as paths
(https://kuzudb.github.io/docs/cypher/difference/); `TRAIL` and `ACYCLIC` are opt-in, and
`ACYCLIC` exempts the source and the destination. Its recursive join is a frontier BFS that
records a chain of `(iteration, edge, parent)` per reached node and enforces nothing while
it runs; `PathsOutputWriter::dfsSlow` applies the trail or acyclic rule when it unrolls the
parent chains, by a linear scan of the path stack, and `dfsFast` skips every check under
`WALK`. Issue 4285 lists pushing the restrictors into the enumeration as future work. This
is why ladybug (the Kùzu fork) counts 89,068 and 390,348 where v3's trail count is 86,520
and memgraph's clause-wide rule gives 198,362 (`docs/path_bench.md`). Chakraborty and
Salihoglu, "Robust Recursive Query Parallelism in GDBMSs" (VLDB 2025,
https://www.vldb.org/pvldb/vol18/p4465-chakraborty.pdf) covers its parallelism, not its
semantics.

**DuckPGQ** (Wolde et al., CIDR 2023, https://www.cidrdb.org/cidr2023/papers/p66-wolde.pdf)
supports `ANY SHORTEST` only and says a bounded `ALL TRAIL` or `ACYCLIC` would be
"translated into plain unions, joins and filters", the all-different baseline; it also
notes that `SHORTEST TRAIL` breaks the decomposition of a two-segment path into two
shortest-path problems that `WALK` allows.

**Apache AGE** (`src/backend/utils/adt/age_vle.c`) documents the rule in its header - "any
optimization that tracks visited vertices as a filter rather than visited edges is
therefore incorrect" - and runs a DFS with a stack of edge ids, a hash table of edge state
with a flag set on push and cleared on backtrack, a linear scan of the stack as an early
skip before the hash probe, and candidate lookups batched eight at a time so the probes
overlap their cache misses. **NebulaGraph** matches trails
(https://docs.nebula-graph.io/master/3.ngql-guide/7.general-query-statements/2.match/) and
checks each candidate by a linear scan of the path's edge list; joining a new segment onto
materialized prefixes is a nested scan of both. **Gremlin's** `simplePath()` is an all-pairs
`equals` over every element of the path, nodes and edges, so it is stronger than Cypher's
rule and quadratic. **TigerGraph** returns only shortest paths for Kleene-star patterns;
what it does with a repeated edge in a fixed pattern is *(unverified)*.

Mandarapu and Kunkunuru, "Same Pattern, Different Answer" (arXiv 2609.23032, 2026) run
seventeen path constructs over Kùzu 0.11.3, DuckDB 1.4.1, Neo4j 5.26 and 2026.04, Memgraph
5.9 and AGE 1.8: nine disagree, fifteen of the twenty-six disagreements silently; one
bounded walk returns 4 walks in Kùzu, 2 trails in the other three, 1 row in DuckPGQ.

## 3. What can be eliminated statically

Published work stops at Neo4j's type rule. Sharma et al., "Schema-Based Query Optimisation
for Graph Databases" (PACMMOD 2025, https://arxiv.org/abs/2403.01863) infer types through
recursive queries to prune impossible label paths, and Arturi et al., "Seeing the Trees for
the Forest" (2026, https://arxiv.org/abs/2603.12476) exploit tree-shaped substructures with
structural indexes; neither drops a uniqueness check. No paper was found that proves two
relationship positions distinct from the acyclicity of a schema or of the data
*(unverified absence)*. The rules `PLAN.md` derives are elementary and stand on their own:

- Distinct nodes force distinct edges, always, multigraph included: an edge has fixed
  endpoints. The converse holds only per type in a simple graph, so a node-only test may
  serve as a guard but not as the check where parallel edges exist.
- `e_i = e_j` in a directed walk forces `u_i = u_j` and `v_i = v_j`: two consecutive
  same-direction hops share an edge only through a self-loop, `(a)->(b)->(c)->(d)` shares
  its first and third only around a 2-cycle, and `(a)-[e1]->(b)<-[e2]-(c)` shares iff
  `a = c`.
- A same-direction chain over types whose edges form a DAG never revisits a node, hence
  never an edge.
- Disjoint type sets never coincide: Neo4j's rule.

## 4. How the check is paid for

The structures in use: a flat array of edge ids scanned per candidate (Neo4j's pruning
variants, Memgraph, Kùzu, Nebula, AGE's early skip), branch-free and vector-friendly up to
a dozen entries; a persistent set (Neo4j's expand), O(1) but one heap object per row; a hash
set copied per repetition (Neo4j's `TrailPipe`); a flag table with backtracking (AGE); a
node-level visited set (Neo4j's BFS pruning, Kùzu's frontier), sound only when the query
wants end nodes and not paths. On the cheap tests: an XOR of identifiers is not a
distinctness test, since `a ^ b ^ c` cancels on sets with no repeat; neither is sum with
sum of squares (`{1, 4, 4}` and `{2, 2, 5}` collide). The sound cheap forms are a compare
against the few ids held in registers, and a per-row Bloom word used as a negative filter
where a clear bit proves the element new.

One refinement that no engine read here applies, and that `PLAN.md` takes up as the
shared-endpoint test: a repeat at hop `j` is possible only if the hop's source node already
coincides with an endpoint of an earlier edge of the row (its source, for two
same-direction hops; either endpoint otherwise). That is one node compare per *source
row*, taken before the adjacency run is read, where every engine above compares per
*candidate edge*; a run whose source node is new needs no edge compare at all.

The cost is materialization, not comparison. ArcadeDB issue 8537
(https://github.com/ArcadeData/arcadedb/issues/8537) measures a three-hop `KNOWS` pattern
at 14.7 to 15.6 ms against 8.8 to 9.8 ms through its edge-object API and 5.2 to 6.1 ms over
adjacency alone, 1.6 to 1.7x spent loading edge records only to compare their identities;
the proposed fix reads the edge id off the adjacency segment, which `EdgeRecord` already
holds in TuringDB. Correa and Riedewald, "Efficient Path Query Processing in Relational
Database Systems" (2026, https://arxiv.org/html/2604.02553) keep an `edge_ids` array per
path row in a recursive CTE, note that Neo4j and Memgraph enforce trails during matching
where Kùzu and DuckDB enforce them afterwards, and measure a negligible cardinality
difference between the two on their sparse, short-path workloads. Gupta, Mhedhbi and
Salihoglu, "Columnar Storage and List-based Processing for GDBMSs" (VLDB 2021,
https://www.vldb.org/pvldb/vol14/p2491-gupta.pdf) is the factorized-execution reference:
the compare belongs on the source-row-by-adjacency product as a selection step, before the
target, edge and property columns are written, not in a separate filter per pair.

LDBC SNB does not isolate the check: its interactive and BI paths are `shortestPath` and
weighted shortest paths (https://github.com/ldbc/ldbc_snb_interactive_v1_impls). No public
benchmark of the uniqueness check alone was found *(unverified absence)*, which is why
`PLAN.md` defines its own.
