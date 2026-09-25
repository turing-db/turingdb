# Variable-length paths on a bank-fraud graph: v2, v3 and memgraph

The graph is `gen-fraud-graph`, Santander's open-source fraud graph generator
(`turing-db/gen-fraud-graph`), at the scale its own benchmark uses: 1,000,000 `:Account`
nodes joined by 9,000,550 `:TRANSFER` edges, mean out-degree 9, with 100 cyclic
laundering rings of depth 4 to 7 injected as the 550 `is_fraud` edges.

That benchmark (`benchmark/benchmark.py` in the repo) writes every chain as an explicit
hop sequence, so it never reaches the quantifier. This one asks the same questions with
variable-length paths and adds the shapes no fixed-hop query can express: a fraud closure
of unbounded length, and a ring found by walking back to the account the walk started
from.

Measured with `bench_fraud_paths.py`, which then ran 55 Cypher queries in five groups
through both engines in the turingdb shell - v2 directly, v3 behind the `#v3` prefix - and
reported the median of five runs per query, the first discarded as a warmup. v2 has since
been removed, and the script now runs the v3 queries alone.

Provenance: `086c4b6cf`, whose engine sources are those of `9e9704046` (the sparse reach
table, no distinct gate), default `-O3` build, 20-core Linux box, quiet machine.

```
gen-fraud-graph --scale 0.1 --format parquet --output ./fraud_1m_parquet
cp ./fraud_1m_parquet/{nodes,edges}.parquet ~/.turing-fraud/data/fraud_1m/
echo "LOAD PARQUET 'fraud_1m' AS fraud_1m" | turingdb -turing-dir ~/.turing-fraud

bench/santander_fraud/bench_fraud_paths.py -reps 5 -verify
```

`LOAD PARQUET` imports the graph in 14.8 s. Both engines agreed on every result they both
produced.

## A. Seeded fan-out - the shape the published benchmark measures

Account `796580` starts one of the rings. Its fan-out is the published benchmark's
queries 1 to 4, written there as one hop per line.

| Question | v2 | v3 | | rows |
|---|---|---|---|---|
| Find the seed account by id | 14.70 ms | 1.04 ms | **14×** | 1 |
| Exactly 1 transfer out, as written hops | 15.01 | 1.08 | **14×** | 9 |
| …as a quantifier | `ANALYZE_ERROR` | 1.39 | — | 9 |
| Exactly 2 transfers out, as written hops | 15.02 | 1.12 | **13×** | 91 |
| …as a quantifier | `ANALYZE_ERROR` | 1.35 | — | 91 |
| Exactly 3 transfers out, as written hops | 15.55 | 1.22 | **13×** | 844 |
| …as a quantifier | `ANALYZE_ERROR` | 1.45 | — | 844 |
| Exactly 4 transfers out, as written hops | 15.99 | 1.38 | **12×** | 7,554 |
| …as a quantifier | `ANALYZE_ERROR` | 1.88 | — | 7,554 |
| Up to 4 transfers out | 16.15 | 1.87 | **8.6×** | 8,498 |

**Almost none of this is the path engine.** v2 spends 14.7 ms finding one account by
`account_id` in a 1M-node scan and 0.3 to 1.5 ms walking; v3 turns the seed lookup into a
ranged property lookup and pays 1.04 ms for the same thing. What the group does measure is
the quantifier's constant against a hand-written chain of the same depth: 0.31 ms at depth
1 falling to 0.50 ms at depth 4, on identical row counts. The quantifier is not free, and
at these sizes it is most of the query - but it is a fixed cost, not a factor.

v2 refuses every typed quantifier (`ANALYZE_ERROR: Edge type filters are not supported
with variable-length paths yet`), which on a graph with one edge type costs it nothing it
cannot get from the untyped form; on a real bank schema with `TRANSFER`, `CARD_PAYMENT`
and `WIRE` it would cost it the query.

## B. Whole-graph fraud chains - the published benchmark's own queries

Its queries 5 to 7 ask for chains of two, three and four transfers where every edge is a
ring edge. Written out, that is one named relationship per hop and a conjunction of
`is_fraud` tests; as a quantifier it is one inline hop predicate.

| Question | v2 | v3 | | rows |
|---|---|---|---|---|
| 2-hop fraud chain, as written hops | 184.23 ms | 143.85 ms | 1.3× | 550 |
| …as a quantifier | `ANALYZE_ERROR` | 192.78 | — | 550 |
| 3-hop fraud chain, as written hops | 200.81 | 150.61 | 1.3× | 550 |
| …as a quantifier | `ANALYZE_ERROR` | 200.19 | — | 550 |
| 4-hop fraud chain, as written hops | 199.99 | 145.40 | 1.4× | 550 |
| …as a quantifier | `ANALYZE_ERROR` | 193.64 | — | 550 |
| Fraud chains of up to 4 transfers | `ANALYZE_ERROR` | 192.34 | — | 2,200 |
| Every fraud chain there is, any length | `ANALYZE_ERROR` | 180.83 | — | 3,166 |
| Every account a fraud chain reaches | `ANALYZE_ERROR` | 166.38 | — | 550 |
| 2 high-value hops into a high-risk account | 209.00 | 239.03 | 0.9× | 62 |

Every one of these is a 9M-edge scan with a hop predicate, so they all cost the same 150
to 200 ms whatever the depth - the walk after the first hop is 550 edges wide and free.
**The quantifier costs about 45 ms more than the written chain** (192.78 against 143.85):
the written form filters `t1.is_fraud` as a predicate on the scan, where the quantified
form evaluates the same test inside the walk, once per candidate. That is the group's one
clear message: when the first hop is a whole-graph scan, hoisting its hop predicate out of
the walk is worth a third of the query.

The last row is the only place v3 is slower than v2 on this graph, by 14 %. It is the
published benchmark's query 8 with the two hops folded into one quantified pattern, and
the 90 M candidate transfers reach the `risk_score` filter as rows rather than being cut
at the frame.

The three rows v2 cannot express are the point of the group. `+` over the ring edges
enumerates every fraud chain in the graph - 3,166 of them, all lengths - in 180.83 ms.
There is no fixed-hop query that asks that: the rings are 4 to 7 long and a chain may
enter one at any account.

## C. Laundering rings - the typology the generator injects

The generator's fraud is a cycle: money leaves an account and comes back to it through 4
to 7 intermediaries. Written as a path that ends where it began, that is one query, and
v2 can express none of it - typed quantifier, hop predicate, and an end bound to the start
variable, each of which it refuses on its own.

| Question | v2 | v3 | rows |
|---|---|---|---|
| Does account 796580 sit on a ring | `ANALYZE_ERROR` | 1.46 ms | 1 |
| How long its ring is, `size(t)` | `ANALYZE_ERROR` | 1.58 | 5 hops |
| **Every account on a ring, whole graph** | `ANALYZE_ERROR` | **1,280.07** | **550** |
| Every account on a ring, no depth bound | `ANALYZE_ERROR` | 1,266.11 | 550 |
| Every ring's length, whole graph | `ANALYZE_ERROR` | 1,231.30 | 4 groups |
| Every account on a 3-transfer ring, any transfer | `PLAN_ERROR` | 23,675.85 | 827 |

```cypher
MATCH (a:Account)-[t:TRANSFER WHERE t.is_fraud = true]->{1,7}(a) RETURN size(t), count(a)
```

finds all 550 ring members across 1M accounts and 9M transfers in 1.28 s, and its
histogram - 116 accounts on rings of length 4, 105 of 5, 126 of 6, 203 of 7 - is exactly
the generator's own `fraud_cases.parquet` metadata: 29 rings of depth 4, 21 of 5, 21 of 6,
29 of 7. Dropping the bound entirely costs nothing (1,266 ms), because a trail cannot
repeat an edge and the rings close.

**Closing the walk onto its own start variable costs a flat 900 ms.** The ring query is
the fraud closure of group B with `(b)` replaced by `(a)`, and that one substitution takes
it from 180.83 ms to 1,266.11 ms on a walk that is 550 edges wide after the first hop.
Swept by bound, the extra cost does not move:

| bound | free end `(b)` | cycle `(a)` | rows |
|---|---|---|---|
| `{1,1}` | 159.43 ms | 1,056.59 ms | 0 |
| `{1,2}` | — | 1,053.83 ms | 0 |
| `{1,4}` | — | 1,079.75 ms | 116 |
| `{1,7}` | — | 1,059.05 ms | 550 |

At `{1,1}` the walk is a single hop and the answer is 0, and it still costs 1.06 s. So the
900 ms is not traversal: it is a fixed cost per seed row, about 0.9 µs across the 1M
accounts, for having the end bound to the start variable. A single seed pays 1.28 ms,
which is the ordinary query floor.

That is the opposite of what should happen. The end is *known per row* - it is the seed -
which is exactly the case `end_column` and `PathTargetIndex` exist to exploit, so binding
it ought to make the query cheaper than the free-end form, not 7x dearer. The fusion does
not fire when the end node is the pattern's own start variable. Wiring that shape into
`fuse_explore_end_nodes` is the single largest win available on this graph: it would take
the ring detection to roughly the 180 ms of its free-end twin, and it is also most of the
23.7 s the untyped 3-cycle row costs, where the same flat charge sits on top of a genuine
1M × 9³ enumeration.

A related shape crashes codegen rather than running. Asking for the same cycle from an
already-bound variable,

```cypher
MATCH (x:Account)-[t0:TRANSFER]->(a) WHERE t0.is_fraud = true
MATCH (a)-[t:TRANSFER WHERE t.is_fraud = true]->+(a) RETURN count(a)
```

fails with `PLAN_ERROR: Internal Error: The assertion '_part._varMap.contains(var)' failed
at query/ir/codegen/DBProgramGenerator.cpp:1584`. It is valid openCypher, so it wants
fixing rather than rejecting.

## D. Untyped variable-length - the only path shapes v2 will run

| Question | v2 | v3 | | paths |
|---|---|---|---|---|
| Within 2 transfers of every account | 5,479.95 ms | 2,331.01 ms | 2.4× | 90,019,597 |
| Within 3 transfers of the 550 ring accounts | 252.10 | 164.08 | 1.5× | 500,111 |
| Within 5 transfers of the seed account | 29.00 | 5.45 | **5.3×** | 76,009 |
| Accounts within 6 transfers of the seed | `PLAN_ERROR` | 201.05 | — | 480,764 |

The first row is the clean read on the explorator: 38.6 M paths/s against v2's 16.4 M. It
is a quarter of the 150 M/s the same code reaches on Reactome, which is what a 1M-node
random transaction graph costs in cache misses next to a 3M-node biological hierarchy -
every hop here is a dependent miss into an adjacency list nine entries long.

v2 answers `PLAN_ERROR` to `count(DISTINCT)` over a variable-length path at all.

## E. Search against walk, by hop bound

The same pattern twice: `RETURN count(b)` enumerates every trail, which is the walk, and
`RETURN count(DISTINCT b)` is marked `distinct` by the pass and runs the multi-source
search. v2 refuses every one of them, the quantifiers being typed.

**Downstream of one account**, `-[:TRANSFER]->`, one seed:

| bound | walk | trails | search | distinct ends |
|---|---|---|---|---|
| `{1,2}` | 1.36 ms | 100 | 1.43 ms | 100 |
| `{1,4}` | 1.98 ms | 8,498 | 3.47 ms | 8,465 |
| `{1,6}` | 36.41 ms | 683,167 | 193.38 ms | 480,764 |
| `{1,7}` | 290.84 ms | 6,151,203 | 619.79 ms | 986,834 |
| `{1,8}` | 2,626.39 ms | 55,375,613 | 824.74 ms | 999,871 |
| `{1,10}` | — | — | 808.64 ms | 999,888 |
| `{1,14}` | — | — | 779.12 ms | 999,888 |
| `+` | — | — | 686.71 ms | 999,888 |

**Downstream of the 550 ring accounts**, the same quantifier from 550 seeds:

| bound | walk | trails | search | distinct ends |
|---|---|---|---|---|
| `{1,2}` | 127.46 ms | 55,001 | 130.87 ms | 48,142 |
| `{1,3}` | 141.81 ms | 500,111 | 237.43 ms | 351,083 |
| `{1,4}` | 258.04 ms | 4,506,417 | 1,131.17 ms | 957,553 |
| `{1,5}` | 1,309.82 ms | 40,573,269 | 9,707.81 ms | 999,836 |
| `{1,6}` | — | — | 62,101.70 ms | 999,888 |

**This graph inverts Reactome's conclusion.** There the search always paid: the ball of
the hub reaction was 1,520 nodes and the walk enumerated 302 M trails to find them. Here
the ball *is* the graph - at degree 9, eight hops reach 999,871 of 1,000,000 accounts - so
the search's cost is the whole graph and the walk's is only the trails. From one seed the
walk wins to bound 7 and the search takes over at 8, where the trails pass 55 M. From 550
seeds **the walk wins at every bound measured**, by 4.4× at `{1,4}` and 7.4× at `{1,5}`.

The 550-seed column is also the one that scales wrongly. The answer stops moving after
bound 4 - 957,553 ends, then 999,836, then 999,888 - while the time goes 1.13 s, 9.71 s,
62.1 s. One seed's unbounded search costs 687 ms; 550 seeds cost 62.1 s, which is 90× for
550× the seeds. The search runs per batch of seed rows and each batch pays its own ball,
so the cost is `batches × ball`, and once the ball saturates there is nothing left to
amortise. Sharing one reach table across the batches of a query, or sizing the batch to
the ball, is worth up to 48× here.

**This is a constant, not the asymptotics.** The argument for always searching still
holds: per (seed, node) the search expands at most once and the walk at least once, and
the numbers bear it out - at `{1,5}` the 550 seeds reach at most 550 x 9^5 = 32 M (seed,
node) pairs where the walk enumerates 40.6 M trails. The search is doing *less* work and
taking 7.4x longer, so what costs is the work's shape: a random probe into an
open-addressing table against a sequential scan of a nine-entry adjacency list, paid again
for every batch, since `PathReachTable` is cleared and regrown per batch of seed rows.
With 550 seeds in batches and a ball that saturates a 1M-node graph, that is 69 rebuilds of
a million-entry table.

So the fix to try first is the table and the batch, not a gate in front of the search:
size the batch to the ball the seeds are estimated to reach, and pick a dense layout above
some ball fraction the way `PathTargetIndex::planBatch` already does - the escape hatch
`9e9704046` left open for exactly this case. The 62.1 s row is the one to aim at; `{1,6}`
returns 52 more ends than `{1,5}` for 6x the time.

## Memgraph on the same graph and the same box

Memgraph 3.x community in `IN_MEMORY_ANALYTICAL` mode, loaded from the same parquet by
`memgraph_load_fraud.py` (43 s for the 1M nodes and 9M edges) and timed by
`bench_memgraph_fraud.py`. Each query is the v3 query with the quantifier
rewritten and nothing else: `-[:TRANSFER]->{1,4}` becomes `-[:TRANSFER *1..4]->`, `+`
becomes `*1..`, and an inline hop predicate becomes memgraph's filter lambda,
`-[t:TRANSFER *1..4 (e, n | e.is_fraud)]->`. Its variable-length expansion enforces
relationship uniqueness, which is v3's trail semantics.

**Every count memgraph produced matches v3's**, including the 3,166 unbounded fraud
chains, the 550 ring members, the 827 untyped 3-cycles, and the 90,019,597-path ball. The
one exception is `*BFS`, which returns 999,887 where v3 returns 999,888: `*BFS` excludes a
node from its own ball, and the seed is reachable from itself around a cycle.

Indexes are dropped, as in the published benchmark, so the seeded queries scan.

| Question | v3 | memgraph | |
|---|---|---|---|
| Find the seed account by id | 1.04 ms | 77.38 ms | **74×** |
| Its fan-out to 4 hops, as a quantifier | 1.88 | 76.85 | **41×** |
| Does account 796580 sit on a ring | 1.46 | 75.23 | **52×** |
| 2-hop fraud chain, as written hops | 143.85 | 3,993.98 | **28×** |
| 2-hop fraud chain, as a quantifier | 192.78 | 4,204.35 | **22×** |
| Every fraud chain there is, any length | 180.83 | 4,143.93 | **23×** |
| Every account on a ring, whole graph | 1,280.07 | 4,222.30 | 3.3× |
| Every account on a 3-transfer ring, any transfer | 23,675.85 | 65,538.82 | 2.8× |
| Within 2 transfers of every account | 2,331.01 | 8,810.25 | 3.8× |
| Within 3 transfers of the 550 ring accounts | 164.08 | 4,245.81 | **26×** |

Memgraph's floor is its scan: 75 ms to find one account without an index, and about 3 s
for anything that starts from every account. Where v3 wins by 20× and more, most of the
margin is that floor - the published benchmark's own 30-37× over Neo4j is the same
measurement. Where the query is genuinely a walk over millions of trails, the margin is
2.8× to 4.6×, and that is the honest number for the explorator against a mature
competitor.

The sweep, v3 against memgraph as written and against its `*BFS` expansion:

| bound, 1 seed | v3 walk | mg walk | v3 search | mg search | mg `*BFS` |
|---|---|---|---|---|---|
| `{1,6}` | 36.41 ms | 163.83 ms | 193.38 ms | 488.84 ms | 711.96 ms |
| `{1,7}` | 290.84 | 742.20 | 619.79 | 3,668.30 | 2,135.75 |
| `{1,8}` | 2,626.39 | 5,862.97 | 824.74 | 32,105.96 | 2,622.49 |
| `+` | — | — | 686.71 | timeout at 150 s | 2,280.75 |

| bound, 550 seeds | v3 walk | mg walk | v3 search | mg search | mg `*BFS` |
|---|---|---|---|---|---|
| `{1,3}` | 141.81 ms | 3,039.06 ms | 237.43 ms | 3,304.28 ms | 3,329.70 ms |
| `{1,4}` | 258.04 | 3,357.88 | 1,131.17 | 5,261.71 | 5,998.35 |
| `{1,5}` | 1,309.82 | 6,047.58 | 9,707.81 | 22,488.37 | 35,182.98 |
| `{1,6}` | — | — | 62,101.70 | timeout at 150 s | timeout at 150 s |

v3's search is ahead of both memgraph forms at every bound, and unbounded it is the only
engine that answers at all - 687 ms against a `count(DISTINCT)` memgraph cannot finish in
150 s and a `*BFS` that takes 2.28 s. That does not rescue the 550-seed column: v3's own
walk answers `{1,5}` in 1.31 s where its search takes 9.71 s, so the engine is losing 7×
to itself, not to memgraph.
