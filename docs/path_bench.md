# Variable-length paths on Reactome: v2 against v3

Measured with `scripts/bench_paths.py`, which runs 28 biologically meaningful Cypher
queries through both engines in the turingdb shell — v2 directly, v3 behind the `#v3`
prefix — and reports the median of five runs per query, the first discarded as a warmup.

Provenance: commit `f56ef43ff`, default `-O3` build, 20-core Linux box, quiet machine.
Graph `reactome` loaded from its binary dump: 2,978,202 nodes and 11,537,331 edges (415
`TopLevelPathway`, 117,945 `Event`, 83,459 `Reaction`, 110,048 `Complex`).

```
scripts/bench_paths.py -reps 5 -verify
```

Both engines agreed on every result they both produced — row multisets, and aggregate
values up to the 125,690,888-path one — with a single exception, which turned out to be a
v2 bug (below).

## A. Property-seeded typed queries — what a curator actually writes

| Question | v2 | v3 | |
|---|---|---|---|
| Find Signal Transduction by accession | 46.80 ms | 0.55 ms | **84×** |
| Its direct sub-events (17 rows) | 46.21 | 1.33 | **35×** |
| Its sub-events exactly 3 levels down (480) | 46.87 | 1.48 | **32×** |
| Its sub-events exactly 4 levels down (1,169) | 47.42 | 1.74 | **27×** |
| Reactions exactly 4 steps downstream of hub `R-HSA-2993780` | 47.09 | 1.66 | **28×** |
| Sub-units of complex `R-HSA-6814275`, 2 levels in | 46.30 | 1.26 | **37×** |

**This is not a path-engine result.** Subtract the seed lookup on the first row and
almost nothing of v2's time is left: its traversal accounts for at most ~0.6 ms on any
of these, against v3's 0.7–1.2 ms. The multiple is `FuseScanByPropertyValue` turning a
2.98M-node scan into a ranged property lookup. On walks this small v3 is level with v2
or behind it, paying a fixed per-query cost of about half a millisecond.

## B. Label-seeded typed traversal — the engine, with no scan in the way

v2 cannot write a typed quantifier at all, but it runs a chain of plain typed hops, which
is the same set at a fixed depth. Both sides return identical counts.

| Question | v2 | v3 | | count |
|---|---|---|---|---|
| Events 2 `hasEvent` levels under all 415 top-level pathways | 2.27 ms | 1.92 ms | 1.2× | 11,630 |
| …3 levels | 9.92 | 3.87 | 2.6× | 28,799 |
| …4 levels | 23.94 | 7.88 | 3.0× | 40,012 |
| …5 levels | 51.06 | 12.32 | **4.1×** | 28,679 |
| …6 levels | 67.26 | 14.87 | **4.5×** | 10,260 |
| Every sub-unit two levels inside every complex | 57.45 | 30.35 | 1.9× | 213,396 |
| Reaction triples three `precedingEvent` steps apart | 46.09 | 18.45 | 2.5× | see below |
| Reactions two `hasEvent` levels under every top-level pathway | 2.43 | 1.16 | 2.1× | 6,371 |
| Every reaction's input entities | 21.28 | 7.79 | 2.7× | 186,017 |

The margin grows with depth: the walk amortises v3's fixed per-query cost.

## C. Untyped variable-length — the only path shapes v2 will run

| Question | v2 | v3 | | paths |
|---|---|---|---|---|
| Within 2 hops of the 415 top-level pathways | 2.99 ms | 1.42 ms | 2.1× | 34,669 |
| Within 3 hops of them | 16.98 | 4.88 | 3.5× | 228,878 |
| Events within 3 hops of them | 25.97 | 7.96 | 3.3× | 90,086 |
| Within 2 hops of all 83,459 reactions | 255.93 | 85.98 | 3.0× | 4,788,031 |
| Within 2 hops in either direction | 7143.46 | 832.97 | **8.6×** | 125,690,888 |

The last row is the cleanest read on the explorator alone: 151M paths/s against 17.6M.

## D. Queries v2 cannot express at all

Asked the same question, v2 answers
`ANALYZE_ERROR: Edge type filters are not supported with variable-length paths yet`.
Every Reactome hierarchy is typed, so this covers most of what a biologist would ask.

| Question | v2 | v3 | rows |
|---|---|---|---|
| Signal Transduction's entire sub-event tree, any depth | `ANALYZE_ERROR` | 2.65 ms | 3,154 |
| Every reaction it eventually decomposes into | `ANALYZE_ERROR` | 2.64 | 2,344 |
| Every reaction under every top-level pathway | `ANALYZE_ERROR` | 26.81 | 92,952 |
| Which top-level pathway contains reaction `R-HSA-2993780` | `ANALYZE_ERROR` | 1.64 | 1 |
| Reactions up to 4 steps downstream of that hub | `ANALYZE_ERROR` | 1.67 | 96 |
| All reactions downstream of that hub, any distance, `DISTINCT` | `ANALYZE_ERROR` | 31.31 | 1,520 |
| That complex's whole sub-unit tree | `ANALYZE_ERROR` | 1.23 | 23 |
| Reaction pairs of one pathway sharing an input entity | `PLAN_ERROR: Common Successor Joins With Common Ancestor Unsupported` | 61.24 | 1 |

The `DISTINCT` cascade is the query the unconditional-search rule fixed: as an all-trails
walk it was killed at 420 s.

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

## Reproducing

A single lock guards a turing dir and other sessions routinely hold `~/.turing`, so copy
the graph out before running:

```
mkdir -p ~/.turing-bench/graphs
cp -r ~/.turing/graphs/reactome ~/.turing-bench/graphs/
scripts/bench_paths.py -reps 5 -verify
```

Run it on an idle box. v3's times are sensitive to competing load in a way v2's are not:
repeating groups B and C during a parallel build on the same machine left v2 unchanged
(7140 ms against 7143 on the last row) while every v3 time roughly doubled, taking that
row from 8.6× down to 4.7×.

`-groups` selects a subset of the four tables, `-only` a comma-separated list of query
ids. `-verify` re-runs the counting queries with output on so their values are compared
and not just their row counts — the disagreement above is invisible without it, since
both engines return one row either way.

The storage-level harness for the same work is `samples/path_bench`, which times the
explorator directly (walker count, lookahead, the three index gates, the distinct search)
on generated out-of-cache graphs rather than through Cypher on Reactome.
