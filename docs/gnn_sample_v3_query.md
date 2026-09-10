# 3-hop GNN neighbourhood sampling — `gnn.neighbourhoodSample`, v3 syntax

The `#v3 ` prefix routes the statement to `QueryInterpreterV3`. The same text
without the prefix runs on v2, so both engines can be compared on one server
process.

Three chained calls, one per hop. Each call yields into the next hop's
variable, so the result is the same four columns the pattern-match form
produced (`n, m, k, l`) and nothing downstream of the query has to change.
The three forms below differ only in how the seed nodes are supplied.

## The procedure follows out-edges only

`gnn.neighbourhoodSample` samples a node's **outgoing** edges. On the graph
`0->1, 0->4, 1->2, 2->3, 4->5`:

| seed | 0 | 1 | 2 | 3 | 4 | 5 |
| --- | --- | --- | --- | --- | --- | --- |
| sampled `tgt` | 1, 4 | 2 | 3 | none | 5 | none |

Nodes 3 and 5 have only in-edges and sample nothing. This is not what the
pattern-match form it replaces did: `(n)-[]-(m)-[]-(k)-[]-(l)` is
**undirected**, and the sampler then added both directions client-side. So the
procedure under-samples the neighbourhood on a graph read as undirected, which
is how the DGL backend reads it — `_undirected_dedupe` there builds "the
canonical directed edge list of an undirected graph".

The signature has no reverse-direction option, so an undirected sample is not
expressible in one call. It needs either that option on the procedure or the
reverse edges materialised in the graph.

## Form 1 — `OR` chain

```cypher
#v3 MATCH (n) WHERE n = <id1> OR n = <id2> OR ... OR n = <idN>
CALL gnn.neighbourhoodSample(n, <fanout1>, <seed1>) YIELD tgt AS m
CALL gnn.neighbourhoodSample(m, <fanout2>, <seed2>) YIELD tgt AS k
CALL gnn.neighbourhoodSample(k, <fanout3>, <seed3>) YIELD tgt AS l
RETURN n, m, k, l
```

The DGL sampler sends it on one line. At the benchmark's parameters that is
2048 seeds and fanout 15 per hop, giving a 30 KB statement:

```cypher
#v3 MATCH (n) WHERE n = 2751903 OR n = 1103886 OR ... (2048 terms) CALL gnn.neighbourhoodSample(n, 15, 11) YIELD tgt AS m CALL gnn.neighbourhoodSample(m, 15, 22) YIELD tgt AS k CALL gnn.neighbourhoodSample(k, 15, 33) YIELD tgt AS l RETURN n, m, k, l
```

## Form 2 — `UNWIND` over the id list

```cypher
#v3 UNWIND [<id1>, <id2>, ..., <idN>] AS id MATCH (n) WHERE n = id
CALL gnn.neighbourhoodSample(n, <fanout1>, <seed1>) YIELD tgt AS m
CALL gnn.neighbourhoodSample(m, <fanout2>, <seed2>) YIELD tgt AS k
CALL gnn.neighbourhoodSample(k, <fanout3>, <seed3>) YIELD tgt AS l
RETURN n, m, k, l
```

At the benchmark's parameters, on one line:

```cypher
#v3 UNWIND [2751903, 1103886, ... (2048 ids)] AS id MATCH (n) WHERE n = id CALL gnn.neighbourhoodSample(n, 15, 11) YIELD tgt AS m CALL gnn.neighbourhoodSample(m, 15, 22) YIELD tgt AS k CALL gnn.neighbourhoodSample(k, 15, 33) YIELD tgt AS l RETURN n, m, k, l
```

On v3 with the trim fix on `mlir-skip-pass-verifier` this compiles to the
same program as form 1: `fuse_unwind_equality` folds the unwind and the equality
into a node-ID disjunction, `fuse_scan_by_node_ids` folds that into
`db.const_scan_nodes([...])`, and the three calls follow. Same seek, same rows,
and 2048 ids parse and compile as one list literal instead of 2048 `OR` terms.

Before the fix it did not fold, and at 2048 seeds it ran 8.8 s on v2 and 18.1 s
on v3, against 4-14 ms for the other two forms. What ran was the unfused shape:

```
%0:2 = db.cross_product factor { db.unwind_const([...]) } factor { db.scan_nodes() }
%1   = db.eq %0#1, %0#0
%2:2 = db.filter(%1, {%0#1, %0#0})
%5:3 = db.call_procedure("gnn.neighbourhoodSample", {%2#0, %3, %4}, {%2#0, %2#1}) yields ["tgt"]
```

Every unwound id crossed with all 2.98 M nodes. The cause was not a second
reader of the unwound column: its only users are the `eq` and the `filter`, and
the fusion allows both. The call carries every column in flight, `id` among
them, so the filter's `id` result has a reader and the fused rows would have to
keep an `id` column. The only column that could stand in for it is `n`, a node
ID column, and the fusion refuses an entity column there: `RETURN id` would
print a node. `UNWIND [0, 1, 7] AS id MATCH (n) WHERE n = id RETURN n, id`
stays unfused for the same reason, with no `CALL` at all.

Nothing reads `id` past the call, so the carried column is dead, and dropping
dead carried columns is what `trim_unread_columns` does. It did not here for two
reasons: it had no carry-set layout for `db.call_procedure`, so a call's carry
set was never trimmed, and it ran last, after the fusions. The fix gives the pass
the call layout and runs it a second time before `fuse_unwind_equality`.
`EXPLAIN` reports the two runs as `trim_unread_columns 1` and
`trim_unread_columns 2`.

v2 has no such fusion and form 2 is still 8.8 s there. Use form 1 on v2.

## Form 3 — `db.getNodes` over the id list

```cypher
#v3 CALL db.getNodes([<id1>, <id2>, ..., <idN>]) YIELD id AS n
CALL gnn.neighbourhoodSample(n, <fanout1>, <seed1>) YIELD tgt AS m
CALL gnn.neighbourhoodSample(m, <fanout2>, <seed2>) YIELD tgt AS k
CALL gnn.neighbourhoodSample(k, <fanout3>, <seed3>) YIELD tgt AS l
RETURN n, m, k, l
```

`db.getNodes` takes node ids, so it substitutes for the `OR` chain in native id
mode only. In `dbId` mode the seeds are property values and form 1 is still
needed.

It is not the fastest form on either engine at 2048 seeds: 13.1 ms on v2 and
13.2 ms on v3, against 3.9 and 7.8 for form 1. It trades form 1's compile cost
for a slower execution, and on this graph that is a bad trade.

## The three forms are equivalent samplers, not identical ones

A fixed `seed` makes the sample deterministic for a given input order and
chunking. Order and chunking differ between the three forms and between the two
engines, so the rows differ when a node's out-degree exceeds the fanout and a
choice actually gets made. 128 seeds on reactome, `RETURN n, m, k, l` row
counts:

| fanout | v2 OR | v2 UNWIND | v2 getNodes | v3 OR | v3 UNWIND | v3 getNodes |
| --- | --- | --- | --- | --- | --- | --- |
| 15 | 3421 | 3440 | 3354 | 3421 | 3405 | 3354 |
| 200 | 4756 | 4756 | 4756 | 4756 | 4756 | 4756 |
| 2000 | 4756 | 4756 | 4756 | 4756 | 4756 | 4756 |

The v3 UNWIND column is the unfused build. With the trim fix form 2 compiles to
form 1's program on v3, and the two return the identical row set, not merely
the same count: 42329 rows each at 2048 seeds and fanout 15, equal once sorted.

At fanout 200 no node in the sampled neighbourhood has a higher out-degree, so
no sampling happens and all six results are the identical row set, not merely
the same count. That is the way to check v2 against v3 for equality: raise the
fanout until it saturates. At fanout 15 the two engines are not bit-comparable
on this query, and a difference in rows is the RNG draw order, not a bug.

## Runnable example

Three seeds, fanout 3, against reactome (2.98 M nodes, 11.6 M edges):

```cypher
#v3 MATCH (n) WHERE n = 0 OR n = 1 OR n = 7 CALL gnn.neighbourhoodSample(n, 3, 11) YIELD tgt AS m CALL gnn.neighbourhoodSample(m, 3, 22) YIELD tgt AS k CALL gnn.neighbourhoodSample(k, 3, 33) YIELD tgt AS l RETURN n, m, k, l
```

45 rows: 7 for node 0, 15 for node 1, 23 for node 7. Columns come back as
`["n","m","k","l"]`, all `UInt64`.

Read that split off the batched query, by swapping the projection for
`RETURN n, count(l)`. Running the three seeds as three separate queries gives 7,
15 and 8 rows, which is 30, because the sample depends on input order and
chunking and one seed per query is a different input.

## Signature

`gnn.neighbourhoodSample(node, sampleSize [, seed])` yields `src`, `edge`,
`edgeType`, `tgt`. `sampleSize` and `seed` must be constants; `sampleSize` is
capped at `ChunkConfig::CHUNK_SIZE`. A node's sample is emitted whole, so a
sample wider than the caller's chunk budget takes the step over budget rather
than being split.

Chaining the calls applies a real per-node fanout. The pattern-match form it
replaces has none, and approximates one with a global `LIMIT`, which is why it
returned roughly 3x more input nodes per batch than the configured
`fanouts = [15, 15, 15]` asks for.

## Cost

Server-reported median at 2048 seeds on reactome, 5 runs after a warm-up, same
server process:

| form | v2 | v3 |
| --- | --- | --- |
| pattern match + global `LIMIT 512000` (what DGL did before) | 100 ms | 82 ms |
| form 1, `OR` chain | 17 ms | 30 ms |
| form 2, `UNWIND` | 19,752 ms | 25,898 ms |
| form 3, `db.getNodes` | 23 ms | **17 ms** |

Re-measured 2026-09-10 through the shell's `#v3`, which reports the same
`QueryStatus` total the server does. Same seeds and fanout, one process per
build, the two builds alternated over three rounds with the first discarded as
warm-up. `v3 branch` is `origin/main` plus the two commits of
`mlir-skip-pass-verifier`:

| form | v2 | v3 main | v3 branch |
| --- | --- | --- | --- |
| pattern match + global `LIMIT 512000` | 12.7 ms | 11.4 ms | **8.9 ms** |
| form 1, `OR` chain | **3.9 ms** | 7.8 ms | 6.1 ms |
| form 2, `UNWIND` | 8,839 ms | 18,131 ms | 18,083 ms |
| form 3, `db.getNodes` | 13.1 ms | 13.2 ms | 14.3 ms |

Three of the four orderings differ from the table above it. Form 1 is the
fastest form here on both engines, not form 3. v3 beats v2 on the pattern
match, not on form 3. And the absolutes are 1.3x to 9x lower across the four
forms, so whatever separates the two measurements is not one constant factor
and I could not pin it down; the seed set, the graph state and the server's
output layer all differ. Trust the shape, not the milliseconds.

v3 loses on form 1 because its compile cost scales with the number of `OR`
terms — against v2: +0.7 ms at 1 term, +2.0 ms at 256, +6.3 ms at 1024,
+11.3 ms at 2048, +25.2 ms at 4096. The cost is compile-time, not execution:
it is already there for `MATCH (n) WHERE <2048 OR terms> RETURN n` with no
`CALL` at all, and it does not grow as calls are chained.

The two commits on the branch take 22% off both `OR`-chain forms by removing
compile work only: the MLIR verifier no longer runs after each db pass, and the
mask cone is walked without recursion or a hash set. Nothing changes for form 2
or form 3, which barely compile anything. Form 3 replaces the 2048 predicate
terms with one list literal, so the compile cost goes away, but it pays that
back in execution.

The trim fix on the same branch changes form 2 alone. Driver phase timers on the
fixed build, same 2048 seeds and fanout 15, medians of 5 runs with the load
under 1, in milliseconds:

```
         parse  analyze  codegen   lower  translate  execute   compile   total    rows
form 1   1.467    0.568    2.595   0.134      0.060    4.088     4.824   8.912   42329
form 2   0.426    0.133    1.393   0.130      0.062    4.128     2.143   6.271   42329
form 3   0.432    0.134    0.393   0.140      0.079   15.019     1.178  16.198   42337
```

Form 2 is now the cheapest form on v3. It executes like form 1 and compiles in
less than half the time, because the 2048 ids are one list literal. Form 1 and
form 3 measure as they did before the fix, within run-to-run spread - form 1 at
HEAD compiles in 4.746 and executes in 3.937 - so the second trim run costs
nothing visible.
Form 2 before the fix, same driver, same seeds: 17,841 ms of execution, median
of 3, for 42551 rows. That is a different sample, not a wrong one: the unfused
shape feeds the calls in list order and the fused one in id order.

## Where form 1's compile time goes

Measured on an Intel Core Ultra 7 265, Release build, at 088b0533b. The
machine is shared with sibling worktrees running their own suites, and a
contended run reads 30-40% slow across the board, so every number below comes
from a window with the load under 3 and was checked for a tight per-run spread.

Profiled with `build/samples/mlir/mlir`, which prints v3's six phase timers, so
the shell's single total is not the only number available:

```
build/samples/mlir/mlir -q "<form 1 at N seeds>" -g ~/.turing/graphs/reactome -e -quiet
```

Random seed ids on reactome, 5 runs per point, medians in milliseconds:

```
     N   parse  analyze  codegen   lower  translate  execute   compile   total    rows
     1   0.043    0.018    0.193   0.072      0.047    0.024     0.374   0.398       0
    16   0.056    0.020    0.224   0.076      0.047    0.131     0.424   0.555     935
    64   0.090    0.029    0.306   0.074      0.048    0.317     0.547   0.864    2445
   256   0.234    0.067    0.639   0.085      0.053    0.789     1.079   1.868    5728
   512   0.392    0.119    1.104   0.078      0.049    1.565     1.742   3.307   12455
  1024   0.757    0.221    2.156   0.124      0.074    3.057     3.334   6.390   26414
  2048   1.499    0.586    4.458   0.073      0.049    3.900     6.665  10.565   46075
  4096   3.277    1.169    8.743   0.081      0.054    6.434    13.324  19.758   96368
  8192   6.807    2.362   18.101   0.083      0.060   11.118    27.413  38.530  194392
```

Compile is linear in the seed count at 3.3 us per `OR` term, flat from 512 up.
Codegen is two thirds of it, parse a quarter, analyze the rest. Execution is
also linear but at 1.35 us per seed, so compile passes execution at 16 seeds
and is 2.5x it at 8192. Lowering and translation do not move: 0.08 ms and
0.05 ms at every N.

Lowering and translation stay flat because the `OR` chain is gone before they
run. `EXPLAIN (codegen)` shows what emission builds - `db.constant`, `db.eq`,
`db.or` per term, so 3N-1 ops - and the fourth pass, `fuse_scan_by_node_ids`,
collapses all of it to one `db.const_scan_nodes([...])`. At 8192 seeds that is
a 24575-op module built and thrown away.

`perf record --call-graph lbr`, 40 runs at 1 seed and 40 at 8192, samples
classified by stack and differenced:

```
codegen: emit                8.52 ms   1.04 us/term
parse                        8.27 ms   1.01 us/term
codegen: MLIR verifier       7.38 ms   0.90 us/term
codegen: db pass bodies      3.46 ms   0.42 us/term
analyze                      2.51 ms   0.31 us/term
free / madvise / teardown    3.5  ms   0.43 us/term   (outside the timed regions)
```

Emission is `DBProgramGenerator::translateBinaryExpr`, 8.0 of the 8.5 ms: one
microsecond to create three MLIR ops, walking down the 8192-deep `OR` tree.

The verifier is second and it is not our code. MLIR verifies the whole module
after every pass, and the module is still 24575 ops for the first three passes.
7.38 ms over four full verifications is 1.8 ms each, 75 ns/op.

Among the pass bodies `fuse_scan_by_node_ids` is the expensive one, about
3.1 ms. Two thirds of that is not the match but the cleanup:
`replaceFilterWithSource` needs the mask cone, and `collectMaskCone` walks the
24575-op chain into a `SmallPtrSet`, recursing ~6300 frames deep and rehashing
as the set grows. `push_down_filters` walks the same module but its own cost is
only 0.14 ms.

Attributing that cone walk is a trap worth writing down. Its recursion is deeper
than the 32-entry LBR window, so the pass frame above it is truncated away and a
call-graph profile leaves the samples unparented. `collectMaskCone`,
`collectConePostOrder` and `replaceFilterWithSource` are shared by
`push_down_filters` and `fuse_scan_by_node_ids`, so classifying by helper name
bills them to whichever pass the regex names first - which put 2.49 ms on
`push_down_filters` here on the first pass over the data. Of the 940 cone-walk
samples only 174 keep a pass frame, and all 174 say `fuse_scan_by_node_ids`.

Parse splits into bison stack push/pop 0.34 us/term,
`SourceManager::setLocation` 0.19 (a hash-map insert per node), AST
construction 0.11, lexing 0.07. Analyze is all
`ExprAnalyzer::analyzeBinaryExpr`.

Two controls. The three `CALL`s are not part of the cost:
`MATCH (n) WHERE <N OR terms> RETURN n` compiles in 7.95 ms at 2048 against
form 1's 10.05 ms, and 35.6 ms at 8192 against 33.8 ms, equal within run-to-run
spread. And form 3 pays 6x less to compile and more to run: at 2048 seeds
compile 1.08 ms against form 1's 6.67 ms, execution 15.4 ms against 3.90 ms.

The two commits on `mlir-skip-pass-verifier` cut the compile side. Codegen no
longer runs the MLIR verifier after each db pass, and the mask cone is walked
iteratively with the membership set consulted only for a multiply-used result:

```
                       HEAD   verifier off   + cone walk
codegen @ 2048        4.458          2.881         2.546 ms
codegen @ 8192       18.101         11.323        10.254 ms
codegen slope          2.20           1.37          1.25  us/term
compile slope          3.32           2.48          2.37  us/term
```

At 8192 seeds the cone walk itself goes from 1.92 ms to 0.80, and what is left
of it is 0.38 ms in the walk loop, 0.29 in `isMaskComputeOp` and 0.10 in
`getDefiningOp`. That is a pointer chase per op and it is the floor for this
emission shape: `isMaskComputeOp` was rewritten three ways - a tablegen
`MaskCompute` trait, one hoisted `TypeID` compared against an array, and the
same as a fold - and all three land within 0.03 us/term of the original
`isa<>`, which is the run-to-run spread. The comparison scheme is not what
costs; reaching the op's name impl is.

`db.getNodes` also rejects an out-of-range id where `n = <id>` matches nothing:

```
CALL db.getNodes([2979330]) YIELD id AS n RETURN n
Invalid node ID: 2979330
```
