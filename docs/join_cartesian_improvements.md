# Cross product and hash join improvements

Work not yet done on the v3 (MLIR) cross product and hash join, with where each piece
lands in the code and what it is expected to buy. Measurements quoted here were taken on
reactome, single node, through `samples/cartesian_bench` and `samples/hash_join_bench`.

## Where the two operators stand today

The cross product is a block nested loop that materialises every pair. `nl.cross_product`
is an iterator driving its own `nl.for`, so one step lays out `min(chunkSize, remaining)`
pairs and peak memory is one chunk per product rather than the whole product
(`NLExecutor::runCrossProductLoop`, `blockRepeatColumn` and `tileColumn`). Each step
physically writes every carried column of both sides, and a residual predicate above it
gathers the survivors into fresh columns.

The hash join buffers its build side once, then probes it. Keys are hashed and compared in
place, never serialised, and the rows carrying one key form a group so a probe confirms a
key once and then emits that group's rows with no further comparison (`NLHashJoinIndex`).
`nl.hash_join_probe` is an iterator driving its own `nl.for`, as the cross product is, so a
step emits one chunk of pairs and may start and end partway through one probe row's matches
(`DBLowering::lowerHashJoin`, `NLExecutor::runHashJoinCollect` and `runHashJoinProbeLoop`).

The optimiser turns a product cut by one equality into `db.hash_join`, pushes
single-variable predicates into the factors, reuses property reads and trims unread columns
(`dbPassPipeline` in `DBProgramGenerator.cpp`).

## Cross product

**Zero-copy pair chunks.** Instead of laying out repeated values, pair one row of the
smaller side, exposed as a `ColumnConst`, with a whole chunk of the larger side, exposed as
the input chunk itself with no copy. Materialisation drops from one write per column per
pair to nothing, and steps stay chunk-sized because the referenced side is always the
larger one. The foundations are already in place: constant columns flow into the product,
the filter has a constant-mask fast path, and the output sink takes a window over columns.
The work is making every consumer accept a constant column where it now expects a vector.
Note that pair order changes when the sides swap, so any test pinning output order without
`ORDER BY` needs checking. This is the DuckDB and Velox cross product design.

**Buffer the smaller factor once.** The inner factor's scans, property reads and hops
re-execute for every outer chunk, because `lowerCrossProduct` roots the inner factor inside
the outer factor's innermost loop. A keyless buffer plus the existing cardinality estimate
to pick the side turns the product into "spool the small side, stream the large one". The
machinery exists as `nl.hash_join_buffer` and `nl.hash_join_collect` minus the key, and
memory is bounded by the smaller factor, which the graph already holds.

**Selection vectors and late materialisation.** A residual predicate such as `a.x < b.y`
currently gathers every carried column. Carrying a selection vector beside a chunk defers
all copying to the output, and combined with zero-copy pair chunks it makes product plus
filter copy-free until the sink. This is an engine-wide change to the chunk model rather
than a product-local one, so it is the largest of these items.

**Better algorithms for non-equality predicates.** For a single inequality or a band
predicate, sort one side and binary-search or merge instead of generating and discarding
pairs. `nl.sort_buffer` and `nl.sort_collect` already accumulate a side. See DuckDB's
piecewise merge join and the IEJoin paper.

**Morsel-driven parallelism.** Once the inner side is buffered, outer chunks are
independent work units. Splitting them across threads with per-thread sinks is the HyPer
design and is the largest wall-clock win available for a pure product.

**Native code through MLIR.** Lowering the `nl` loop nest to LLVM makes pairs plain loop
indices that never become columns. It is the end state the MLIR design points at, though
the constant-column form above captures most of the benefit at interpreter cost.

**Algebraic shortcuts.** An unfiltered product under `count(*)` is the product of the two
counts and needs no loop at all. For very large results, run-end encoding the repeated side
ships the answer in linear rather than quadratic space, which is where the remaining time
goes once the engine itself is cheap.

## Hash join

**Contiguous per-group runs.** A group's rows are chained through a linked list, so
emitting them costs one dependent load per row where a contiguous array would prefetch.
Materialising each group as a contiguous run after the build completes, as DuckDB does,
would remove that, at the price of a finalise step between the last collect and the first
probe. No measurement points at it today, so it is speculative: the chunked probe already
brought the reactome species join, whose largest species group of roughly 6300 pathways
emits 39.7 of its 40.2 million pairs, to 2.8x faster than the linked walk it replaced.

**A sorted side with a branchless search.** Sorting one side and searching it answers `<`,
`<=`, ranges and the complement of an equality from one build, where a hash table answers
equality only. Make the search branchless, or lay the side out in Eytzinger order
(Khuong and Morin), since a textbook binary search mispredicts at every level. Sorting the
probe chunk too lets each search gallop from the previous hit rather than restart at the
root, which degrades toward a merge as the probe chunk gets denser.

**Node IDs come out range by range.** A label scan walks a range per label set per data
part (`ScanNodesByLabelIterator` over `NodeRange`), so each range is ascending and a join
whose key is node identity can merge the ranges rather than sort a side or build a table at
all. Worth confirming how the ranges of several label sets and data parts order against
each other before relying on it.

**An ordered property index.** The only property index today is a prefix trie for
approximate string matching (`StringPropertyIndexer`), so a sorted or hashed side must be
built per query. A per-data-part ordered property index turns a repeated join on the same
key into an index nested loop join with no build cost, which is the standard answer for
that workload.

**Parallel build and probe.** The build side is read once and the probe streams chunks, so
both phases parallelise over chunks with a shared read-only index after the build. This
depends on the same morsel machinery as the product.

**A pattern that revisits a node should be a join, not a traversal.** These two queries ask
one question and return the same 40,244,478 rows, the first through a value equality and the
second through a pattern that passes back out of the species node:

    MATCH (p1:Pathway)-[:species]->(s1:Species), (p2:Pathway)-[:species]->(s2:Species)
    WHERE s1.dbId = s2.dbId RETURN count(*)

    MATCH (p1:Pathway)-[:species]->(s:Species)<-[:species]-(p2:Pathway) RETURN count(*)

The first fuses into `db.hash_join` and runs in 128 ms. The second contains no
`db.hash_join` at all - its plan is `scan_nodes_by_label`, `get_out_edges_by_type`, a label
check, `get_in_edges_by_type`, a second label check, `count` - and runs in 17,322 ms, 135
times slower.

The cost is an intermediate blow-up, not a slow traversal. `p2` is restricted to `:Pathway`
only *after* the in-edge expansion, so the plan expands every species in-edge whatever its
source type. Dropping that one label makes the query return 1,325,916,617 rows in 2,168 ms,
against 40,244,478 rows in 14,465 ms with it: the same expansion either way, 1.33 billion
intermediate rows, of which the label check keeps 3% and for which it charges about 9 ns
each. The bare expansion runs at 1.6 ns per row, so the traversal itself is not the problem.
The join form never expands anything unqualified, because each factor restricts itself to
pathways at 23,290 rows before a single pair is made.

What is missing is that the second shape is a join on `s` and is never considered as one.
`FuseHashJoin` only matches a `db.cross_product` cut by an equality (`matchEqualityCross`),
and a single connected pattern never becomes a product, so the pass never sees it.

The fix is to recognise a node reached by two hops within one pattern as a join key: emit
the two halves as factors of a product carrying an equality on that node, which the
existing pass then fuses, or add a pass rewriting the hop-out/hop-in pair directly into
`db.hash_join`. It has to be cost-based, not unconditional. Traversal streams and needs no
buffer, so it wins whenever the fan-out is small; the join wins when the revisited node has
many edges, which is exactly when the traversal form pays a random in-edge lookup per row.

Ladybug (a Kùzu fork, `~/lbbench`, one thread) runs the pattern in 697 ms, 25x faster than
we do, and its `EXPLAIN` shows precisely the plan proposed above:

    SCAN_NODE_TABLE -> FILTER isPathway -> SCAN_REL_TABLE (p2)-[]->(s) -> FLATTEN -> HASH_JOIN_BUILD
    SCAN_NODE_TABLE -> FILTER isPathway -> FLATTEN -> HASH_JOIN_BUILD
    SCAN_NODE_TABLE -> FILTER isSpecies -> SCAN_REL_TABLE (s)<-[]-(p1) -> SEMI_MASKER
    -> HASH_JOIN_PROBE -> HASH_JOIN_PROBE -> AGGREGATE

Both pathway sides are filtered before their relationship scan, so no unqualified edge is
ever expanded, and the shared node becomes two hash joins with a semi-mask - the paper's
ASP-Join sideways information passing. The `FLATTEN` operators mark where it gives up its
factorized representation.

It prefers the opposite phrasing to ours, 697 ms for the pattern against 1039 ms for the
value equality, since adjacency-list joins over CSR indices are what its storage exists to
serve. Each engine is between 5 and 135 times slower on the other's phrasing of one
question. Comparing each at its own best form, v3 leads on the join by 5.4x - so the engine
is not the gap here, the plan for this phrasing is.

## Work above a row-multiplying operator

A cross product and a join both emit more rows than they consume, so every operator placed
above one is charged per output row instead of per input row. This is one root cause with
several faces, and on the species join it costs more than everything else in the query
combined.

**Sink a projection's property read into the factor.** The join above costs 128 ms for
`count(*)`. The same join projecting three properties costs 1161 ms:

| query | median |
|---|---|
| `RETURN count(*)` | 128 ms |
| `RETURN p1, p2, s1` | 158 ms |
| `RETURN p1.displayName` | 428 ms |
| `RETURN p1.displayName, p2.displayName, s1.displayName` | 1142 ms |

The three property reads are about 984 ms of that - the difference between projecting the
three node ids and projecting their properties - near 8.2 ns per value over 120 million
reads, and 86% of the query. They are that expensive only because of where they sit.
The plan shows all three `db.get_node_properties` ops above the join, over its 40 million
output rows, while the factors that produced `p1` and `s1` hold 23,290 rows between them.
Reading `displayName` inside the factor and letting the join carry the column would turn 120
million random property lookups into 70 thousand, leaving only a gather from a
cache-resident buffer - which the id-only row above measures at about 10 ms per column. That
puts the whole query near 220 ms rather than 1142 ms.

The machinery already exists and is already used for exactly this, one level up:
`sinkKeyIntoFactor` clones the key's property read into each factor so the join can index
it, which is why `db.get_node_properties(s1, "dbId")` appears inside both factors of the
plan. What is missing is applying the same move to a read that only the projection consumes.
Like the item above it must be cost-based: sinking pays off when the join multiplies rows,
and costs when the join is selective enough that its output is smaller than its inputs.

**The same problem wears other hats.** In the traversal plan above, the label check
establishing `p2:Pathway` (`get_node_label_set`, then `check_label_constraint`, then
`filter`) runs over all 1.33 billion expanded rows and discards 97% of them, at about 9 ns
each: some 12 of the query's 14.5 seconds. `get_node_label_set` is a random gather per row
into `NodeContainer::_nodes`, whose `NodeRecord` is one `LabelSetHandle` - a 4-byte id plus
an 8-byte pointer - so roughly 48 MB across reactome's 2.98 million nodes, past any cache.
The join form never pays any of it, because both sides check their labels at 23,290 rows and
the join only pairs rows that already qualified. Any predicate, conversion or fetch that
depends on one side alone belongs below the operator that multiplies rows.

**Factorization is the general form of this, and the state of the art does not reach it
either.** Kùzu's CIDR 2023 paper makes it design goal (i): "Intermediate relations of m-n
joins should be factorized, i.e., represented as Cartesian products instead of flat tuples",
and it notes that aggregations "cannot blindly assume that each factorized tuple represents
one tuple, and may need to check multiplicities to produce correct outputs" - so counting a
property over a factorized result should read one value per distinct value, not one per
output row. The paper is also candid about the limit: "S-Join achieves our two goals only if
the build side contains a good factorization structure in which the join key is flat...
sometimes the good factorization structure of a sub-query may contain an unflat join keys, so
joining requires flattening the join key values and losing the factorization structure."

Measured, that limit bites. Ladybug pays about 1.3 s for the three property reads in both
phrasings, 1961 ms against its 1039 ms baseline for the value equality and 2009 ms against
its 697 ms baseline for the pattern, so it flattens before the reads rather than exploiting
multiplicity even in the shape its factorization targets. Both engines therefore pay per
output row for properties, which is why v3's 8.1x lead on `count(*)` shrinks to 1.7x once
three properties are projected. Sinking the read into the factor is the cheap way to take
most of that back without rebuilding the chunk model; full factorized vectors, which would
also subsume the selection-vector item in the cross product section, is the thorough one.

Ladybug numbers here come from `~/lbbench` on the same reactome dump, pinned to one thread,
with the query translated to its schema: one `Node` table with the labels as boolean columns,
and "the same species" expressed as `s1.id = s2.id` on the primary key, since the conversion
carried `stId`, `displayName`, `schemaClass` and `speciesName` but not `dbId`.
