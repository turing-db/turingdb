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
brought the reactome species join, whose single group of roughly 6300 rows emits 40 million
pairs, to 2.8x faster than the linked walk it replaced.

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
