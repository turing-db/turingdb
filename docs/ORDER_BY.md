# Specification for the `OrderByProcessor`

- `ORDER BY` is a pipeline blocker: we need to see the entire input before
  outputting chunks
- Therefore we need to store all the chunks we see into some "memory",
  stored by the `OrderByProcessor`

## Top-level Algorithmic Specification

The algorithm is split into two stages.

Stage 1; executed at each call to `OrderByProcessor::execute` whilst input is not closed:
  1. Performs a subsort of a input chunk on all order keys
  2. Stores the sorted chunk (which we may call a "sorted run") into a "memory store"

Stage 2; executed at each call to `OrderByProcessor::execute` once input is closed:
  1. Performs a merge-implemented sort of all "sorted runs" in the "memory store"
  2. Emits at most one chunk of ulitmately sorted data to the output

  Stage 2.2 is repeated until the entire memory store has been emitted, at which point the
  output port of `OrderByProcessor` is closed

### Stage 1 specification
- "Subsort" algorithm for sorting columnar data w.r.t. multiple order-keys defined in [^1] as:

  for an ordered sequence of order-keys, $k_1, k_2, k_3,\dots, k_i$
    1. Sort with respect to values in $k_1$
    for $2\le j < i$:
      2. Check for rows in column $k_{j}$ where there are ties on the same value in $k_{j-1}$, call these runs $R$
      3. Sort the rows in each run $r \in R$ with respect to $k_{j}$

- Sort input chunks using a column-orientated subsort approach (as defined in [^1] page 4)
  - Column-orientated sorting: motivated by suspected negligible speedup of ~1.2x shown in
    row-orientated sorting on inputs of size ~65k (CHUNK_SIZE), where 1.2x is *without*
    accounting for time taken to transform columnar input to row-orientated input [^1]
  - "Subsort" approach: motivated by lower cache misses and branch mispredictions when compared
    with alternative "tuple-at-a-time" for columnar sorting [^1]
    
### Stage 2 specification
- Merge-implemented sorts cannot use "subsort"-esque algorithms because merge-implemented sorts require examining
  the entire row to determine the ordering, whilst a subsort algorithm only considers one column at a time
- Stage 2 merge-implemented sort is therefore implemented using order-key normalisation, defined as:
  - For an ordered sequence of order-keys, $k_1, k_2, k_3, \dots, k_i$ define a "normalised key", $K$, of size
    $\texttt{sizeof(}K\texttt{)} = \sum_{i=1}^n \texttt{sizeof(}k_i\texttt{)}$ (assuming all $k_i$ are static size\*)
  - Elements of $K$ are produced by `memcpy` the values in a row for each $k_i$, and let $K_i$ be the normalised key
    for row $i$.
  - A merge-implemented sort can use `memcmp` on $K_i$ and $K_j$ as a total-ordering of rows
  \* Non-static sizes such as strings can have a fixed-size prefix be stored in $K$, and the ordering of two rows with
     exact prefixes can be resolved by examining the full strings
(adapted from [^1])

### General sorting notes
- "Late materialisation" sorting, defined in [^4] as sorting only row-indices, and then materialising the final output
  once sorting has completed, is shown to be favoured in scenarios adjacent to Stage 1 and Stage 2 [^1]

## Top-level Algorithm:

- `OrderByProcessor::execute` sorts its input chunk (some algorithm)
- Produces a sorted chunk, we call this a sorted run
- Stores that sorted chunk into its "memory"

- Say we recieve $n$ chunks in total as input
- After sorting chunk $n$, we have $n$ sorted runs in our memory
- Apply a run-sorted optimised algorithm such as Vergesort [^3] (used in DuckDB)
  - We can also roll our own custom implementation, with `std::inplace_merge`,
    which is $O(n)$ on sorted ranges

Advantages:

- Sorting cost is distributed over calls to `execute`
- Final sort of sorted runs is optimisable to near $O(n)$ [^3]
- The processor's "memory" can be either in-memory for full speed, or offloaded to disk
  (see [^4] for plenty on classical algorithms to do this with memory-constraints),
  allowing us to more easily convert to a memory-constrained, disk-spilling implementation

## Sorting Individual Chunks

- `std::sort` can be a starting place, best performance-effort tradeoff
  - Can roll our own custom sort after if needed

Ideas from CMU:
1. Late materialisation
  - We sort only row-indexes and order-by keyed columns
  - Sort by ordered columns, then "materialise" the sorted chunk
    by transposing the row indexes into the final block thats stored to memory
  - We can probably reuse the "transform" logic used in `MaterializeProcessor`
    to materialize rows after sorting

Ideas from DuckDB:
1. Use blobbing to remove operator dispatch
  - DuckDB converts the ordered keys to binary blobs, meaning the type which is being
    sorted is always known, and we can hopefully inline `operator<==>(const Blob& a, const Blob& b)`
    instead of having to do a runtime dispatch on whatever `T` we are sorting in the`ColumnVector<T>`
    \* I'm not too sure on this one: I feel like since we deal in chunks of at least ~65k, we can do a
     single dispatch to get the operator per chunk, and reuse that, so this might not be that useful

2. Since sorting is inherently a row-based operation, DuckDB present an approach to convert columnar
   data (such as ours) to row format, and sort the row-representation.
   Their data shows a ~1.15x speedup on sorting row-oriented versus column-oriented data of size ~65k rows
   (i.e. our chunk size) for a sort with three keys [^1] (page 5). This speedup does not account for the time
   spent converting a columnar format to the row format. I believe the speedup they present would not outweigh
   the latency (nor engineering effort) which our column-to-row and post-sort row-to-column mutations would require.

3. "Subsort approach" [^1]
- To sort by multiple keys in a column-oriented manner:
  1. Sort by the first key
  2. Identify rows where the first key is equal
  3. Sort these rows w.r.t the second key
  4. Identify rows where the second key is equal
  ...
  Repeat until no ties or all keys sorted

  - DuckDB identifies this as faster than "tuple-at-a-time" approach (basically row based) [^1]
  - This also integrates nicely into `std::sort`
  - Drawbacks: cannot be used for a merging sort, because a merge requires seeing the entire row at once
    This means that columnar-subsort is appropriate (and favoured) for sortings of individual chunks for each call to
    `OrderByProcessor::execute`, however it is not suitable for manifesting the final result, which requires merging
    all the aggregated sorted runs. To solve this, we can use a "blobbing" approach which is also mentioned by DuckDB [^1].

## Handling multiple `ORDER BY` keys

- `MATCH ... RETURN ... ORDER BY x, y, z` is a sort with respect to `x`, then `y`, then `z`
  - i.e. the precedence of sorting order is left-to-right

### Key elimination via functional dependencies
- We can use functional dependencies on keys to eliminate uneccesarry sorting; outlined by Remy for
  `DISTINCT` here on Notion [^5].
  - e.g. `MATCH (n) RETURN n, n.name ORDER BY n, n.name`
  `n.name` is functionally dependent on `n`: we can remove `n.name` from the ordered keys


[^1] [DuckDB Sorting Rows](https://duckdb.org/pdf/ICDE2023-kuiper-muehleisen-sorting.pdf)

[^2] [New DuckDB sorting blog](https://duckdb.org/2025/09/24/sorting-again)

[^3] [Vergesort](https://github.com/Morwenn/vergesort)

[^4] [CMU Sorting 2024](https://www.youtube.com/watch?v=mM3sFwSuGNY)

[^5] [Remy's DISTINCT notes](https://www.notion.so/turingbio/DISTINCT-study-2f13aad664c880aaa97fd2d6dd5c4486)
