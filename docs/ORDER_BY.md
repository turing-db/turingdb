# Specification for the ORDER BY clause

- ORDER BY is a pipeline blocker: we need to see the entire input before
  outputting chunks
- Therefore we need to store all the chunks we see into some "memory",
  stored by the `OrderByProcessor`

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

## Handling multiple `ORDER BY` keys
- `MATCH ... RETURN ... ORDER BY x, y, z` is a sort with respect to `x`, then `y`, then `z`
  - i.e. the precedence of sorting order is left-to-right

- We can use functional dependencies on keys to eliminate uneccesarry sorting; outlined by Remy for
  `DISTINCT` by here on Notion [^5].
  - e.g. `MATCH (n) RETURN n, n.name ORDER BY n, n.name`
  `n.name` is functionally dependent on `n`: we can remove `n.name` from the ordered keys


[^1] [DuckDB Sorting Rows](https://duckdb.org/pdf/ICDE2023-kuiper-muehleisen-sorting.pdf)

[^2] [New DuckDB sorting blog](https://duckdb.org/2025/09/24/sorting-again)

[^3] [Vergesort](https://github.com/Morwenn/vergesort)

[^4] [CMU Sorting 2024](https://www.youtube.com/watch?v=mM3sFwSuGNY)

[^5] [Remy's DISTINCT notes](https://www.notion.so/turingbio/DISTINCT-study-2f13aad664c880aaa97fd2d6dd5c4486)
