# Specification for the `OrderByProcessor`

- `ORDER BY` is a pipeline blocker: we need to see the entire input before
  outputting chunks
- Therefore we need to store all the chunks we see into some "memory",
  stored by the `OrderByProcessor`

## Top-level Algorithmic Specification

The algorithm is split into two stages.

Stage 1; executed at each call to `OrderByProcessor::execute` whilst input is not closed:
  1. Performs a subsort (see below) of a input chunk on all order keys
  2. Stores the sorted chunk (which we may call a "sorted run") into a "memory store"

Stage 2; executed at each call to `OrderByProcessor::execute` once input is closed:
  1. Performs a merge-implemented sort of all "sorted runs" in the "memory store"
  2. Emits at most one chunk of ultimately sorted data to the output

  Stage 2.2 is repeated until the entire memory store has been emitted, at which point the
  output port of `OrderByProcessor` is closed

### Stage 1 specification
- Sort input chunks using a column-orientated subsort approach (see below)
  - Column-orientated sorting: motivated by suspected negligible speedup of ~1.2x shown in
    row-orientated sorting on inputs of size ~65k (CHUNK_SIZE), where 1.2x is *without*
    accounting for time taken to transform columnar input to row-orientated input [^1]
  - "Subsort" approach: motivated by lower cache misses and branch mispredictions when compared
    with alternative "tuple-at-a-time" for columnar sorting [^1]

- "Subsort" algorithm for sorting columnar data w.r.t. multiple order-keys defined in [^1] (page 4) as:

  for an ordered sequence of order-keys, $k_1, k_2, k_3,\dots, k_i$
    1. Sort with respect to values in $k_1$

    for $2\le j < i$:

    2. Check for contiguous runs of rows in column $k_{j}$ where there are ties on the same value in $k_{j-1}$, call these runs $R$

    3. Sort the rows in each run $r \in R$ with respect to $k_{j}$

### Stage 2 specification
- Merge-implemented sorts cannot use "subsort"-esque algorithms because merge-implemented sorts require examining
  the entire row to determine the ordering, whilst a subsort algorithm only considers one column at a time

- Instead, since "memory" will contain a sequence of sorted ranges, $r_i$ ,which are all adjacent in memory, we can use `std::inplace_merge`
  to merge these ranges together, creating a total ordering of all input chunks

- We "fold" the ranges, merging the first with the second, achieving a new sorted range, in place, and merging that new range with the third.
  Repeat until we have amassed a single merged range, which is our total ordering.

- Actually, instead of merging the ranges in place, we can just construct an `indices` vector with size equal to the number of rows in memory.
  We then use the ranges to sort these indices. This saves us moving entire rows around in memory, and is a form of late materialisation
  (see #general-sorting-notes).

- Once we have the total ordering of indices, we can just project from memory, using the indices, to the output dataframe, one chunk at a time.

>[!note] Key normalisation not currently implemented, as dispatch-based sorting was simpler to implement whilst being relatively performant
>- Stage 2 merge-implemented sort is therefore implemented using order-key normalisation, defined as:
>- For an ordered sequence of order-keys, $k_1, k_2, k_3, \dots, k_i$ define a "normalised key", $K$, of size
>   $\texttt{sizeof(}K\texttt{)} = \sum_{i=1}^n \texttt{sizeof(}k_i\texttt{)}$ (assuming all $k_i$ are static size\*)
>  - Elements of $K$ are produced by `memcpy` the values in a row for each $k_i$, and let $K_i$ be the normalised key
>   for row $i$.
> - A merge-implemented sort can use `memcmp` on $K_i$ and $K_j$ as a total-ordering of rows

  \* Non-static sizes such as strings can have a fixed-size prefix be stored in $K$, and the ordering of two rows with
     exact prefixes can be resolved by examining the full strings
(adapted from [^1])

### General sorting notes
- "Late materialisation" sorting, defined in [^4] as sorting only row-indices, and then materialising the final output
  once sorting has completed, is shown to be favoured in scenarios adjacent to Stage 1 and Stage 2 [^1]

> Semantics of multiple `ORDER BY` keys
> - `MATCH ... RETURN ... ORDER BY x, y, z` is a sort with respect to `x`, then `y`, then `z`
>   - i.e. the precedence of sorting order is left-to-right

### Key elimination via functional dependencies
- We can use functional dependencies on keys to eliminate uneccesarry sorting; outlined by Remy for
  `DISTINCT` here on Notion [^5].
  - e.g. `MATCH (n) RETURN n, n.name ORDER BY n, n.name`
  `n.name` is functionally dependent on `n`: we can remove `n.name` from the ordered keys
TODO: Expand on this


[^1]: [DuckDB Sorting Rows](https://duckdb.org/pdf/ICDE2023-kuiper-muehleisen-sorting.pdf)

[^2]: [New DuckDB sorting blog](https://duckdb.org/2025/09/24/sorting-again)

[^3]: [Vergesort](https://github.com/Morwenn/vergesort)

[^4]: [CMU Sorting 2024](https://www.youtube.com/watch?v=mM3sFwSuGNY)

[^5]: [Remy's DISTINCT notes](https://www.notion.so/turingbio/DISTINCT-study-2f13aad664c880aaa97fd2d6dd5c4486)
