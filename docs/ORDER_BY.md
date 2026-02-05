# Specification for the ORDER BY clause

## Top-level Algorithm:

- `OrderByProcessor::execute` sorts its input (some algorithm)
- Produces a sorted chunk, we call this a sorted run
- Stores that sorted chunk into its "memory"

- Say we recieve $n$ chunks in total as input
- After sorting chunk $n$, we have $n$ sorted runs in our memory
- Apply a run-sorted optimised algorithm such as Vergesort [^3] (used in DuckDB)

Advantages:

- Sorting cost is distributed over calls to `execute`
- Final sort of sorted runs is optimisable to near $O(n)$
- "Memory" can be either in-memory for full speed, or offloaded to disk (see [^4] for plenty on classical algorithms to do this with memory-constraints)

## Sorting Chunks

Ideas from CMU:
1. Late materialisation
  - We store 

Ideas from DuckDB:
1. 


[^1] [DuckDB Sorting Rows](https://duckdb.org/pdf/ICDE2023-kuiper-muehleisen-sorting.pdf)
[^2] [New DuckDB sorting blog](https://duckdb.org/2025/09/24/sorting-again)
[^3] [Vergesort](https://github.com/Morwenn/vergesort)
[^4] [CMU Sorting 2024](https://www.youtube.com/watch?v=mM3sFwSuGNY)
