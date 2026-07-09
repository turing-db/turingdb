---
name: gcc-maybe-uninit-sort-permutation
description: Sort an index permutation (not the heavy movable values) to dodge GCC's -Wmaybe-uninitialized false positive; a pragma can't suppress it
metadata:
  type: reference
---

`test/query/ir/DBLoweringTest.cpp` builds with `-Werror=maybe-uninitialized` under GCC 13. `CollectingNodeEmbeddingPropSink` holds rows of `std::pair<uint64_t, std::optional<std::vector<float>>>`. `std::sort`-ing that vector directly instantiates `__insertion_sort`, whose `__val = move(*__last)` trips a spurious `maybe-uninitialized` on the moved `optional<vector<float>>`. Its appearance is sensitive to total TU complexity, so adding unrelated `TEST_F` cases can flip it on.

**Key fact:** `-Wmaybe-uninitialized` is a *middle-end* (optimization-time) warning; its location is inside the std header, so a `#pragma GCC diagnostic ignored` at the call site does NOT suppress it. Don't waste time on the pragma.

**Fix (already applied):** `CollectingNodeEmbeddingPropSink::sortedRows` sorts a `std::vector<size_t>` index permutation with a comparator (`_rows[a] < _rows[b]`) instead of moving the heavy pairs, then materializes the rows in that order. Sorting `size_t` never instantiates the offending move. Behavior is identical.

**How to apply:** if a new test case revives this error, it's GCC noise, not your bug — and the index-permutation sort pattern is the cure for any sink/helper that `std::sort`s a movable heavy type.
