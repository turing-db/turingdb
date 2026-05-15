---
name: feedback-size-t-for-indices
description: "Prefer size_t for indices and counts in new code (row groups, columns, row counts), even when the wrapped library uses int"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

For indices and counts in new code, prefer `size_t` over `int`. This applies to row group indices, column indices, row counts, batch sizes, etc.

**Why:** User explicitly asked for `size_t` for the row group / column index parameters in `ParquetReader`'s SAX visitor signature.

**How to apply:** Public API signatures and stored counters use `size_t`. When calling third-party APIs (like `parquet::`) that use `int`, narrow with `static_cast<int>(...)` at the boundary inside the `.cpp`. Don't churn existing code that uses `int` unless asked.
