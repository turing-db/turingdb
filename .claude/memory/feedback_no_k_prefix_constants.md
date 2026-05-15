---
name: feedback-no-k-prefix-constants
description: "Don't prefix constants with `k` (Google style); use descriptive names like `previewRowsCount`"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

Don't use the Google-style `k`-prefix for constants (`kPreviewRows`, `kBatch`, `kMaxSize`). Use plain descriptive names instead (`previewRowsCount`, `batchSize`, `maxSize`).

**Why:** User explicitly renamed `kPreviewRows` → `previewRowsCount` in `samples/parquet-import/main.cpp`. This codebase doesn't use Google's `k`-prefix convention — it's purely a habit I've imported from elsewhere.

**How to apply:** Any new `constexpr` / `const` local or static. Member constants stay with `_` prefix per the usual rule. Don't churn existing code that already uses `k`-prefix unless asked.
