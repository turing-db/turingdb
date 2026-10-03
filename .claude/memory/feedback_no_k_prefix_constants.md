---
name: feedback-no-k-prefix-constants
description: "constexpr constants are UPPER_SNAKE_CASE (DB_PASS_COUNT); never the Google-style k-prefix"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

Write `constexpr` constants in uppercase with underscores: `DB_PASS_COUNT`, not `dbPassCount`; `PREVIEW_ROWS_COUNT`, not `kPreviewRows`. Never use the Google-style `k`-prefix (`kPreviewRows`, `kBatch`, `kMaxSize`).

**Why:** User renamed `kPreviewRows` in `samples/parquet-import/main.cpp` because the `k`-prefix is a habit imported from Google style. On 2026-10-03 they asked for `dbPassCount` in `query/ir/codegen/DBPassPipeline.cpp` to be `DB_PASS_COUNT`, and for the rule to be stated in CLAUDE.md.

**How to apply:** Any new `constexpr` constant, at namespace, class or function scope. Plain `const` locals and parameters stay lowerCamelCase (`const size_t remaining = ...`). The codebase still has lowerCamelCase `constexpr` names; don't churn them unless asked.
