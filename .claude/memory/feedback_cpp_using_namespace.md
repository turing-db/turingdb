---
name: feedback-cpp-using-namespace
description: "In .cpp files prefer \"using namespace db\" at the top over wrapping in \"namespace db { ... }\"; no anonymous namespaces"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

In `.cpp` files for this project:
- Open with `using namespace db;` (after the includes), don't wrap the file body in `namespace db { ... }`.
- Don't use anonymous namespaces. Inline single-use helpers at the call site, or make them private static members.

**Why:** User explicit preference, repeated when reviewing imports for `io/parquet/ParquetReader.cpp`. Note this conflicts with the CLAUDE.md style guide which says to use anonymous namespaces for `.cpp` helpers — the user's preference here overrides that.

**How to apply:** New `.cpp` files in this codebase. Existing files that use anonymous namespaces stay as-is; don't churn them. Only matters for code I'm writing.
