---
name: feedback-cpp-using-namespace
description: In .cpp files, open with `using namespace db;` at the top rather than wrapping the file body in `namespace db { ... }`
metadata:
  type: feedback
---

In `.cpp` files for this project, open with `using namespace db;` (after the includes) rather than wrapping the whole file body in `namespace db { ... }`.

**Why:** User explicit preference, repeated when reviewing imports for `io/parquet/ParquetReader.cpp`.

**How to apply:** New `.cpp` files start with the includes, then `using namespace db;`, then the implementations. Anonymous namespaces for class-independent `.cpp`-local helpers are fine (per CLAUDE.md); place them at the top of the file — see [[anon-namespace-at-top]].
