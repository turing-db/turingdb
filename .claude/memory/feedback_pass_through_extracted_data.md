---
name: pass-through-extracted-data
description: When the caller already extracts/owns data, the callee should accept it as a parameter rather than re-extracting internally
metadata:
  type: feedback
---

When the caller has already done extraction/setup work (e.g., `main()` pre-extracts a `ParquetSchema` for each input file via `ParquetSchemaExtractor`), the callee should accept that data as a parameter and reuse it — don't push the extraction back inside the callee.

**Why:** Came up on PR #645 review of the turing-parquet importer. A reviewer asked "Why do we extract a schema for not using it here later?" at the `ParquetGraphImporter importer(...)` construction line. The point: schemas are already extracted upstream for display/analysis, so threading them into `importNodeFile(path, schema)` is the right design — re-extracting inside the importer just duplicates work.

**How to apply:** Before proposing a refactor that moves work into a helper class, check whether the caller already does that work for other purposes. If so, keep the data flowing through as a parameter rather than re-deriving it. Applies to `tools/turing-parquet/` and analogous import/inspection tooling under `tools/`, `import/`, `dump/`. Related: [[parquet-helpers-dedicated-class]].
