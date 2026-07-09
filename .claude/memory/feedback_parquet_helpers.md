---
name: parquet-helpers-dedicated-class
description: In tools/turing-parquet/, multi-step ops on ParquetSchema-family types get their own ParquetXxx class + header/cpp, not anon-namespace free functions in TuringParquetTool.cpp
metadata:
  type: feedback
---

In `tools/turing-parquet/`, non-trivial multi-step operations on the Parquet-family types (`ParquetSchema`, `ParquetSchemaField`, `ParquetPropertyAnalysis`, etc.) should be implemented as a dedicated `ParquetXxx` class with its own header + `.cpp` file, NOT as anonymous-namespace free functions in `TuringParquetTool.cpp`.

**Why:** Matches the established directory pattern (`ParquetSchemaExtractor`, `ParquetJsonDetector`, `ParquetPropertyAnalyzer`, `ParquetGraphMapping`). User asked to refactor schema merging out of an anonymous-namespace draft into a `ParquetSchemaMerge` class.

**How to apply:** Whenever a new tool-level operation on Parquet schemas/analyses spans more than a couple of trivial helpers, default to a new `ParquetXxx.h/.cpp` pair under `tools/turing-parquet/` and wire it into the local `CMakeLists.txt`. Internal static helpers that operate on third-party / sibling-class types can still live in the `.cpp`'s anonymous namespace per CLAUDE.md, but the public surface goes through the class.
