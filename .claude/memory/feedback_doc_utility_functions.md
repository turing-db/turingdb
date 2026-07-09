---
name: doc-tool-utility-functions
description: Add a one-line WHAT comment above each free/utility function in tool .cpp drivers, despite CLAUDE.md's default no-comments rule
metadata:
  type: feedback
---

In tool-style `.cpp` files (e.g. `tools/turing-parquet/TuringParquetTool.cpp`) with a stack of free functions in an anonymous namespace, put a concise one-line comment above each one describing what it does. Keep it to one line and skip it for `main()`.

**Why:** Requested on PR #645 ("Add a comment to describe concisely what each utility function does") after the functions were left undocumented per CLAUDE.md's general "default to writing no comments" rule. The default still applies inside library code; the exception is the free-function layer at the top of tool drivers where readers skim to learn what the file does.

**How to apply:** When writing or editing a tool `.cpp` with multiple free functions in `namespace { ... }`, write `// <one-line description>` directly above each one's definition. Don't add the same comments to class methods or library code — that's still no-comments-by-default. A file qualifies if it lives under `tools/` and has a `main()`.
