---
name: no-string-predicate-functions
description: TuringDB does not support CONTAINS / STARTS WITH / ENDS WITH string predicates, despite StringOperator existing in the AST/analyzer layer
metadata:
  type: reference
---

TuringDB does not support the string predicate operators `CONTAINS`, `STARTS WITH`, or `ENDS WITH` in `WHERE` clauses.

**Careful — the AST/analyzer layer is misleading:** `StringOperator::{StartsWith, EndsWith, Contains}` exists in `query/AST/expr/Operators.h`, `StringExpr` is defined, and `ExprAnalyzer::analyzeStringExpr` (`query/analyzer/ExprAnalyzer.cpp`) handles it — but there is no execution path, so the operator is not usable end to end. **The presence of these enum values and analyzer hooks does NOT mean the operator is supported** — a grep for `Contains`/`StartsWith` finds hits and looks supported, which is a real trap (it produced a wrong "it's supported" verdict once).

**How to apply:** Match nodes by exact property values only — `MATCH (n {prop: value})`. Don't write `WHERE n.prop CONTAINS ...` / `STARTS WITH` / `ENDS WITH`. If you need substring/prefix matching, precompute the exact key and match on it.
