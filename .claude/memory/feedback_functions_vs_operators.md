---
name: Functions vs binary operators
description: User prefers similarity/distance operations implemented as functions (like labels(n)), not as binary operators
type: feedback
---

Embedding distance/similarity operations (cosine_similarity, euclidean_distance) should be implemented as functions through the EvalFunction path, not as binary operators through EvalBinaryExpr.

**Why:** The user views these as semantically functions (like labels(n), toInteger(s)), not operators (like +, -, =). The codebase distinguishes between OPTYPE_BINARY and OPTYPE_FUNC, and operations like these belong in the function category.

**How to apply:** When adding new computation operations on embeddings or similar types, follow the function pattern (Functions.h, EvalFunction, ColumnFunctions) rather than the binary operator pattern (BinaryOperators.h, EvalBinaryExpr). Extend the function framework for binary functions rather than shoehorning into the operator framework. Do not create separate processor files either — integrate into the existing infrastructure.
