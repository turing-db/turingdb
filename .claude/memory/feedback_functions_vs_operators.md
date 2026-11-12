---
name: Functions vs binary operators
description: User prefers similarity/distance operations implemented as functions (like labels(n)), not as binary operators
type: feedback
---

Embedding distance/similarity operations (cosine_similarity, euclidean_distance) are functions, not binary operators.

**Why:** The user views these as semantically functions (like labels(n), toInteger(s)), not operators (like +, -, =). The codebase distinguishes between OPTYPE_BINARY and OPTYPE_FUNC, and operations like these belong in the function category.

**How to apply:** Declare a new computation over embeddings as a function in `query/AST/FunctionDecls.cpp`, not as an operator in the grammar. The executor runs it through `BinaryOperators::exec`, wrapped in `EmbeddingFunction<F>` (`storage/columns/Functions.h`), which reads an embedding from a plain, nullable or cell column. Do not create separate processor files either — integrate into the existing infrastructure.
