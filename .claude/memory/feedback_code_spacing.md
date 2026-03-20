---
name: feedback_code_spacing
description: Use blank lines between logical groups of statements — don't write overly compact code
type: feedback
---

Add blank lines between logical groups of statements within a function body. Don't pack all lines together to save tokens.

**Why:** The user had to manually add spacing to make the code "more breathable." Compact code is harder to read; logical grouping with blank lines makes intent clearer.

**How to apply:** When writing or editing C++ function bodies, separate distinct logical steps (validation, variable extraction, computation, side effects, return) with blank lines. Err on the side of readability over compactness.
