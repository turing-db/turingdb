---
name: Code style - braces and memory access patterns
description: Every control structure must have braces; indexed _currentPtr[0..3] pattern for multi-byte checks is acceptable
type: feedback
---

Every control structure (if, while, for) must have braces — no braceless single-line bodies like `if (x) break;`.

**Why:** User enforces consistent brace style throughout the codebase.

**How to apply:** Always use braces, even for single-statement bodies. When writing `break`/`continue`/`return` after a condition, wrap the body in braces.

---

Indexed access pattern `_currentPtr[0..3]` for checking multi-byte sequences (like `\r\n\r\n`) is acceptable and should not be refactored into a temp variable.

**Why:** The indexed pattern reads naturally for fixed-offset multi-byte checks. User rejected changing it in `jumpToPayload`.

**How to apply:** Only consolidate repeated dereferences of the *same* offset into a temp. Multi-byte indexed checks are fine as-is.
