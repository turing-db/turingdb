---
name: Always use braces on control structures
description: Every if/else/for/while must have braces, even for single-line bodies
type: feedback
---

Every control structure (if, else, for, while) must have braces around its body, even when it's a single statement.

**Why:** User enforces this as a strict style rule — no exceptions.
**How to apply:** Never write braceless `if (cond) statement;` — always wrap in `{ }`.
