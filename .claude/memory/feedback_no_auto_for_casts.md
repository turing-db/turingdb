---
name: No auto for cast results
description: User prefers explicit types over auto when using static_cast, and wants getter results hoisted into local variables
type: feedback
---

Don't use `auto` for the result of `static_cast` — spell out the type explicitly.

Also hoist repeated getter calls (like `getKind()`, `getOperator()`) into named local variables.

**Why:** Explicit types improve readability at cast sites; local variables avoid redundant calls and make the code clearer.

**How to apply:** When writing new code with `static_cast`, always use the explicit type on the LHS. When a getter is called more than once, save the result in a local variable first.
