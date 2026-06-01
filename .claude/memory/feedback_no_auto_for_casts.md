---
name: No auto for cast results
description: User prefers explicit types over auto when using static_cast, and wants getter results hoisted into local variables
type: feedback
---

Don't use `auto` for the result of `static_cast` — spell out the type explicitly.

Also hoist getter calls (like `getKind()`, `getOperator()`, `getChangeManager()`) into named local variables instead of chaining `getter().method()` — even when the getter is called only once.

**Why:** Explicit types improve readability at cast sites; a named local reads better, avoids re-invoking a (possibly proxying) getter, and keeps related calls visibly operating on the same object.

**How to apply:** When writing new code with `static_cast`, always use the explicit type on the LHS. When you call a method on a getter's result, bind it first (`Type& name = getter();`) and call `name.method(...)` — don't write `getter().method(...)` inline, even for a single call.
