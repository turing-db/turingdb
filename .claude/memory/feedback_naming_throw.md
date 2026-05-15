---
name: feedback-naming-throw
description: "Prefer \"throw\"-prefixed names (e.g. throwError) for helpers that always throw — not \"raise\""
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

When writing a helper that unconditionally throws an exception, name it with a `throw` prefix (e.g. `throwError`, `throwIfError`), not `raise`.

**Why:** User explicitly corrected `raise(...)` to a throw-prefixed name in this codebase. C++ uses `throw` as the language keyword for exceptions, so matching that terminology reads more naturally than the Python/Ruby-style `raise`.

**How to apply:** Any time you reach for `raise` / `raiseError` / `bail` / `fail` to name a `[[noreturn]]` helper that throws (typically `TuringException`), name it `throwError` (or similar `throw*`) instead. Doesn't apply to helpers that *return* errors — only to ones that throw.
