---
name: feedback-no-unused-param-comments
description: "Don't wrap unused parameter names in /*comments*/; just name them — the build doesn't warn on unused params"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

Don't write `void f(int /*idx*/, int value)` — the comment-wrapping pattern to silence unused-parameter warnings. Just write `void f(int idx, int value)`.

**Why:** User explicitly told me to stop doing this in the parquet reader visitor overrides. The project's warning flags (`-Wall -Werror -Wunused-function -Wunused-variable`) don't include `-Wunused-parameter`, so there's no warning to silence — the `/*...*/` is pure noise that hurts readability.

**How to apply:** Function and method parameter lists in new code. If the parameter genuinely needs to be silenced (e.g., some specific TU has stricter flags), use `(void)param;` in the body or `[[maybe_unused]]`, never `/*comment*/`.
