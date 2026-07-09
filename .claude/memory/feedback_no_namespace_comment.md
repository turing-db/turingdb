---
name: no-namespace-closing-comment
description: Don't add "// namespace" trailing comments on closing braces
metadata:
  type: feedback
---

Do not add trailing comments like `// namespace` on the closing brace of a namespace (including anonymous namespaces).

**Why:** User explicitly rejected it — not the project's style.

**How to apply:** When writing or wrapping code in `namespace { ... }`, close with just `}` and no comment.
