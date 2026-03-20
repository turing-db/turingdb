---
name: feedback_return_type_same_line
description: Return types must be on the same line as the function name in definitions, never on a separate line
type: feedback
---

Do not split the return type onto a separate line from the function name in function definitions. Keep the return type and function name on the same line.

**Why:** User strongly dislikes the pattern of putting the return type on a line by itself above the function name.

**How to apply:** Always write `ReturnType ClassName::functionName(...)` on one line, even if it's long. Prefer a long line over splitting the return type.
