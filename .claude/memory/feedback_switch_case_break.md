---
name: feedback-switch-case-break
description: "Switch cases use return inside the case body, with break still written aligned to case"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 6de4d846-73be-41ab-bfd7-5ce4471c910f
---

Inside a switch, write `return` directly in each case body when the case produces the function's result — do NOT capture into a local and return after the switch. But still write `break;` after the return, aligned with the `case` keyword (not indented under the case body), even though it is unreachable.

**Why:** The user prefers visual uniformity across all cases — every case ends with `break;` at the same column as `case`. They also dislike the extra `success` local; direct returns read cleaner.

**How to apply:** When writing or refactoring a switch in this codebase:

```cpp
switch (x) {
    case A:
        return doA();
    break;
    case B:
        return doB();
    break;
    default:
        return defaultValue;
    break;
}
```

- `case` and `break` at the same indentation level.
- Case body indented one level deeper.
- `return ...;` inside the body, `break;` after it (unreachable but kept for symmetry).
- Do not introduce a `success` local to capture per-case results.
