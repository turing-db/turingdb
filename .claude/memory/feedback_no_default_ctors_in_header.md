---
name: no-default-ctors-dtors-in-header
description: Define constructors and destructors in .cpp files; never = default in headers
metadata:
  type: feedback
---

Define constructors and destructors in `.cpp` files, not as `= default` in headers.

**Why:** User explicitly corrected this — extends the project rule that destructors are declared in the header but defined in the `.cpp` (CLAUDE.md) to constructors as well.

**How to apply:** When adding or modifying constructors/destructors, always declare in `.h` and define in `.cpp`. Never use `= default` in headers.
