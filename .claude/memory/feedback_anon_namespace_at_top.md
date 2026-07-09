---
name: anon-namespace-at-top
description: Place anonymous namespace blocks at the top of .cpp files, right after `using namespace db;`, before any class implementations
metadata:
  type: feedback
---

Place anonymous namespace blocks at the top of `.cpp` files, right after the `using namespace db;` declaration, before any class method implementations.

**Why:** User explicitly corrected placement twice — this is the expected file organization.

**How to apply:** When adding helper functions in an anonymous namespace in a `.cpp` file, put the block immediately after `using namespace db;`, not interspersed between class methods. See [[feedback-cpp-using-namespace]] for the surrounding `.cpp` layout convention.
