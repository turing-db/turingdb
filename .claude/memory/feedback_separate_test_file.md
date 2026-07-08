---
name: separate_test_file
description: Put a new unit test in its own dedicated .cpp file, not appended to an existing test file
type: feedback
---

When asked to add a unit test for a distinct feature/behavior, create a new
dedicated test file (e.g. `BranchTwoHopTest.cpp` with its own `add_ir_tests`
target in the CMakeLists) instead of appending a `TEST_F` to an existing large
test file like `NLExecutorTest.cpp`.

**Why:** The user prefers focused, self-contained test files over growing an
existing catch-all; the new test's fixture, program strings, and sink stay
isolated and easy to find.

**How to apply:** New feature test → new `SomethingTest.cpp` + its own CMake
target (copy the shared link helper, add any extra libs like
`turing_db_examples_s`). Only extend an existing test file when the user asks or
when adding a case to an already-matching suite. See [[test_on_simpledb_fixture]].
