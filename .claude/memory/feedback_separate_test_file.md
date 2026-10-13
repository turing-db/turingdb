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

**How to apply:** New feature test → new `SomethingTest.cpp` + one
`add_turing_test` (or `add_ir_tests`) line in the CMakeLists. Do not add a link
list: every test links `turing_test_libs` (test/testutils/CMakeLists.txt), which
already holds every engine library, so a per-test `target_link_libraries` would
give the binary a second copy of a library. Only extend an existing test file when
the user asks or when adding a case to an already-matching suite. See
[[test_on_simpledb_fixture]].
