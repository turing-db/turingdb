---
name: reference_build_error_filter
description: filter make output with 'error:|Error [0-9]', not ' error '; a missed failure leaves stale test binaries that still pass
metadata:
  type: reference
---

GCC prints compile failures as `file.cpp:12:5: error: ...` and make as `make[3]: *** ...
Error 1`. A filter like `grep -E ' error '` matches neither, so a failed build looks clean.
The test targets then keep their old binaries, and running them tests the code from before
the edit. On 2026-09-30 a rename collided with an existing member and the executor library
stopped compiling for several rounds; the old binaries kept running and the failures looked
like logic bugs.

**How to apply:** filter builds with `grep -E 'error:|Error [0-9]'`. After a change to a
library, check the test binary is newer than it (`ls -la --time-style=+%T`) before trusting
a result. A binary older than `libturing_db_ir_exec_s.a` that ctest does not list is a
leftover of a removed target, not a stale test. See [[feedback_no_build_during_iteration]].
