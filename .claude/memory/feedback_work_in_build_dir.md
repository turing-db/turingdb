---
name: feedback_work_in_build_dir
description: keep build logs, bench scripts and probe outputs under build/, not the session scratchpad
metadata:
  type: feedback
---

Write build logs, benchmark scripts and probe outputs under `build/` in the repo, not in
the session's `/tmp` scratchpad.

**Why:** the user said "you can build inside this directory instead of using brittle
scratchpads" after the scratchpad was wiped overnight on 2026-10-01, taking the reactome
benchmark scripts and a build log with it mid-task. `build/` is git-ignored, survives the
session, and is where the binaries already live.

**How to apply:** `make -j8 > build/<name>.log 2>&1`, scripts as `build/<name>.py`. Query
suite probes still go in `test/query-test-suite/tests/` (compiled in) and are removed
after use. See [[reference_build_error_filter]] for reading the logs.
