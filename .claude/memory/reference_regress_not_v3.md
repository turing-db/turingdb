---
name: reference_regress_not_v3
description: regress/ drives the classic pipeline, not QueryInterpreterV3 — MLIR-only changes need ctest, not make run_regress
metadata:
  type: reference
---

The `regress/` suite does not exercise `QueryInterpreterV3` (the MLIR db/nl path).
Its tests start a `turingdb` server and drive it over the wire through the classic
query pipeline, so a change confined to `query/ir/` — codegen, lowering, the nl
interpreter, the dialects — is not covered by `make run_regress`.

**Why:** Remy stopped a `make run_regress` invocation after an MLIR-only rebase with
"regress does not use v3". Verified: no file under `regress/` mentions V3 or selects
the V3 engine. Running it after an MLIR change costs several minutes and proves
nothing about the change.

**How to apply:** For work on the MLIR engine, verify with `ctest` (the
`test/query/ir/` targets are the ones that drive V3 end to end) and stop there. Do not
run `make run_regress` on the side either, even when the change reaches outside
`query/ir/` into `query/analyzer/` or `storage/columns/` to make a V3 query work -
Remy stopped such a run with "do not run regress yet because not supported in v3".
`ctest` is the whole verification story for this work. See [[feedback_test_on_simpledb]]
and [[feedback_separate_test_file]].

**This machine cannot run the native pass anyway.** `run_regress.sh` runs most tests
once under the binary protocol (`TURINGDB_TYPE=native USE_TURING_PROTO=1`) against the
first wheel in `wheel/` - currently `turingdb-1.32.1.dev20`, several versions behind the
built server. Every `[native]` test then fails or hangs on an established but silent
socket while every `json` test passes; two concurrent runs also fight over port 6666 and
leave a daemon holding it. None of that says anything about the change under test.
