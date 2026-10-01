---
name: reference_regress_runs_v3
description: regress DOES go through the v3 MLIR engine (V2 is deleted), so an MLIR change can break it - but this machine still cannot run the native pass
metadata:
  type: reference
---

`regress/` drives a `turingdb` server or CLI over the wire, and since V2 was deleted
there is only one interpreter left: `db/TuringDB.cpp:39` instantiates
`QueryInterpreterV3`. **So regress does exercise codegen, lowering and the nl
interpreter**, and a change confined to `query/ir/` can break it.

This note previously said the opposite ("regress does not exercise v3, verify with
ctest and stop there"). That was wrong, and it is how a quadratic lowering regression
reached CI on 2026-09-22: `DBLowering::setInsertionAfterHoistedConstants` rescanned the
entry block's run of hoisted constants for every constant it lowered, which no `ctest`
target noticed and which hung `regress/bulk_create_crash` for 27+ minutes. Measured on
the node CREATE, truncated: 4K lines 0.28s, 8K 1.04s, 16K 4.85s, 32K 20.28s - ~4x per
doubling. With the scan replaced by `setInsertionPointToStart`, the same inputs ran
0.09 / 0.12 / 0.24 / 0.49s and the whole 21 MB file loaded in 5.83s.

**Why that fixture is the one that catches it:** `regress/bulk_create_crash/roads.cypher`
is 21 MB and 354,916 lines holding exactly **2** statements - one CREATE of 175,813
nodes with 3 literals each, one of 179,102 edges. About half a million constants in a
single statement, by far the largest single-statement stress in the repo, so any
per-op scan in lowering that is not O(1) shows up there and nowhere else.

**Still true:** Remy has stopped local `make run_regress` runs more than once
("regress does not use v3", "do not run regress yet because not supported in v3"), and
this machine genuinely cannot run the native pass - `run_regress.sh` runs most tests
under `TURINGDB_TYPE=native USE_TURING_PROTO=1` against the first wheel in `wheel/`,
currently several versions behind the built server, so every `[native]` test fails or
hangs on a silent socket, and two concurrent runs fight over port 6666. Verify locally
with `ctest`; rely on CI's regress for the coverage `ctest` does not give, and read a
long regress step as a possible perf regression rather than infrastructure.
