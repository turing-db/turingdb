---
name: change-commit-visibility
description: Within a change, MATCH sees the change's COMMITted tip (read-your-own-writes works after COMMIT, not just data from before the change); after SUBMIT you must checkout head
metadata:
  type: reference
---

Write/read visibility across TuringDB changes (verified empirically 2026-07-09 against a local build; this is engine behavior and can evolve — re-verify with a `CREATE`/`COMMIT`/`MATCH` sequence if it matters).

**Within an open change, reads see the change's committed tip — not its uncommitted buffer.** Statement writes stage in a `CommitWriteBuffer` (`storage/versioning/`) and only become queryable at `COMMIT`:
- `CREATE (...)` then `MATCH` in the same change with **no `COMMIT` between → 0 rows** (the write is still buffered, invisible).
- `CREATE (...)`, then `COMMIT`, then `MATCH` in the **same change (before `SUBMIT`) → visible.** Read-your-own-writes DOES hold once you `COMMIT`. It is NOT true that a change can only see data committed before it started — that is a common misconception.

**Consequence for `MATCH ... CREATE (edge)` that references nodes made in the same change:** it works only if those nodes were `COMMIT`ted first. Without an intermediate `COMMIT` the `MATCH` matches nothing, so the edge is silently not created — later surfacing as e.g. `ANALYZE_ERROR: Unknown edge type: <TYPE>` because the type never came into existence. Safe bulk-load pattern: create nodes → `COMMIT` → `MATCH` + create edges → `COMMIT`.

**After `CHANGE SUBMIT` the change is gone.** Querying with the old change context still selected errors `CHANGE_NOT_FOUND`. `checkout()` (return to head/main) and the committed data is visible there.
