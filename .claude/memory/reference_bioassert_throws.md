---
name: bioassert-throws-not-abort
description: bioassert throws a catchable FatalException (TuringException) — the abort() in BioAssert.h is dead code
metadata:
  type: reference
---

`bioassert(cond, msg, ...)` (`common/BioAssert.h`) does NOT abort the process. `__bioAssertWithLocation` calls `__bioAssertImpl(...)` then `abort()`, but `__bioAssertImpl` (`common/BioAssert.cpp:9`) unconditionally `throw FatalException(...)`, so the `abort()` is unreachable dead code. `FatalException` derives from `TuringException`.

Practical consequences:
- A tripped `bioassert` is **catchable** with `catch (const TuringException&)` / `EXPECT_THROW(..., TuringException)`. No death test needed.
- Its `what()` message starts with `"Internal Error: The assertion '<expr>' failed at <file>:<line>"` — useful to distinguish an internal logic error from a clean user-facing rejection.
- In the Parquet importer, `importGraph` runs the importer synchronously on the caller thread inside a `try{...}catch(...){throw;}`, so bioassert failures propagate to the caller (e.g. a test calling `system.importGraph`).

This subtlety produced a wrong verdict in one review pass (a verifier read only `BioAssert.h`, saw the `noreturn` attribute plus `abort()`, and concluded "aborts"). Always check `BioAssert.cpp`.
