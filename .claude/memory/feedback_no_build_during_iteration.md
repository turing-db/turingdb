---
name: feedback-no-build-during-iteration
description: "Don't build during code-review/iteration cycles; wait for user to ask once the design is settled"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

When iterating on an implementation with the user (style feedback, API shape, refactors), don't try to compile/build/test after each change. Just write the code edits and stop. Wait for the user to explicitly say "build" / "compile" / "run it" once they're happy with the design.

**Why:** User said "Don't try to build until we have finished iterating on the implementation." Builds in this project are slow and noisy; running them on every micro-revision burns time and clutters the conversation with output the user doesn't care about during a design pass.

**How to apply:** During multi-turn refactor/review sessions where the user is giving line-level feedback, stop after the edits. Don't volunteer a build invocation. Build only when explicitly asked, or after the user signals they're done iterating ("ok, ship it" / "now let's build" / "does it compile?").

Reinforced 2026-09-09 (list-index work): approving a stage of a plan is NOT a request to
build or to run tests. Asked to "go ahead with step 1", I wrote the code, then ran
`cmake ..`, `make`, and the new test binary unprompted; the user interrupted with "do not
build. do not run tests. just write code."

- "Go ahead", "implement it", "do stage N" mean *write the edits and stop*.
- This holds even when the stage adds a new test file and a new CMake target, and even
  when the change is compile-time-only and "needs" a build to be verified. Say what would
  need verifying and let the user decide when to run it.
- It also holds when a test would be cheap to run, and when a first run fails for an
  environment reason - do not start debugging the toolchain. Only `cmake ..` / `make` /
  the test binary on an explicit ask.
