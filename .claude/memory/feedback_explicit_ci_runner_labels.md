---
name: feedback-explicit-ci-runner-labels
description: "In GitHub Actions, prefer explicit real runner labels in each workflow's os_list + runs-on: ${{ matrix.os }} over a clever computed runs-on ternary"
metadata:
  node_type: memory
  type: feedback
---

For GitHub Actions `runs-on`, prefer the dumb-but-clear shape: put the **literal runner label** in each workflow's `os_list`/matrix and let `runs-on: ${{ matrix.os }}` pass it straight through. Do NOT use a computed ternary that rewrites os names (e.g. `format('{0}-turing', matrix.os)`) or injects a runner-group object (`fromJSON('{"group":"turing-runners",...}')`) to derive the target at dispatch time.

**Why:** The user repeatedly pushed back ("this expression is not very clear", "why can't we just take the os list as it is", "why not putting the real runner labels in each of the workflows?") on a runs-on ternary that was doing suffix-mapping (`ubuntu-24.04` → `ubuntu-24.04-turing`) and runner-group scoping. The clever expression also hid a real bug: passing already-suffixed names (`ubuntu-24.04-turing`) fell through to a `group: turing-runners` branch that no runner satisfied, so release jobs queued forever even with idle runners. Explicit labels make the dispatch target obvious and eliminate that class of bug.

**How to apply:** When os_list entries equal the real runner labels (`ubuntu-24.04-turing`, `macos15-turing` self-hosted; `ubuntu-24.04-arm` GitHub-hosted), collapse `runs-on` to `${{ matrix.os }}`. The only transform that may stay is the implicit `self-hosted` label when a bare label would collide with a GitHub-hosted one — but only if the user wants self-hosted; here they explicitly chose hosted for arm. Watch the one trap: a self-hosted-only resource (the `/opt/actions-local-cache` dep cache via `corca-ai/local-cache`) means an entry routed to a GitHub-hosted runner rebuilds deps from source every run, so keep x86 build jobs on the `-turing` label.
