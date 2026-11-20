---
description: Review, test, fix in a loop until it converges, then one commit
argument-hint: "[effort level and/or review target, e.g. 'high' or 'medium 42']"
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, Agent, Skill, TodoWrite, ReportFindings
---

# Converge

Review the change, pin every correctness finding with a failing test, fix it, review again.
At most 4 iterations, fewer once the review stops finding anything worth fixing. One commit
at the end carrying the fixes and the tests, and a review certificate on the branch's PR.
`REVIEW.md` records the whole run and is never committed.

`$ARGUMENTS` may carry an effort level (`low`, `medium`, `high`, `xhigh`, `max`) and a
review target (a PR number, a branch, a path, or a jj change id). Default: `xhigh`, and the
target phase 0 works out.

Run in **jj mode** when `$ARGUMENTS` contains `jj`, or when `jj root` succeeds in the working
directory. jj mode keeps the loop and replaces every git step; see [jj mode](#jj-mode) below.
Where a step below names a git command, jj mode's version of that step is the one to follow.

## 0. Fix the target and the baseline, once

```bash
git status --porcelain
git branch --show-current
git diff --stat
```

Record four things now and do not recompute them later.

- **The target.** `$ARGUMENTS` if it names one. Otherwise the uncommitted working-tree
  change when `git diff --stat` is non-empty, and the current branch against `main` when it
  is not. Every iteration reviews that same target at that same effort level, or the
  iterations do not compare.
- **The review base.** The commit the target's diff starts from: `git rev-parse HEAD` for
  the working-tree change, `git merge-base origin/main HEAD` for the branch, the `mergeBase`
  that `python3 scripts/review_certificate.py target --pr <n>` prints for a PR. The
  certificate needs it to show the review saw the whole PR. A path target has none, and
  posts no certificate.
- **The baseline.** The full `git status --porcelain` output. It is what tells apart the
  files the loop touches from the ones that were already dirty, and the commit phase stages
  against it. This tree normally carries untracked scratch turing directories and `.out/`
  directories.
- **The branch.** Only so the commit phase knows whether to branch first.

A clean tree with nothing to diff against ends the run here: say there is nothing to review
and stop.

## jj mode

In a jj workspace, git is not a way to look at or change the tree. A workspace of another
repo has no `.git` of its own, or one pointing at the other checkout, so `git status`,
`git diff` and `git add` read and write *that* checkout's index. Run no git command in jj
mode, read-only ones included: their output describes a different tree.

**Phase 0.** Record the same three things from jj:

```bash
jj root
jj st
jj log -r '@ | @-' --no-graph -T 'change_id.short() ++ " " ++ commit_id.short() ++ " " ++ description.first_line() ++ "\n"'
```

- **The target** is one change. `$ARGUMENTS`' change id if it names one. Otherwise `@` when
  `jj diff -r @ --stat` is non-empty, and `@-` when it is not. Record its change id, which
  survives rewrites, not its commit id.
- **The baseline** is `jj diff -r <target> --stat`, plus the descendants:
  `jj log -r '<target>::' --no-graph -T 'change_id.short() ++ " " ++ description.first_line() ++ "\n"'`.
- **The review base** is the target's parent commit:
  `jj log -r '<target>-' --no-graph -T commit_id`.
- **Where you started.** The change `@` is on now, so the loop can return to it.

When the target is not `@`, run `jj edit <target>` before the first review, so every fix
lands in it. Descendants rebase onto each rewrite by themselves. Going back to where you
started at the end rewrites every file the two changes differ in, so the next build
recompiles them.

**Review.** The review cannot use git to find the diff. Pass it the change and the command
that shows it:

```
Skill(skill: "code-review", args: "<level> jj change <id>: read the diff with `jj diff --git -r <id>`; run no git command")
```

**REVIEW.md** goes to `build/REVIEW.md`, not the repo root. jj auto-tracks new files, so a
root `REVIEW.md` would be snapshotted into the target on the next jj command. `build/` is
ignored. Anything else the loop writes outside `build/` becomes part of the target too:
scratch output belongs under `build/` or the session scratchpad.

**Step 5.** Never `jj abandon`, `jj restore`, `jj undo`, `jj op restore` or `jj squash`
over the user's changes. They rewrite history the loop does not own, and nothing restores a
change they drop. A fix that turns out wrong is edited back out by hand, as in git mode.

**The commit** already exists: the target is the one commit, carrying the fixes and the
tests. Make no new change and do not edit its description. Before the report, check:

```bash
jj diff -r <target> --stat
jj log -r '<target>::' --no-graph -T 'change_id.short() ++ " " ++ description.first_line() ++ if(conflict, " CONFLICT") ++ "\n"'
```

The stat must list the phase-0 baseline plus the files the fixes touched, the new tests
and the `CMakeLists.txt` that registers them, and nothing else. A stray scratch file is
deleted from disk, which drops it from the change on the next snapshot. A descendant
marked `CONFLICT` is reported, not resolved: it is the user's commit.

When the target was not `@`, `jj edit` back to the change you started on.

**Certify** needs the git store behind the jj repo and a PR whose head is the target's
commit. Run every `review_certificate.py` command with `GIT_DIR` set to that store (the
colocated `.git`, or `.jj/repo/store/git`), and pass the PR and the commit explicitly:
`target --pr <n> --head <commit id>`, and the same `pr` and `head` in the input file. No
PR carrying the target means no certificate: say so in the report.

**The report** names the target's change id and new commit id in place of a sha and a
branch, lists any descendant that conflicts, and says `REVIEW.md` is at `build/REVIEW.md`.

## The iteration

Six steps, in this order. Keep a todo list with one entry per finding.

### 1. Review

Invoke the built-in `code-review` skill with the level and target from phase 0:

```
Skill(skill: "code-review", args: "<level> [target]")
```

Not the `code-review:code-review` plugin command — that one reviews a GitHub pull request
and posts a comment on it. Never pass `--fix`, which would apply fixes before the test
exists, and never `--comment`, which writes to GitHub; converge applies its own fixes, and
its one write to GitHub is the certificate at the end.

Let the review run its verification pass and call `ReportFindings` as it always does. Those
findings, verbatim, are the input to step 2. Do not also print them as chat text.

From the second iteration the target's diff contains the loop's own fixes and tests, and
the review will have things to say about them. Those findings count — a bad test is a real
finding — but a review that only re-describes your own edits is the convergence signal.

### 2. Triage

Sort every finding into one of these, and check it against REVIEW.md's earlier iterations
before deciding:

- **correctness** — a failing test first, then a fix.
- **simplification**, reuse, altitude — a fix, no test.
- **efficiency** — a fix, no test.
- **test coverage** — recorded in REVIEW.md, not acted on. Every correctness finding
  already brings its own test; a bare coverage gap is not what this loop is for.
- **rejected** — with the reason that made it a false positive.

A finding an earlier iteration recorded as rejected or deferred is not new: keep its
earlier status, and move on. A finding recorded as **fixed** that comes back is a failed
fix: reopen it under its old id and say in the new section whether the test was wrong or
the fix was.

Three standing rejections, from CLAUDE.md. A finding asking for any of them is wrong, and
acting on it would commit a regression:

- turning away a valid openCypher query in the analyzer. Implement it or leave it.
- liveness, dead-column or use-later analysis in `DBProgramGenerator`. That belongs in an
  MLIR pass over the emitted dialect.
- a comment that narrates the code, marks where a statement sits, or annotates a house
  convention.

Scope guard: fix what the finding names, not the code around it. A finding whose fix is a
broad refactor is recorded as deferred with one line on what it would take.

### 3. Write REVIEW.md

Append this iteration's section. Never edit an earlier one — the file is the record the
convergence decision reads. Format below.

### 4. A failing test per correctness finding, before the fix

- One behaviour, one new file, one target: `test/<area>/<Behaviour>Test.cpp`, registered
  with that directory's own helper — `add_ir_tests`, `add_storage_tests`,
  `add_vector_tests`, `add_db_system_tests`, or `add_turing_test` where the directory has
  no wrapper. Never append to an existing test file.
- Name the test for the behaviour it asserts, never for the finding or the bug.
- The expected values are the correct ones, derived independently. Never encode what the
  code prints today.
- Query-level behaviour is tested on `SimpleGraph::createSimpleGraph` (link
  `turing_db_examples_s`), not on a purpose-built graph.
- Build and run it. Ninja regenerates the build first when a CMakeLists.txt changed, so a
  brand-new target builds directly. Never `cmake ..`:

  ```bash
  cd build && ninja -j8 test_<area>_<name>
  ./test/<area>/test_<area>_<name>
  ```

**The failing test is the verification gate.** A correctness finding whose test passes
before the fix is not a defect, whatever verdict the review gave it: record it as rejected
with the test output as the evidence, and do not write the fix. Keep the test when it pins
behaviour worth pinning and delete it when it does not; say which in REVIEW.md.

A correctness finding in code no test can reach — a CMake file, a CI workflow, a shell
script, a packaging path — is fixed without one. Record that, and name what would have to
exist to test it. "No test" is a fact to record, never the fallback for a test that is
merely awkward to write.

### 5. Fix

Correctness findings first, then simplification, then efficiency.

- After each correctness fix, re-run its test. It must pass, and the output goes in
  REVIEW.md next to the failure it replaces.
- After the simplification and efficiency fixes, re-run every target the loop has built and
  the targets that already covered the files you touched: `ctest -R <pattern>` from
  `build/`.
- Do not run `ninja run_regress`. It drives the classic pipeline, so it says nothing about
  work in `query/ir/`, and on this machine its native pass fails for unrelated reasons.
- Never `git checkout --`, `git reset --hard` or `git stash`. The tree holds the user's
  uncommitted work and nothing can restore it. A fix that turns out wrong is edited back
  out by hand. jj mode has its own list of history-rewriting commands to avoid.

### 6. Decide

Apply the stop rules below. When none fires, start the next iteration at step 1.

## When to stop

Stop after the iteration that first satisfies one of these, and name which one in the final
report:

- Triage leaves no correctness, simplification or efficiency finding to act on.
- Every actionable finding this iteration is one an earlier iteration already recorded as
  rejected or deferred.
- The iteration changed no code.
- Four iterations have run.

Stop before the fourth when the findings have gone cosmetic, or when each round only
rewords the last one's. Churn is not convergence: three iterations that rewrite the same
function are the signal to stop and report, not to spend the fourth.

Abort and go straight to the commit phase when a fix breaks a test that passed before and
you cannot make both pass. Edit that fix back out, record the finding as deferred with the
conflict, and commit what is green.

The fourth iteration still writes its tests and its fixes; there is no fifth review behind
them. When the loop ends at the cap, say so — the last round's fixes are unreviewed.

## REVIEW.md

At the repo root, untracked, never staged, one section appended per iteration. In jj mode
it lives at `build/REVIEW.md` instead. Give each
finding an id numbered across the run, not within the iteration — `C1`, `S1`, `E1` — so a
later section can write `C2 reopened`.

```markdown
# Converge — <target> — <date>

## Iteration 1 — code-review xhigh

### Correctness
C1 `query/ir/DBLowering.cpp:412` — the limit attribute is signed, so a negative literal
   reaches the verifier instead of failing to parse. CONFIRMED.
   fails on: LIMIT -1 parses and yields every row, expected a parse error
   test: test/query/ir/LimitAttrTest.cpp — LimitAttrTest.NegativeLimitIsRejected
   before: failed, "expected 0 rows, got 18". after: passes.
   status: fixed

### Simplification
S1 ...

### Efficiency
E1 ...

### Test coverage — recorded, not acted on
...

### Rejected
R1 `net/http_common/HTTPWriter.cpp:88` — ... — the test asserting it passes unchanged,
   so there is no defect.
```

Keep the entries plain: the file, the defect in one sentence, the input that breaks it, the
test, the status. No prose paragraphs.

## The commit

One commit for the whole run, made after the loop, never inside it. In jj mode the target
is that commit, and this section is replaced by jj mode's checks.

- On `main`, branch first — `git checkout -b converge/<slug>` carries the working tree over.
- Stage by explicit path. Never `git add -A`, `git add .` or `git add -u`: the tree carries
  untracked scratch directories, `.out/` directories and `REVIEW.md`.
- The paths are the files the fixes touched, the new test files, and the `CMakeLists.txt`
  that registers them. When the target was the uncommitted working-tree change, its files
  are part of the same commit — that change and its fixes are one commit by design.
  Anything the phase-0 baseline already showed dirty and the loop never touched stays out.

```bash
git add <path> <path> ...
git diff --cached --stat
git status --porcelain
git commit -m "<Area>: <what the fixes do>"
git show --name-only --pretty= HEAD
```

Read `git diff --cached --stat` before committing and confirm the list is exactly what you
meant. Read `git show --name-only --pretty= HEAD` after, and confirm `REVIEW.md` is not in
it.

The subject is one short area-prefixed line with no body, in the repo's style — `MLIR: fix
the null grouping key`. Name the area the fixes share; when they span areas, the one the
correctness fixes are in. No co-author trailer and no footer.

## Certify

After the commit, post the run's review certificate on the pull request. The `Review
Certificate` workflow reads it into the `review-certificate/main` commit status, which
`main` requires before a PR merges. It passes when every correctness and efficiency finding
is fixed or rejected.

Only a branch or a PR target is certified. A path target reviewed part of the PR, so skip
the rest of this section and say so in the report. For a branch, run `target`. For a PR,
run `target --pr <n>` from that PR's branch, since the certificate's head is the commit just
made:

```bash
python3 scripts/review_certificate.py target [--pr <n>]
```

When the branch has no open PR (the loop branched off `main`, or the PR is not opened yet),
skip the rest of this section. The report then says no certificate was posted.

Otherwise write `build/review-certificate.json` with every finding REVIEW.md recorded,
under its id and with its final status:

```json
{
  "pr": 1450,
  "head": "<git rev-parse HEAD, the commit just made>",
  "reviewBase": "<the review base from phase 0>",
  "kind": "converge",
  "level": "xhigh",
  "iterations": 3,
  "stopRule": "triage left nothing to act on",
  "findings": [
    {
      "id": "C1",
      "category": "correctness",
      "file": "query/ir/DBLowering.cpp",
      "line": 412,
      "summary": "the limit attribute is signed, so a negative literal reaches the verifier",
      "status": "fixed",
      "note": "LimitAttrTest.NegativeLimitIsRejected"
    }
  ]
}
```

- `category` is `correctness`, `efficiency`, `simplification`, `test-coverage` or `style`,
  as triage sorted it.
- `status` is `fixed`, `rejected`, `deferred`, `recorded` (a test-coverage finding) or
  `open`. A reopened finding carries the status it ended with. A correctness or efficiency
  finding that ends deferred or open fails the certificate.
- `note` gives the test for a fixed correctness finding, the reason for a rejected one, and
  what it would take for a deferred one. A rejected or deferred finding without a note is
  refused.
- `stopRule` names the rule that ended the loop. When it was the cap, say the last round's
  fixes are unreviewed.

```bash
python3 scripts/review_certificate.py issue --input build/review-certificate.json
```

The script refuses when the review base comes after the PR's merge base: a working-tree
target on a branch that already had commits reviewed only part of the PR. Post nothing
then, and say in the report that /certify has to review the whole PR. The commit is not
pushed, so the verdict it prints is `pending` until the branch is.

## The report

A few plain lines, concrete numbers, no bullets-for-their-own-sake:

- how many iterations ran and which stop rule ended the loop.
- findings by category — fixed, rejected, deferred — naming each deferred one.
- the tests added, and that each failed before its fix and passes after.
- the commit subject and short sha, and the branch when the loop created one.
- the certificate's comment URL and the verdict it gives once the branch is pushed, or why
  no certificate was posted.
- that `REVIEW.md` is left in the working tree, untracked, for the user to read and delete.

Say plainly what did not converge. A deferred finding, an aborted fix, or a run that ended
at the cap is the most useful thing in the report.
