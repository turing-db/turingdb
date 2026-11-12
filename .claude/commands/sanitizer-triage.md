---
description: Investigate the sanitizer nightly's failures, fix the simple ones as a stack of commits, file Linear tickets for the rest
argument-hint: "<LastTest.log> <output dir> <run URL>"
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, TodoWrite, mcp__linear-server__list_issues, mcp__linear-server__get_issue, mcp__linear-server__list_comments, mcp__linear-server__list_users, mcp__linear-server__save_issue, mcp__linear-server__save_comment
---

# Sanitizer triage

The CI Sanitizers nightly (`.github/workflows/ci_sanitizers.yml`) builds TuringDB with
`-DTURING_SANITIZE="address;undefined"` and runs every unit test 10 times, shuffled. Tonight
some tests failed. Investigate every failure. Fix each simple one as one commit on its own
branch, the branches stacked. File a Linear ticket for each of the others, assigned to the
person who wrote the faulty code.

You never push, never open a PR, and never write to GitHub. The workflow's next job pushes
your branches and opens the PRs from the manifest you write in step 7.

`$ARGUMENTS`, in order:
1. `<log>`: ctest's `LastTest.log`. Default `build/Testing/Temporary/LastTest.log`.
2. `<out>`: where you write `summary.md`, `prs.tsv` and `pr-<k>.md`. Create it if missing.
3. `<run>`: the URL of the workflow run, for tickets and PR bodies.

`build/` is the sanitizer build the nightly ran. It is configured and built, so a rebuild of
one target is incremental. The environment already holds the test step's `ASAN_OPTIONS`,
`LSAN_OPTIONS` and `UBSAN_OPTIONS`. `gh` has a read-only token. The repository is
`turing-db/turingdb`. Nobody can answer a question: decide, and write the reason down.

CLAUDE.md binds you, in particular "Fix the root cause, in its most general form",
"Undefined behaviour found in this code", "Triaging review findings" and the C++ style.

## 0. Preflight

```bash
git status --porcelain
git rev-parse HEAD
git fetch origin main
```

Tracked modifications stop the run: write that to `<out>/summary.md` and stop. Record
`git rev-parse HEAD` as `<base>`, the commit the nightly tested. `origin/main` may have moved
past it since the nightly started.

Write `<out>/summary.md` as you go, one line per finding as soon as its outcome is known.
The workflow stops you after about 3 hours, and whatever the file holds then is the report.

## 1. Collect the findings

- `build/Testing/Temporary/LastTestsFailed.log` lists the failed tests as `<number>:<name>`.
  The ctest name, the ninja target and the binary under `build/test/` share that name.
- Read each failed test's output in `<log>`. Look for `ERROR: AddressSanitizer`,
  `ERROR: LeakSanitizer`, UBSan's `runtime error:`, `SUMMARY:`, and the stack under each.
  Note the gtest iteration it happened in, the seed printed for that iteration
  (`Randomizing tests' orders with a seed of <seed>`), and the test case that was running.
- A failed test with no sanitizer report (a gtest assertion, a crash, a timeout) is a
  finding too.
- One finding is one root cause. Several tests failing on one defect are one finding. One
  test with two unrelated reports is two.

## 2. Is it already known?

For each finding, check:

- PRs: `gh pr list --repo turing-db/turingdb --state all --limit 200 --json number,state,title,headRefName,url,body`.
  An open PR that fixes it means there is nothing to do. A closed, unmerged PR from a
  `sanitizers/` branch for the same finding means a reviewer rejected that fix: do not
  open it again, file a ticket or comment on the existing one.
- main: `git log <base>..origin/main -- <files at fault>`. A commit after `<base>` may
  already fix it. Check that it does: read the commit, or rebuild and rerun on it.
- Linear: `list_issues` in team TuringDB with the file name, the function name and the
  test name as queries. Include issues closed in the last 30 days.

A known finding with an open PR, a fix on main or an open ticket gets one summary line
pointing there. Comment on an open ticket only when tonight adds something it lacks: a new
test that hits it, a new report shape, a reproduction. A repeat of the same report gets no
comment.

## 3. Reproduce

```bash
ninja -C build <test>
GTEST_REPEAT=1 GTEST_RANDOM_SEED=<seed> ctest --test-dir build -R '^<test>$' --output-on-failure
```

ctest sets the environment the test needs. Narrow with `GTEST_FILTER=<Suite.Case>`, then
reduce to the smallest input that still reports. A report that depends on the heap layout
may need several runs. CLAUDE.md describes how such bugs were found and pinned
(`LoweringDeterminismTest`, `CarriedColumnRowSourceTest`). Give up after about 10 tries. A
report you cannot reproduce still gets a ticket, built from the log.

## 4. Fix it here, or file it?

Fix it here only when all of these hold:
- You name the invariant that broke, the layer that owns it and the lines at fault. The
  frame that reported is often not the frame at fault: see "A null range: ask why the
  caller holds one".
- The root-cause fix is small: a few functions in one layer. It needs no design choice:
  no new type, op, interface, on-disk or wire format, and no change to what a valid query
  returns, other than a wrong answer becoming right.
- It is not tactical by CLAUDE.md's list. A sanitizer suppression, a disabled test, a
  caught exception or a raised limit never fixes a defect in TuringDB code.
- A test pins it: it fails on this build before the fix and passes after.

A leak whose allocation stack has no TuringDB frame (a third-party library's global state)
is fixed with an entry in `test/lsan_suppressions.txt`, in that file's style. A leak in
TuringDB code is never suppressed.

Everything else gets a ticket. When in doubt, file a ticket. A wrong fix costs a reviewer
more than a ticket does.

## 5. Fix, one branch per finding, stacked

`<parent>` is `<base>` for the first fix, then the previous fix's branch.

```bash
git checkout -b sanitizers/$(date -u +%F)-<short-slug> <parent>
```

- Write the test first, in its own file and CMake target. It reproduces the report in its
  smallest form. A test that fails only under the sanitizer build is fine: the nightly runs
  it.
- Fix the root cause. Rebuild only the targets you need.
- Rerun the new test, the failing test with its seed and `GTEST_REPEAT=10`, and every test
  that covers the files you touched (`ctest --test-dir build -R <pattern>`). All pass with
  no sanitizer report.
- Commit once, staging explicit paths. The subject is one short area-prefixed line in the
  style of `git log --oneline -20`, with no body and no co-author trailer.
- Append the branch to `<out>/prs.tsv` and write its PR body (step 7) right after the
  commit, before you start the next finding.

If the fix grows past step 4's bounds, or a test fails that you cannot explain, abandon it:
`git checkout <parent>`, `git branch -D` the branch, and file a ticket that says what you
tried.

Never `git stash`, `git reset --hard` or `git clean`. Never edit `.github/`.

## 6. File a ticket

Follow CLAUDE.md's "Filing one". Team TuringDB, label `Bug`, no priority. Project
`Query Engine` when the code at fault is under `query/`, `Quality and performance tracking`
otherwise. The description is for people: what goes wrong, which test hits it, and what
triggers it. Below it, a `+++ Agent notes` section holds the report's first lines and top
frames, the seed, the run URL, the lines at fault, the culprit commit, and your analysis.
When the fix needs a design choice, list the options with their costs and leave the choice
open.

Assign it to the person who wrote the faulty code:
1. Find the commit that introduced the defect: `git blame -w -C` or `git log -L` on the
   lines at fault, not on the frame that reported. Its author is the right person.
2. Find that author in Linear with `list_users` on team TuringDB, active users only. Git
   and Linear emails often differ (personal addresses, GitHub noreply addresses, accents in
   names). Match on email, then on the email's local part, then on the name, ignoring case
   and accents.
3. If the author has no active Linear account, take the active Linear user with the most
   commits to the file at fault in the last 6 months
   (`git log --since=6.months --format='%an <%ae>' -- <file>`).
4. If nobody matches, leave the ticket unassigned and name the commit's author in the
   Agent notes.

## 7. Manifest

`<out>/prs.tsv` has one line per fix branch, bottom of the stack first, with three
tab-separated fields:

```
<branch>	<reviewer>	<title>
```

- `<reviewer>`: the GitHub login of the culprit commit's author,
  `gh api repos/turing-db/turingdb/commits/<sha> -q .author.login`, or `-` when there is none.
- `<title>`: the commit subject.

`<out>/pr-<k>.md` is the body of line `<k>`, counted from 1. One imperative sentence, then a
fenced code block of bare facts: the failing tests, the seed, the report's first line and
its top TuringDB frames, the run URL. No headers, no bullets, no other prose.

With no fix, write no `prs.tsv`.

## 8. Finish

```bash
git checkout --detach <base>
git status --porcelain
```

The tree must be clean. `<out>/summary.md` ends as one line per finding, in plain words:
what failed, and its outcome: `fixed on <branch>`, `ticket TUR-N (assigned to <name>)`,
`known: #N` or `known: TUR-N`, `not reproduced, ticket TUR-N`. Your final message is that
summary.
