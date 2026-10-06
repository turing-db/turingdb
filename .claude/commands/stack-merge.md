---
description: Rebase a stack of PRs onto the latest main if needed, then land the whole stack with a rebase
argument-hint: "[top PR number or branch] [--no-ci]"
allowed-tools: Bash, Read, Glob, Grep, TodoWrite, Monitor
---

# Stack merge

Land a stack of PRs, such as one `/stack-tickets` made, on `main`. Rebase the stack onto
the latest `main` when it is behind, wait for CI on the top of the stack, then
fast-forward `main` to the top. Every PR in the stack ends up marked merged, and its
commits land with the SHAs that were tested.

`$ARGUMENTS`:
- The top PR of the stack, as a number or a branch name. Default: the PR of the current
  branch.
- `--no-ci`: skip the CI wait. Use it only when the user asks for it.

## Why a fast-forward push and not `gh pr merge --rebase`

`main`'s ruleset allows only rebase merges and linear history, and admins bypass the review
requirement. GitHub's "rebase and merge" always writes new commits with new SHAs. After the
bottom PR merges, the next PR still carries the old SHAs of the commit below it, so each PR
would need its own rebase, force-push and CI run before it could merge. A stack already
rebased onto `main` is a fast-forward of it. Retargeting every PR to `main` and pushing the
top commit to `main` lands all of them at once, and GitHub marks each PR merged because its
head commit is now on its base branch.

## 0. Preflight

```bash
git status --porcelain
git branch --show-current
git fetch origin
```

Any tracked modification aborts the run. Untracked files are fine. Record the current
branch, then `git checkout main && git pull --ff-only`, so no stack branch is checked out
while you move it.

## 1. Find the stack

Start from the top PR and walk down its bases until you reach `main`:

```bash
gh pr view <pr> --json number,title,headRefName,baseRefName,state,isDraft,reviewDecision
gh pr list --head <base branch> --state open --json number,headRefName,baseRefName
```

Each base other than `main` must be the head of exactly one open PR. Collect the PRs bottom
first. Then look one level above the top too: `gh pr list --base <top branch> --state open`.

Stop and report when:
- a PR in the chain is closed, merged, or a draft,
- a base is the head of zero or several open PRs,
- a PR has `reviewDecision: CHANGES_REQUESTED`,
- open PRs are stacked above the one you were given. The user meant a higher top, or wants
  only part of the stack landed; ask which.

Print the stack bottom first: number, branch, title.

## 2. Line up the local branches with origin

For each branch `b_k`, record `old_k = git rev-parse origin/b_k`. Then:
- no local branch: `git branch b_k origin/b_k`.
- local equals origin: nothing to do.
- local is an ancestor of origin: `git branch -f b_k origin/b_k`.
- otherwise the local branch has commits origin does not have. Stop and report it.

## 3. Rebase if needed

Compute the fork points **before** moving anything: `fork_k = git merge-base old_{k-1} old_k`
for k > 1. When the stack is linear, `fork_k` is `old_{k-1}`. When a lower PR gained
commits after the PR above it was made, `fork_k` is the older tip that `b_k` was forked
from, and replaying from it leaves the lower PR's new commits out of `b_k`'s range.

The stack needs no rebase when `origin/main` is an ancestor of `old_1` and each `old_{k-1}`
is an ancestor of `old_k`. Skip to step 4.

Otherwise, bottom first:

```bash
git rebase origin/main b_1
git rebase --onto b_{k-1} fork_k b_k     # k = 2..N
```

On a conflict, run `git rebase --abort`, put every branch back with
`git branch -f b_k old_k`, and stop. Report the PR, its commit, and the conflicting files.
Do not resolve the conflict: a hand-made resolution would land on `main` unreviewed.

A commit whose change is already on `main` is dropped by the rebase. Note it for the report.

When the rebase succeeds, push every moved branch in one atomic push, each lease pinned to
the SHA recorded in step 2:

```bash
git push --atomic origin --force-with-lease=b_1:old_1 --force-with-lease=b_2:old_2 ... b_1 b_2 ...
```

A rejected lease means someone pushed to that branch during the run. Stop and report it.

## 4. Wait for CI on the top

Skip this step with `--no-ci`.

CI runs once, on the top of the stack, whose tree contains every commit in it.

```bash
TOP_SHA=$(git rev-parse b_N)
gh run list --workflow ci_build.yml --commit "$TOP_SHA" --json databaseId,event,status,conclusion,createdAt
```

A run whose jobs were all skipped built nothing and does not count
(`gh run view <id> --json jobs`). Of the others:

- One has `conclusion: success`: CI is done, go to step 5.
- One is in progress: wait for the newest.
- None, or only failed or cancelled ones:
  - If step 3 pushed and `gh api repos/turing-db/turingdb/pulls/<top PR number> -q .stack`
    shows a stack whose `position` equals its `size`, the push starts the top PR's
    `pull_request` run. Actions runs `ci_build.yml` for every PR of a stack as if it
    targeted `main`, and the workflow builds only the top one. Poll `gh run list` until
    that run appears. Dispatch nothing.
  - Otherwise `gh workflow run ci_build.yml --ref b_N`, and poll `gh run list` until its
    run appears.

Wait on it with `gh run watch <id> --exit-status`, run in the background. A push to a
stacked PR can start two runs for the same commit, and the PR's concurrency group cancels
the older one. If the run you watch ends `cancelled`, wait on the newer run for `TOP_SHA`
instead. A run takes about 25 minutes. A failure stops the run. Report the failing job's
URL. The rebased branches stay pushed: that is harmless and saves the next attempt the
rebase.

## 5. Land

Re-check `main` first. CI took long enough for it to move:

```bash
git fetch origin
git merge-base --is-ancestor origin/main b_1
```

If `main` moved, go back to step 2 once. If it moves again during the second CI wait, stop
and report it.

GitHub refuses to change the base of a PR in a GitHub stack ("Cannot change the base branch
because the pull request is part of a stack"). If the top PR has a `.stack`, dissolve it
first:

```bash
gh api repos/turing-db/turingdb/pulls/<top PR number> -q .stack.number
gh extension list | grep -q github/gh-stack || gh extension install github/gh-stack
gh stack unstack <stack number>
```

`unstack` closes no PR and deletes no branch. GitHub keeps a PR that is queued for merge or
has auto-merge on in the stack: if any PR still has a `.stack` afterwards, stop and report
it.

Retarget every PR above the bottom one to `main`. Record each original base first, so a
failure can be undone:

```bash
gh api -X PATCH repos/turing-db/turingdb/pulls/<n> -f base=main
```

Then fast-forward `main` to the top:

```bash
git push origin b_N:refs/heads/main
```

Never pass `--force`. The push must be a fast-forward, and the ruleset rejects anything
else anyway. If a retarget or the push fails, retarget each PR back to its original base,
rebuild the stack with `gh stack link <n_1> <n_2> ... <n_N>` if you dissolved one, and stop
with the error. Do not fall back to `gh pr merge`.

## 6. Check

```bash
gh pr view <n> --json state,mergedAt -q .state
```

Every PR in the stack must read `MERGED`. GitHub can take a few seconds to notice; wait
with Monitor for up to two minutes. A PR still `OPEN` after that is reported with its
number. Do not close it.

Then `git checkout main && git pull --ff-only`.

When every PR reads `MERGED`, delete the stack's branches. Deleting the head branch of an
open PR closes it, so when any PR is not merged, keep them all.

```bash
git push origin --delete b_1 b_2 ... b_N
git branch -d b_1 b_2 ... b_N
```

`-d` refuses a branch that is not on `main`. If it does, stop and report the branch; never
fall back to `-D`.

Return to the branch the run started on, if it still exists.

## 7. Report

A few plain lines:

```
landed 3 PRs on main, 8c1d2e4..5f6a7b8, rebased onto 8c1d2e4
#1101  TUR-201  Cypher: implement coalesce over lists
#1102  TUR-198  MLIR: ...
#1103  TUR-207  ...
CI: <run URL>
```

Say whether a rebase was needed, whether a GitHub stack was dissolved, name any commit the
rebase dropped, and name any PR not marked merged. Say whether the branches were deleted.
If they were kept, give the command that deletes them:

```bash
git push origin --delete b_1 b_2 ... && git branch -d b_1 b_2 ...
```
