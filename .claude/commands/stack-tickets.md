---
description: Fix a list of Linear tickets in sequence, one fresh agent per ticket, as a stack of PRs
argument-hint: "TUR-201 TUR-205 TUR-198 [--converge] [--base <branch>]"
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, Agent, TodoWrite, ToolSearch, mcp__linear-server__get_issue, mcp__linear-server__list_comments
---

# Stack tickets

Fix each ticket in `$ARGUMENTS`, in the order given. Each ticket gets its own agent with a
fresh context, its own branch stacked on the previous ticket's branch, one commit, and one
PR whose base is the previous ticket's branch. The user reviews the stack on GitHub and
lands it with `/stack-merge`.

You orchestrate. You never edit code yourself. The agents never touch git branches,
GitHub or Linear. Only you do.

`$ARGUMENTS`:
- Ticket ids in stack order. Accept `TUR-201`, `tur-201`, `201`, or a Linear issue URL.
  Normalise all of them to `TUR-201`.
- `--converge`: each agent runs `/converge` on its change before committing. It costs up to
  4 review rounds per ticket, so it is off by default.
- `--base <branch>`: the branch the first ticket stacks on. Default `main`. Use it to add
  tickets on top of an existing stack.

`<scratch>` below is this session's scratchpad directory. `<repo>` is
`git rev-parse --show-toplevel`.

## Why the main checkout and not worktrees

The agents run one at a time in the main checkout and share `build/`. A git worktree would
need its own `external/dependencies`, and that tree bakes absolute paths into its CMake
files (see CLAUDE.md), so each worktree would rebuild LLVM and MLIR from source. Working in
sequence in one checkout keeps every build incremental.

## 0. Preflight

```bash
git status --porcelain
git branch --show-current
git fetch origin
```

- Any tracked modification aborts the run: report it and stop. Untracked files are fine.
  Record the full `git status --porcelain` output as **the baseline**. After every agent the
  tree must match it exactly.
- Record the current branch, so the run can return to it at the end.
- Check out the base and bring it up to date. For `main`:
  `git checkout main && git pull --ff-only`. For another base, check it out and confirm it
  equals `origin/<base>`. If it does not, stop.

For each ticket, load the Linear tools with `ToolSearch("select:mcp__linear-server__get_issue")`
and fetch the issue. Record its identifier, title, and `gitBranchName`. Branch names in this
repo look like `remyboutonnet/tur-163-support-relationships-with-both-arrow-heads`. If
`gitBranchName` is empty, use `tur-<n>-<slug of the title>`.

Stop before starting any agent if:
- a ticket id does not resolve,
- the same ticket appears twice,
- a branch name already exists locally or on `origin` (`git rev-parse --verify --quiet`
  and `git ls-remote --heads origin <branch>`).

Print the plan in a few lines: position, ticket, title, branch, parent branch. Then start.
Do not wait for confirmation. Keep a todo list with one entry per ticket.

## 1. Per ticket, in order

`<parent>` is the base for the first ticket. After that it is the branch of the last ticket
that produced a commit. A skipped ticket does not move it.

### 1a. Branch

```bash
git checkout -b <branch> <parent>
```

### 1b. Launch the agent

Launch one `general-purpose` agent with the prompt below, placeholders filled in. Wait for
its completion notification before you do anything else. Never run two ticket agents at
once: they share the checkout and `build/`.

The agent's context is its own. Pass it no notes from earlier agents beyond what the prompt
template lists. The commits below it are in the tree, and it can read them with `git log`.

```
You are fixing Linear ticket <TICKET>: "<title>".

Repository: <repo>. You are on branch <branch>, which is stacked on <parent>. The commits
below you in this stack are, bottom first:
<one line per earlier ticket: TICKET, commit subject, or "none">
Scratch files go in <scratch>/<TICKET>/, never in the repository.

Read CLAUDE.md and .claude/memory/MEMORY.md, with the files MEMORY.md links to, before
writing any code, unless they are already in your context. They bind you. In particular:
test first, a new test in its own file and CMake target, SimpleGraph for query behaviour,
build only the targets you need (never a bare `make -j8`, never `cmake ..`), no
`make run_regress`, and the C++ style rules, comments included.

1. Read the ticket: load mcp__linear-server__get_issue and mcp__linear-server__list_comments
   with ToolSearch, then fetch <TICKET> and its comments.
2. Reproduce it. Write a failing test that pins the correct behaviour first. Expected
   values are derived from the openCypher rules or the ticket, never from what the engine
   prints today. Build and run the test, and keep the failure output.
3. Fix it. Re-run the test and every test target that covers the files you touched
   (`ctest -R <pattern>` from build/). All must pass.
4. <COMMIT STEP>
```

`<COMMIT STEP>` without `--converge`:

```
Commit once. Stage by explicit path, never `git add -A`, `git add .` or `git add -u`.
   The subject is one short area-prefixed line with no body and no co-author trailer, in
   the style of `git log --oneline -20`. Do not put the ticket code in the subject.
```

`<COMMIT STEP>` with `--converge`:

```
Do not commit yourself. Run `git add -N <each new file>` so your new files count as
   part of the working-tree change, then invoke Skill(skill: "converge", args: "high"). It
   reviews your uncommitted change, fixes what it finds, and makes the one commit, with a
   subject in the repo's style. When it finishes, move REVIEW.md to
   <scratch>/<TICKET>/REVIEW.md.
```

The rest of the prompt:

```

Rules:
- Stay on <branch>. Do not check out, create, rebase or delete branches. Do not push, do
  not open PRs, do not edit the Linear ticket. The orchestrator does all of that.
- Never `git stash`, `git reset --hard`, `git checkout -- .` or `git clean`.
- Leave the tree as you found it apart from your commit: no stray files in the repository.
- You cannot ask the user anything. When the ticket leaves a small choice open, take the
  conservative option and say which in NOTES. When it needs a real decision (a product
  choice, two designs with different user-visible behaviour, a ticket too vague to test),
  skip it.
- Skip it too when the failing test passes before any fix: the ticket is already fixed on
  this branch. Report the test output as evidence.
- To skip: make no commit. Restore each tracked file you changed with `git restore <path>`
  and delete each file you created, by explicit path, so `git status --porcelain` shows
  what it showed when you started.

End your final message with exactly this block:

STATUS: DONE | SKIPPED
SUBJECT: <commit subject, or empty>
PR_SENTENCE: <one imperative sentence for the PR body, e.g. "Implement reduce.">
TESTS: <each test binary you added or ran, one per line, as build/... paths with pass/fail>
REASON: <why skipped, or empty>
NOTES: <choices you made that the reviewer should know about, one line each, or empty>
```

### 1c. Check what the agent left

Do not trust the report. Check it:

```bash
git branch --show-current
git rev-list --count <parent>..HEAD
git status --porcelain
git log --format='%s%n%b' <parent>..HEAD
```

- The branch must still be `<branch>`. If it is not, stop the run.
- `git status --porcelain` must equal the baseline. If it does not, stop the run and report
  the difference. Do not clean up: the user decides what the stray files are.
- **DONE** needs at least one commit. More than one: squash with
  `git reset --soft <parent> && git commit -m "<SUBJECT>"`. The commit must have no body
  and no `Co-Authored-By`; amend it if it does.
- Re-run each test binary the agent listed under TESTS. A failure stops the run: every
  ticket above a red commit would be stacked on a broken tree.
- **SKIPPED** needs zero commits. Delete the empty branch with
  `git checkout <parent> && git branch -D <branch>`, record the reason, and go to the next
  ticket with the same `<parent>`.

### 1d. Push and open the PR

```bash
git push -u origin <branch>
gh pr create --base <parent> --head <branch> --title "<TICKET>: <SUBJECT>" --body "<PR_SENTENCE>"
```

The title is the ticket code, a colon, then the commit subject. The body is the agent's
one sentence and nothing else: no headers, no bullets, no footer. The `tur-<n>` in the
branch name is what links the PR to the Linear ticket, so do not touch Linear.

`gh pr edit` fails on this repo. To change a PR afterwards use
`gh api -X PATCH repos/turing-db/turingdb/pulls/<n> -f body=...`.

Record the PR number and URL, then go to the next ticket with `<parent> = <branch>`.

## 2. After the last ticket

CI's `pull_request` trigger only fires for PRs whose base is `main`, so every PR above the
bottom one gets no CI. Run it once on the top of the stack, which builds every commit in
it:

```bash
gh workflow run ci_build.yml --ref <top branch>
```

Then return to the branch you started on.

## 3. Report

A few plain lines. List the stack bottom first, one line per ticket:

```
1  TUR-201  #1101  Cypher: implement coalesce over lists
2  TUR-205  skipped: the ticket asks for one of two null orderings and does not say which
3  TUR-198  #1102  MLIR: ...
```

Then the agents' NOTES, under the ticket they belong to, and the CI run started on the top
branch. If the run stopped early, say which check stopped it, what state the branch and
tree are in, and which tickets did not run. To rerun those tickets on top of the stack,
pass `--base <top branch>`.
