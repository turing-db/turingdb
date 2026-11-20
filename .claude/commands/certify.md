---
description: Review a PR's own diff, triage every finding, and post a review certificate on the PR
argument-hint: "[effort level] [PR number]"
allowed-tools: Bash, Read, Write, Glob, Grep, Skill
---

# Certify

Review exactly the diff a pull request carries, give every finding a category and mark it
rejected or open, and have the `Review Certificate` workflow post the result on the PR as
github-actions[bot]. The comment opens with ✅ when no correctness or efficiency finding is
open and ❌ otherwise. The workflow sets the `review-certificate/<base>` commit status, which
`main` requires before a PR merges. It passes when the certificate covers the PR's current
diff and no correctness or efficiency finding is open.

/certify fixes nothing. To fix the findings and certify in one run, use /converge.

`$ARGUMENTS` may carry an effort level (`low`, `medium`, `high`, `xhigh`, `max`) and a PR
number. Default: `xhigh`, and the PR of the current branch.

## 1. Fix the target

```bash
python3 scripts/review_certificate.py target [--pr <n>]
```

It prints the PR number, its base branch, the merge base, the head commit and the diff's
patch ID. On the PR's own branch the head is the local `HEAD`, pushed or not. On any other
PR it is the PR's head on GitHub. It refuses when the local `HEAD` does not contain the
PR's pushed head: a teammate pushed, or the branch was rewritten. Pull in the first case.
In the second, ask the user before certifying, since the push that follows is a force-push. The certificate covers `git diff <mergeBase> <head>`:
committed code only. Uncommitted changes are neither reviewed nor certified, so say so when
`git status --porcelain --untracked-files=no` is not empty.

## 2. Review

Invoke the built-in `code-review` skill on that range and nothing else:

```
Skill(skill: "code-review", args: "<level> PR #<n>: review exactly the diff `git diff <mergeBase> <head>`; ignore the working tree")
```

Never pass `--fix` or `--comment`. The review runs in the background and returns its
findings as a JSON array. Wait for it.

## 3. Triage

Read the code at every finding, then give it a category:

- **correctness**: wrong output, a crash, lost data, or a check that can be bypassed.
- **efficiency**: time or memory spent for nothing.
- **simplification**: reuse, dead code, or a simpler shape for the same behaviour.
- **test-coverage**: a behaviour no test pins.
- **style**: a broken CLAUDE.md style or comment rule.

The category is the kind of defect, not its size: a real bug is correctness however small.
Only correctness and efficiency findings decide the verdict.

Then give it one status:

- **rejected**: a false positive. The note names the evidence in one sentence: the code
  that rules the failure out, a test that passes on the claimed input, or one of
  CLAUDE.md's standing rejections (an analyzer rule turning away valid openCypher,
  use-later analysis in `DBProgramGenerator`, a comment that narrates the code).
- **open**: everything else. A real bug is open even when its fix is out of scope.

When in doubt, open. A rejection is shown on the PR next to its reason, and a reviewer will
read it.

## 4. Post

Write `build/review-certificate.json`, numbering the findings `F1`, `F2`, ... in the order
the review returned them:

```json
{
  "pr": 1450,
  "head": "<head from step 1>",
  "reviewBase": "<mergeBase from step 1>",
  "kind": "certify",
  "level": "xhigh",
  "findings": [
    {
      "id": "F1",
      "category": "correctness",
      "file": "query/ir/DBLowering.cpp",
      "line": 412,
      "summary": "the review's one-sentence statement of the bug",
      "status": "rejected",
      "note": "the evidence that makes it a false positive"
    }
  ]
}
```

Then post it:

```bash
python3 scripts/review_certificate.py issue --input build/review-certificate.json
```

The script recomputes the patch ID from `head` and dispatches the `Review Certificate`
workflow on `main` with the certificate. That run posts the comment as github-actions[bot],
deletes your earlier certificates on the PR, and sets the status. It keeps an earlier one
that covers the head on GitHub when the new one does not, so certifying an unpushed commit
leaves the pushed one certified. The script waits for the
run, then prints its URL, the comment's URL and the verdict on the PR as it stands.
`pending: the certificate covers another diff` means the head is not pushed yet. Only a
user with write access can dispatch it.

## 5. Report

A few plain lines: the comment URL, the verdict, the open correctness and efficiency
findings by id and file, and whether the branch still needs a push for the PR to carry the
certified diff.
