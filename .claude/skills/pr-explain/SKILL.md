---
name: pr-explain
description: "WHAT: Explains a pull request to a reader — a Matt Pocock `pr`-shaped write-up plus a PR Lens animated diagram, posted as a PR comment. WHEN: asked to explain, walk through, summarise or document a pull request, or when a reviewer needs to understand a change without reading the diff first. KEYWORDS: pr-explain, explain PR, walk through a PR, PR diagram, PR Lens, review help"
allowed-tools: Bash, Read, Glob, Grep, Skill, TodoWrite
---

# Explain a pull request

Compose an explanation of a pull request and post it as a comment on that pull request. The
`pr` skill gives the write-up its shape. PR Lens draws the change as an animated diagram.

## Arguments

`$ARGUMENTS` can hold a PR number, a PR URL, or a branch name. An empty argument means the
pull request of the current branch.

Two flags change the run:

- `--local` renders the diagram but pushes nothing and posts nothing.
- `--no-diagram` writes the explanation alone and skips every PR Lens step.

## House rules

These hold on every run.

1. Never edit the pull request body or its title. The explanation goes in a comment.
   This repository requires a body of one plain sentence. A comment breaks no such rule.
2. Post nothing without a yes from the user.
3. Push nothing to `prlens.dev` without a yes from the user. A push sends a description of
   the change to a third party.
4. Never commit `.pr-lens/`. The CLI adds it to `.gitignore`.

## Steps

### 1. Resolve the target

Read `$ARGUMENTS`. A number or a URL names the pull request directly. A branch name needs
`gh pr view <branch>`. An empty argument needs `gh pr view`.

```bash
gh pr view ${TARGET:-} --json number,title,url,baseRefName,headRefName,isDraft
```

Stop and say so when no pull request exists, unless the user passed `--local`. With
`--local` and no pull request, explain the current branch against its base instead.

### 2. Read the change

```bash
git merge-base <baseRefName> <headRefName>
git diff --find-renames <mergeBase>...<headRefName>
git log --oneline <mergeBase>..<headRefName>
```

Open the files the diff changes. Find the reason for the change, not only its shape. The
commit subjects, the linked ticket and the PR title carry that reason more often than the
diff does.

### 3. Draft the explanation

Invoke the `pr` skill and follow its template exactly: `## Summary`, `## Evidence`,
`## Merge Danger`.

```
Skill(pr)
```

Three reminders for this command:

- Pick the smallest view that makes the point. Pseudocode, a call tree, a file tree, a
  Mermaid sequence or a diff sketch — one or two of them, never all of them.
- Fill `Evidence` with the real test names and the real output. Run the tests when the
  repository makes that cheap. Say plainly when you did not run them.
- Use the repository's own vocabulary. Read `GLOSSARY.md` and `CLAUDE.md` when they exist.

Stop here on `--no-diagram`, and go to step 7.

### 4. Resolve node

The PR Lens CLI needs node 20.11 or later. The default node is often older.

```bash
export PATH="$(<base directory>/scripts/resolve-node.sh):$PATH"
```

`<base directory>` is the path printed at the top of this skill when it loads. Use it as
printed. Do not derive it from `git rev-parse --show-toplevel`: in a jj workspace that
resolves to the main checkout, which is a different directory.

The script prints the bin directory of a good node, or exits 1. On exit 1, tell the user:

> PR Lens needs node 20.11 or later, and this machine has none. Install node 22, then run
> `/pr-explain` again.

Then continue without a diagram, and go to step 7.

### 5. Draw the diagram

Invoke the `pr-lens` skill. It is a stub, so it sends you to the CLI for the manual that
matches the installed version. Read that manual before you write anything.

```
Skill(pr-lens)
```

```bash
npx @coldtea/pr-lens-cli@latest skill
npx @coldtea/pr-lens-cli@latest skill references
```

Then follow it: write `.pr-lens/graph.json`, run `validate`, fix every failure it names,
and run it again. Never render an invalid document. Never delete the element a failure
names to make the failure go away.

```bash
npx @coldtea/pr-lens-cli@latest validate .pr-lens/graph.json
npx @coldtea/pr-lens-cli@latest render .pr-lens/graph.json --theme neutral
```

Step 6 says why the theme is `neutral` and not `light`.

The render prints the directory it wrote. Read the file names from the `manifest.json`
there.

### 6. Publish the diagram

The diagram goes to the pull request twice: as an attached picture, and as a canvas link.
The picture keeps the reader on GitHub. The canvas carries what a picture cannot — the
`payload` sample traffic on a flow step, the drill-downs, and a camera you can drive.

**The picture.** `gh pr comment --attach` uploads the file with the comment and rewrites
the reference to the uploaded asset. It needs GitHub CLI 2.99 or later:

```bash
gh --version
```

On an older `gh`, skip the attachment and say so in one line. The canvas link is then the
only route every reader can open.

Step 5 rendered `--theme neutral` for this reason: a Markdown image cannot carry a light
and dark pair, because `gh` leaves an HTML `<picture>` as written and appends the file at
the bottom instead. The neutral render brings its own background and reads in both GitHub
themes.

**The canvas.** Ask the user first, in one line:

> Push the diagram to prlens.dev? It becomes an unlisted page that its link reaches.

On a yes:

```bash
npx @coldtea/pr-lens-cli@latest canvas push .pr-lens/<drawing>/drawn.graph.json
```

It prints three links. Keep the view link, `https://prlens.dev/c/<id>`. Never put the edit
link, the one that ends in `#w=`, in a comment or anywhere public.

**The push is the optional half. Never stall on it.** Skip it and carry on with the
attachment alone whenever any of these holds:

- the user said no, or passed `--local`;
- a permission rule, a classifier or a sandbox refused the command;
- the CLI failed, with no network or a `CANVAS_*` error code.

Do not retry a refused push, and do not reach the same host another way. Finish the run,
post the comment, and close with the one command the user can run:

> The canvas was not pushed. To get the explorable version, run:
> `npx @coldtea/pr-lens-cli@latest canvas push .pr-lens/<drawing>/drawn.graph.json`
> Then send me the link and I will add it to the comment.

### 7. Assemble the comment

Open every comment with this callout, before anything else:

```markdown
> [!NOTE]
> AI-generated summary.
```

Then the diagram, at the top of `## Summary`, above the text it supports. Reference the
**local** file, because `gh` rewrites that path to the uploaded asset:

```markdown
## Summary

![<one line that says what the diagram shows>](.pr-lens/<drawing>/<view>-neutral-<hash>.svg)

[Open the animated diagram](https://prlens.dev/c/<id>)
```

Write the canvas line only when a push actually returned a link. With no push there is no
link, and the comment carries the attached diagrams alone. Never write the line with a
placeholder in it, and never guess a canvas id.

Three rules `gh` imposes on the reference:

1. It must be a Markdown image, `![alt](path)`. An HTML `<img>` or `<picture>` is left as
   written, and the file lands at the bottom of the comment instead.
2. The alt text is what a reader without images gets. Say what the diagram shows, in one
   line.
3. Pass one `--attach` for every diagram the comment references. A file the body does not
   reference is appended at the bottom instead, and `--attach '<file>#<alt text>'` names it
   there. Up to 50 files go in one command.

Attach the views a reviewer needs and leave the rest in `.pr-lens/`. Lead with the
architecture view. Add a data flow when the change has a sequence worth following. Two
diagrams read better than four.

The rest follows the `pr` template. Write the whole comment to `.pr-lens/body.md` and print
it in the terminal.

### 8. Post it

Stop with `--local`. Otherwise ask, then post:

```bash
gh pr comment <number> --body-file .pr-lens/body.md \
  --attach .pr-lens/<drawing>/<view>-neutral-<hash>.svg
```

Never run `gh pr edit`. That flag writes the description, and the description is the
author's. Report the comment URL.

To change a comment already posted, edit it in place: read the live body with `gh api
repos/<owner>/<repo>/issues/comments/<id>`, change that text, and PATCH it back. Never
re-post `body.md` over it. GitHub rewrote the image paths on upload, and the local file
still holds the `.pr-lens/` paths, which resolve to nothing for a reader.

## Notes

A canvas link reaches anyone who holds it, with no login. On a private repository, say that
to the user before the push in step 6.

The PR Lens GitHub App and GitHub Action draw the same diagrams on every push, with no
agent in the loop. This command does not install either one.
