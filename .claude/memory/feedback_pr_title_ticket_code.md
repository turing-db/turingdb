When a PR fixes or implements a Linear ticket, start its title with the ticket code and a
colon: `TUR-124: Cypher: fix comparisons over null and mixed-type list elements`. Keep the
rest of the title as it would be otherwise.

**Why:** Remy asked for it on PR #1026 (2026-09-30) after it was titled with only the commit
subject. The repo's other ticket PRs already follow `TUR-NNN: <title>` (TUR-144, TUR-120,
TUR-130, ...).

**How to apply:** Take the code from the ticket the work came from, or from the `tur-NNN` in
the branch name. Commit subjects keep their plain area prefix (`Cypher: ...`); only the PR
title carries the code. No ticket, no prefix. See [[feedback_pr_body_plain_sentence]] for the
body.
