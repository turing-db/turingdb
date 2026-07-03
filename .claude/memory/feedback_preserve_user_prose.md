---
name: feedback_preserve_user_prose
description: When editing the user's own prose (docs, PR descriptions), make only the minimal change asked — don't restructure their sentences
type: feedback
---

When the user has written prose — a doc like COMPARISON.md, a PR description, a comment — and asks you to "clean it up" or fix something, change only what was asked. Preserve their wording and sentence structure. Don't rephrase a sentence into a different construction, and don't inject extra editorializing sentences into a section they authored (e.g. a Conclusion).

**Why:** On the commit-index-sim COMPARISON.md the user repeatedly rejected over-reaching edits: rewriting "reading the neighborhood of a node" as the possessive "a node's neighborhood" (they wanted only the minimal `of node` → `of a node` fix); appending an extra qualifying sentence to a Conclusion they had written; and, when asked to "add a mermaid diagram," wrapping it in a new `## Index structure` section with a heading, a NodeID bit-split line, and a caption — they wanted *just the diagram*, placed exactly where they said ("just before the last paragraph of Overview"). They want their voice kept intact and additions kept to exactly what was asked.

**How to apply:** Do exactly what was asked, nothing more. When adding an element (a figure, a row, a table), add that element only — don't invent a surrounding section, heading, intro, or caption unless asked. When touching existing prose, do the smallest possible edit: fix the exact nit and nothing more, never restructure a sentence you weren't asked to, don't add sentences to sections the user wrote. If they specify placement, follow it literally. Relates to [[feedback_code_spacing]] (their style is short sentences, blank lines between, bullet lists, present tense).
