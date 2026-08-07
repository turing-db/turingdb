Write far fewer comments. The default is **none**. When one is genuinely
warranted, cap it at **2-4 lines**.

**Why:** User feedback: "You write too much comments." Generated code arrives
padded with narration that restates the code, section banners, and doc blocks on
every function. It bloats diffs, goes stale, and buries the rare comment that
actually carries information. If code needs prose to be understood, the fix is a
better name or a smaller function — not a paragraph above it.

**How to apply:**
- Write no comment unless the code genuinely cannot express the point.
- Never restate the code (`// increment counter`, `// loop over rows`), never
  write section banners (`// ---- helpers ----`), never doc-block every
  public function by reflex.
- A justified comment explains a **why**: a non-obvious invariant, a subtle
  ordering constraint, a workaround for an external bug. Max 2-4 lines — longer
  explanations go in the commit message, the PR, or a design doc. The bar is
  high; when in doubt, leave it out.
- Never annotate the project's own conventions. The unreachable `break;` after a
  `case` body's `return`, plainly-named unused parameters, no `= default` in
  headers — these are house style, not oddities needing `// unreachable` or
  `// intentionally unused`.
- `CODING_STYLE.md`'s examples all carry `//` markers (`// Good:`, `// Bad:`,
  `// Do something`) to label the snippets. Don't read them as a model for how
  much to comment real code.
- No change-log narration (`// was previously X`, `// NEW:`, `// fixes bug ...`);
  git history covers that.
- Never re-add a comment a reviewer deleted, and don't sprinkle comments into
  untouched code you're merely editing near — see
  [[feedback_no_build_during_iteration]] on keeping diffs minimal.
- Standing exception: the free-function layer at the top of `tools/*` drivers
  with a `main()` gets one one-line description each — see
  [[feedback_doc_utility_functions]].
