A PR body is a bare imperative statement of intent - "Implement system-level
statements." - not a one-sentence summary of the scope or the design.

**Why:** CLAUDE.md's PR style already says "one short sentence", and a body can
obey the letter of that and still break it. On PR #813 the body was written as:

> Runs every system-level statement — LOAD GRAPH, the imports, change management,
> S3, procedures, extensions, vector and property indexes — through the MLIR
> engine, as one typed db op per command lowered one-for-one to its nl sibling.

Remy replaced the whole thing with "Implement system-level statements." Listing
the areas covered and describing the lowering strategy is review material: it is
in the diff and the commit history the reviewer is already reading. Packing it
into the body turns one line into a paragraph in disguise, which is exactly what
the no-template rule exists to stop.

**How to apply:**
- Write the body as if it were a commit subject: imperative, no enumeration of
  what is covered, no design detail, no em-dash clauses stacking extra claims.
- Do not restate the title with adornments. "Implement X." is a complete body.
- Reference material below the sentence is fine when asked for: on the same PR
  Remy then asked for a fenced code block listing every statement's grammar. The
  objection is to explanatory prose, not to content - an exhaustive list, grouped
  with blank lines and carrying no headers or bullets, is welcome as a code block.
