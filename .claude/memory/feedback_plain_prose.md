Write plainly. Short declarative sentences, concrete numbers, no ornament.

**Why:** Remy on PR #867: "Your bullet points look like AI splot garbage. Make them
understandable. Look at how I write myself." The bullets were grammatical but
written in a mannered register that reads as machine-generated.

The tells, all from that one draft:
- Inverted or portentous phrasing: "That is how a null node or edge is spelled",
  "an edge having no null endpoint", "which is the anti-join".
- Hedged symmetry instead of a fact: "writes nothing where it can, and is rejected
  where it cannot" for "skips the null rows; CREATE is rejected".
- Trailing qualifiers that add nothing: "whatever the input chunk says", "again",
  "of its own".
- Abstraction where a number was available: "returns the rows it missed" instead of
  "returns all 8 people, 6 with a null friend".

Naming, from the follow-up questions on the same PR ("What does 'padded rows' mean?",
"There is no join in turingdb?"):
- Don't rename a thing the reader already knows. The clause is OPTIONAL MATCH; calling
  it "the join" made every bullet that used it undecodable. Prose in the tree may say
  "a left outer join of the rows in flight" as description - that does not make "the
  join" a name for the clause, and there is no join operator in the engine.
- A term the code uses is still unexplained on first contact in a PR. "Padded row"
  appears in NLTypes.td, DBLowering.h and ~20 test comments, and still needed defining
  in the bullet that introduced it.
- Introduce a term where it is first used, then reuse it. Don't reach for a synonym
  ("anti-join") that raises the same question again.

**How to apply:**
- One claim per sentence. Then the number, query, or name that shows it.
- Prefer numerals and concrete names: "0", "[]", "2 rows, not 8", "6 people with no
  KNOWS_WELL edge".
- Cut any clause that would survive deletion without loss of meaning.
- Don't restate a claim in a second, more elegant form.
- This applies to PR bodies, comments, and user-facing text alike - see
  [[feedback_minimal_comments]] for the comment-specific rule and
  [[feedback_pr_body_plain_sentence]] for PR shape.

Remy's own PR bodies are the reference: one imperative sentence, then a fenced code
block of bare facts (queries, or before/after IR dumps). **None of them use bullet
points.** Bullets appear only when asked for; the default is sentence + block.
