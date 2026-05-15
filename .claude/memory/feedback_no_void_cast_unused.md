---
name: feedback-no-void-cast-unused
description: "Don't add `(void)param;` lines to mark parameters as used — leave parameters named and that's it"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

Don't add `(void)param;` statements in function bodies just to silence unused-parameter warnings.

**Why:** User said "don't add (void)var just to mark the variable as used, I don't care." Combined with the no-`/*comment*/` rule and the fact that `-Wunused-parameter` isn't enabled in this codebase, there's nothing to silence — both patterns are noise.

**How to apply:** Function bodies in new code. Just name the parameters and leave it. Applies to both `(void)param;` cast-to-void and `(void)argc; (void)argv;` in main.
