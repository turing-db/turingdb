---
name: feedback-friend-placement
description: Friend declarations go at the top of the public section, no `class` keyword
metadata:
  type: feedback
---

`friend` declarations in a class belong at the very top of the `public:` section, and the `class` keyword should be omitted (write `friend SlotReservation<T>;`, not `friend class SlotReservation<T>;`).

**Why:** Project style per CLAUDE.md / CODING_STYLE.md interpretation by the user (Remy). Putting `friend` at the top of `public:` makes the cross-class relationship immediately visible to readers, rather than burying it inside `private:` or mixing it with member declarations.

**How to apply:** Whenever adding a `friend` declaration to a class in this codebase, place it as the first line of the `public:` section and write `friend TypeName;` without the `class` keyword. Related: [[feedback-constructor-brace]].
