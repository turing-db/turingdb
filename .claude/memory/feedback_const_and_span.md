---
name: Use const and span preferences
description: User prefers const on locals when possible, and std::span over std::vector for non-owning parameters and members
type: feedback
---

Use `const` on local variables whenever they are not mutated — e.g. `const size_t remaining = ...`.

**Why:** User corrected omission of const on two local size_t variables. The CODING_STYLE.md already says "Use const extensively" — this applies to locals too, not just parameters.

**How to apply:** When declaring local variables, default to const unless the variable needs to be mutated later.

Prefer `std::span` over `std::vector` for non-owning data references in both parameters and member variables. If the processor/class doesn't need to own the data, store a span rather than copying into a vector.

**Why:** User corrected both the constructor parameter and the member from vector to span. The data is owned externally and the processor just needs a view.

**How to apply:** When a class receives a collection it doesn't need to own or resize, use `std::span<const T>` for both the parameter and the stored member.
