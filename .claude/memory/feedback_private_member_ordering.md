---
name: feedback_private_member_ordering
description: In private sections, member variables come before private member functions
type: feedback
---

Private static/member functions should be declared after member variables in the private section, not before.

**Why:** User preference for consistent ordering within class declarations.

**How to apply:** When adding private member function declarations, place them after all private member variable declarations.
