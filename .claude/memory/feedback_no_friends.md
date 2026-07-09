---
name: prefer-accessors-over-friend
description: Prefer targeted public getters/setters over `friend` declarations when a collaborator needs access to a type's internals
metadata:
  type: feedback
---

When a closely-related collaborator class (e.g., a merger) needs access to another class's private fields, expose targeted public getters and setters on the holder type rather than declaring `friend`.

**Why:** User rejected a `friend class ParquetPropertyMerge` declaration in `tools/turing-parquet/ParquetPropertyAnalysis.h` with "don't use friend but getters and setters". Preference for explicit public API over friendship — keeps the type's surface visible and avoids hidden coupling.

**How to apply:** Default to adding read accessors (`hasX()`, `getX()`) and narrow mutators (`markX()`, `addX()`, `setX()`) on the holder. The mutators should match the merge primitives needed (e.g., `addCount(size_t)`, `markMixed()`), not just generic field setters. Reach for `friend` only if you've discussed and confirmed it; never introduce it unprompted. When you do use `friend`, follow [[feedback-friend-placement]] for placement.
