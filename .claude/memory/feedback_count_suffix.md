Name a variable that holds a count with a `Count` suffix so the name reflects that it is a count.

**Why:** The user renamed `currentDataParts` → `currentDataPartsCount` (and by the same logic `maxDataParts` → `maxDataPartsCount`) "to reflect that it is a count." `currentDataParts` reads like it might be the parts themselves; `currentDataPartsCount` is unambiguously the number of them.

**How to apply:** When a `size_t`/integer local, member, or parameter stores *how many* of something, suffix it with `Count` (`rowGroupCount`, `pendingNodesCount`, `maxDataPartsCount`) rather than reusing the plural noun of the thing being counted. Pairs with the no-abbreviations rule [[feedback_no_abbreviations]] and hoisting getter results into locals [[feedback_no_auto_for_casts]].
