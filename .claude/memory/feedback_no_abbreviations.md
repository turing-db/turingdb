---
name: feedback-no-abbreviations
description: "No abbreviations like 'rg' for rowGroup; spell identifiers out"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 41939e8b-863b-4c54-ae5a-04b1412a4d71
---

Do not abbreviate identifiers. Spell them out in full, even when the long form repeats often.

**Why:** Reviewer preference for readable code. Examples called out: `rg` → `rowGroup`, never use the two-letter form anywhere — variable names, parameters, member fields, comments.

**How to apply:**
- Local variables: `rowGroup`, not `rg`.
- Members: `_currentRowGroup`, `_rowGroupReader`, `_rowsInRowGroup`, etc., never `_rg*`.
- Loop indices over row groups: `rowGroup` or `rowGroupIndex`, not `rg`.
- Likely generalizes to other obvious abbreviations (`col` → `column`, `idx` → `index`) — when in doubt, write it out.
