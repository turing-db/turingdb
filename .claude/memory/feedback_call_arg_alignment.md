---
name: feedback-call-arg-alignment
description: "When wrapping a function call across lines, put one argument per line aligned under the first"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 6de4d846-73be-41ab-bfd7-5ce4471c910f
---

When a function call must wrap across multiple lines, don't pack two-or-three-per-line. Put **each argument on its own line**, all aligned under the first argument.

**Why:** User prefers vertical argument lists for readability when wrapping is necessary; the goal is uniform column alignment, not minimal line count.

**How to apply:**

Avoid (groupwise wrap):
```cpp
return readFixedLenByteArraySlice(columnReader, scratch, columnIndex,
                                  byteWidth, batchRows);
```

Prefer (one per line, aligned under first arg):
```cpp
return readFixedLenByteArraySlice(columnReader,
                                  scratch,
                                  columnIndex,
                                  byteWidth,
                                  batchRows);
```

This only applies when the call already needs to wrap. Single-line calls stay single-line — see [[feedback-no-overwrap]] / CLAUDE.md "don't overwrap".
