---
name: no-macos-strip
description: Don't strip binaries/dylibs on macOS in the turingdb wheel build — Linux only
metadata:
  type: feedback
---

In `build_backend.py`, the `_strip_binary` helper must early-return on `sys.platform == "darwin"`. Only Linux runs `strip`.

**Why:** macOS `strip` defaults are too aggressive on dylibs/.so (fails with "symbols referenced by indirect symbol table entries that can't be stripped") and even with `-x` it invalidates ad-hoc code signatures, which would require a `codesign --force --sign -` follow-up to keep binaries loadable on Apple Silicon. User chose to skip macOS entirely rather than carry that complexity.

**How to apply:** Do not propose enabling strip for macOS in this project — not with `-x`, not with `-S`, not with re-signing. If future work adds new binaries to the wheel, the existing Linux-only gate already covers them.
