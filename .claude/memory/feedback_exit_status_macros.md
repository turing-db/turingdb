---
name: feedback-exit-status-macros
description: "In main, return EXIT_SUCCESS / EXIT_FAILURE from <stdlib.h>, not literal 0 / 1"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

In `main()` use `EXIT_SUCCESS` / `EXIT_FAILURE` (from `<stdlib.h>`) instead of literal `0` / `1`.

**Why:** User corrected `return 0;` to `return EXIT_SUCCESS;` in `samples/parquet-import/main.cpp`.

**How to apply:** Any `return` from `main` in new code. Include `<stdlib.h>` for the macros. Don't churn existing samples that use literal `0` / `1`.
