---
name: feedback-no-shared-ptr
description: "Never introduce std::shared_ptr; reach for raw pointers, references, or unique_ptr first"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

Do not introduce `std::shared_ptr` in new code, even when a third-party API uses it.

**Why:** The codebase's CODING_STYLE.md rule "Only use `std::unique_ptr` for ownership; no `std::shared_ptr`" is enforced strictly. User has repeated this preference even when integrating libraries (e.g., Arrow) whose API surface is shared_ptr-heavy — that's not a justification to leak shared_ptr into our code.

**How to apply:**
- Prefer the C++ API variants that return `unique_ptr` (e.g., `parquet::ParquetFileReader::OpenFile` instead of `parquet::arrow::OpenFile`) when they exist.
- When a third-party call's *output parameter* is shared_ptr, dereference immediately into a raw pointer/reference and keep the shared_ptr lifetime to the smallest possible scope inside the .cpp.
- Never store shared_ptr as a class member or expose it in any header / public API.
- If unavoidable, surface the constraint to the user before writing the code instead of using shared_ptr silently.
