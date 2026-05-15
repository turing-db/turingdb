---
name: Use layer-appropriate exceptions
description: Use FatalException in storage layer, PipelineException in pipeline layer — respect architectural boundaries
type: feedback
---

Use the exception type appropriate to the layer:
- `FatalException` in `storage/` code (available via common)
- `PipelineException` in `query/pipeline/` code

**Why:** Storage cannot include pipeline headers (would create circular dependency). User initially requested PipelineException but corrected to FatalException when the build broke.
**How to apply:** Check which layer the code is in before choosing the exception type.
