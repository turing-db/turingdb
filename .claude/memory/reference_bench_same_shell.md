---
name: reference_bench_same_shell
description: reactome timings vary up to 2x between turingdb shell processes; compare a query and its control alternately in one shell
metadata:
  type: reference
---

A query's engine time on reactome is stable within one `turingdb` shell process but not
across them. Measured 2026-09-30: `MATCH (p:TopLevelPathway)--(b) MATCH (b)--(c) RETURN
count(*)` ran 60, 63, 80, 91, 94 and 104 ms in six fresh shells, and the query it controls
ran 60 to 113 ms. The variance sits in `gatherColumn`, and 20 runs in one shell stay
within 1 ms.

**How to apply:** when comparing a query against its split-clause control, run them
alternately (Q, C, Q, C) in the same shell, and repeat over two or three shells. A
before/after table built from separate shells can be off by 2x. `perf stat` counts
instructions and cache misses per run, which separates extra work from memory stalls.
See [[reference_reactome_copies_outdated]] for the shell setup.
