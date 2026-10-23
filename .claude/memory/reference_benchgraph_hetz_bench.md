---
name: reference_benchgraph_hetz_bench
description: Memgraph's benchgraph (mgbench) with a turingdb vendor plugged in, on hetz-bench; how to run it and the traps (per-commit disk cost, shared count cache)
metadata:
  type: reference
---

mgbench is cloned at `hetz-bench:~/remy/benchgraph/memgraph/tests/mgbench` (sparse checkout of
memgraph 3033657) with a `turingdb` vendor added; the diff is `~/remy/benchgraph/mgbench_turingdb.patch`.
The vendor is a native `TuringDB` runner (`-demon`, PID from `<turing-dir>/turingdb.lock`, the imported
graph copied aside and restored before every cold start), a `TuringDBClient` in `python_client.py`
(HTTP keep-alive, `$params` inlined since the parser rejects them, writes wrapped in CHANGE NEW /
CHANGE SUBMIT) and a Pokec importer branch (cypher file -> JSONL -> `LOAD JSONL`). Run with
`--client-language python`; deps are in `~/remy/benchgraph/pylib` via PYTHONPATH. Memgraph 3.12.0 runs
natively through the wrapper `~/remy/benchgraph/bin/memgraph` (libs from the docker image), which reads
the host's `/etc/memgraph/memgraph.conf`, so WAL is on.

Traps learnt on 2026-10-01:
- The query-count cache (`.cache/config.json`) is keyed by query, not vendor: run memgraph first so
  every vendor runs memgraph's calibrated counts, as benchgraph publishes.
- Every turingdb CHANGE SUBMIT dumps ~27 MB (16 MB datapart + 2 MB per metadata file) regardless of
  the change's size, and in memory each commit costs O(commits so far). The calibrated 127k-175k write
  counts filled the shared disk (620 GB) once; cap write counts (5000) and run with a disk/RSS watchdog.
- Results and table from that run: `build/benchgraph/` in the repo (git-ignored), `report.py`.

See [[reference_hetz_bench]] for the box itself.
