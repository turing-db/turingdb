---
name: reference_reactome_copies_outdated
description: the old reactome copies (~/.turing/graphs, ~/.turing-bench, ~/reactome-parquet-dump) fail with "File outdated"; a loadable one built 2026-09-29 sits in ~/.turing-uniq, and the parquet dump converts to LOAD JSONL in 33 s when the format moves again
metadata:
  type: reference
---

`load graph reactome` on `~/.turing-bench` (a copy of `~/.turing/graphs/reactome`, the S3
copy of 2026-04-14) and `LOAD PARQUET 'reactome' AS reactome` over `~/reactome-parquet-dump`
both fail on a current build with "Graph dump/load error: File outdated": the dump's
minimum version (`UP_TO_DATE_VERSION` in `storage/dump/DumpConfig.h`) was raised four
times since, last on 2026-09-11.

A loadable copy was rebuilt on 2026-09-29 at `~/.turing-uniq/graphs/reactome` (1.2 GB):
`./build/tools/turingdb/turingdb -turing-dir ~/.turing-uniq -p <port>` then
`load graph reactome`. When the format moves again, regenerate it from the parquet dump:
with pyarrow, read `commits/*/labels.parquet`, `labelsets.parquet` (four 64-bit words of
label bits), `edge-types.parquet` and `property-types.parquet`, then the one datapart's
`node-ranges.parquet` (label set per node id range), `node-props-<id>.parquet`
(`entity_id`, `value`) and `edges-out.parquet` (`edge_id`, `node_id`, `other_id`,
`edge_type_id`), and write APOC-shaped JSONL: `{"type":"node","id":..,"labels":[..],
"properties":{..}}` and `{"type":"relationship","id":..,"label":TYPE,"start":{"id":..},
"end":{"id":..},"properties":{}}`. 14.5M lines, 2.2 GB, 33 s to write; the import through
`LOAD JSONL 'reactome.jsonl' AS reactome` runs a few minutes and persists the graph.
`stId`, `displayName`, `schemaClass` and `speciesName` are the properties the bench
queries read.

Rules of the shell learnt on the way: a graph that fails to load at startup ends the
process with EXIT_FAILURE (`tools/turingdb/StartCmd.cpp`), so point `-turing-dir` at a
directory whose `default` graph is loadable or absent; `LOAD JSONL 'name'` and
`LOAD PARQUET 'name'` resolve `name` under `<turing-dir>/data/` and reject a symlink there;
and `build/tools/turingdb/turingdb` is not relinked by a test-only `make`, so run
`make turingdb` before measuring, or the shell runs the engine from before the change.
`bench/vlp/clients.py` shows how to drive the shell over a pipe and read "Query executed in
N ms." back. See [[reference_adhoc_query_cli]] for simpledb-sized probes.
