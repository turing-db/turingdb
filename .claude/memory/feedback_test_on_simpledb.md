---
name: test_on_simpledb_fixture
description: Test query/traversal behavior against the real SimpleGraph (simpledb) fixture, not a bespoke minimal graph
type: feedback
---

When adding a unit test for query/traversal behavior — especially to reproduce a
bug reported on simpledb — build the actual simpledb graph via
`SimpleGraph::createSimpleGraph(graph.get())` (link `turing_db_examples_s`,
include `SimpleGraph.h`) rather than constructing a purpose-built minimal graph.

**Why:** The user reproduces and regresses bugs against the same fixture the bug
was reported on. A synthetic graph is an extra thing to trust; the shared
simpledb fixture is the ground truth and its node IDs/counts are stable and
already relied on by other tests (e.g. `IndexLookupTest` hardcodes "Remy (0),
Adam (1)").

**How to apply:** Prefer `Graph::create()` + `SimpleGraph::createSimpleGraph`,
then assert against hand-derived expected counts on that graph. See
[[test_first_workflow]] and [[separate_test_file]].

**simpledb node IDs are not in the creation order of `examples/SimpleGraph.cpp`.**
Reading them off that file gets several wrong: `maxime` is created before
`padel` yet Padel=7 and Maxime=8, and Cyrus/Gym/Travel and Doruk/JiuJitsu are
transposed the same way. The verified mapping, taken 2026-09-07 from
`success-reads-order-by-1` in the query test suite (which the v2 engine passes,
so it is the engine's own answer):

    Remy=0, Adam=1, Computers=2, Eighties=3, Bio=4, Cooking=5, Ghosts=6,
    Padel=7, Maxime=8, Luc=9, Animals=10, Martina=11, Suhas=12, Gym=13,
    Travel=14, Cyrus=15, JiuJitsu=16, Doruk=17

Re-derive it from that fixture's `expect.result` (`MATCH (n) RETURN n, n.name
ORDER BY n.name ASC`) rather than from the node order in the C++.
