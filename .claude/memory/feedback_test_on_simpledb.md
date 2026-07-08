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
then assert against hand-derived expected counts on that graph. Note simpledb
node IDs: Remy=0, Adam=1, Computers=2, Eighties=3, Bio=4, Cooking=5, Ghosts=6,
Maxime=7, ... See [[test_first_workflow]] and [[separate_test_file]].
