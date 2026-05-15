---
name: Replication motivation — both scale and write availability
description: The TuringDB replication / distribution initiative is driven by BOTH scaling past single-node memory AND active-active write availability, not just one
type: project
originSessionId: 71910c03-9929-4504-bb03-0636b3175b2c
---
The replication / distribution initiative is motivated by *both* (a) scaling beyond single-node memory (which forces partitioning) and (b) write availability across network partitions / regions (which motivates CRDT-style merge).

**Why:** A pure CRDT-replicate-the-whole-graph design solves (b) but not (a). A pure shard+leader design solves (a) but not (b). The realistic landing point is a layered hybrid — partition for scale, replicate within partition for HA/read-scale, CRDT across regions for cross-region write availability.

**How to apply:** When evaluating any replication or distribution proposal, check it against both goals — don't conflate them. They motivate different mechanisms (partitioning vs. CRDT merge) and slot into different layers. The current REPLICATE.md PoC plan focuses on the CRDT/availability axis; partitioning is a parallel workstream that will eventually be the outer layer.
