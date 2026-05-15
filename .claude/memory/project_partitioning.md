---
name: Partitioning roadmap and METIS interest
description: Graph partitioning is on the near-term TuringDB roadmap; some customers have specifically asked for METIS-style structural partitioning
type: project
originSessionId: 71910c03-9929-4504-bb03-0636b3175b2c
---
Graph partitioning is on the near-term roadmap as of 2026-05-04. Some customers have expressed interest in METIS specifically — i.e. structural edge-cut minimization, not hash partitioning.

**Why:** Some customer graphs won't fit in a single node's memory; partitioning is the only path past that ceiling. METIS is a direct customer ask, not just a default — so structural / cut-minimizing partitioners are in scope, not just hash schemes.

**How to apply:** Treat partitioning as a near-term constraint, not a future-optional. Distribution / replication designs that assume the full graph lives on one node (e.g. pure full-replication CRDT) are insufficient for scale even if they work for the current single-node analytical profile. When discussing scale-out, default to METIS-style structural partitioning rather than hash, unless there's a reason to switch.
