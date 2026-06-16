---
name: reference_distribution_design_docs
description: Where the distributed/replication/GraphHub design docs live (scattered across sibling clones) and the unified identity model that reconciles them
metadata:
  type: reference
---

Distributed-TuringDB design work is spread across sibling clones in `$HOME`, not the primary repo. When the topic comes up, check these before re-deriving:

- `~/turingdb3/REPLICATE.md` — "Active-Active Replication PoC": branch-per-replica, reconciler merges to `main`, join-semilattice convergence (Tarski) + LWW. Proposes content-hashing DataParts/Commits; uses `hash(merge(a,b))==hash(merge(b,a))` as the convergence equality oracle. Latent gap: its `mergeDivergent` "set-union by ID" assumes disjoint entity-ID spaces it never supplies.
- `~/turingdb2/docs/GRAPHHUB.md` — mature push/pull-to-S3 hub design (data/control plane split, presigned URLs, ref CAS, Merkle integrity). Keeps random local `uint64` IDs "sacrosanct", content-hash as a parallel layer via `HubObjectMap` sidecar, and **rebases entity IDs on every push** — which defeats cross-replica dedup and forces it to defer active-active (its §14).
- `~/turingdb-hub-spec.md` — package-registry framing of the hub (lockfiles, `sha256:<hash>` DataParts, supply-chain integrity).

The three agree on content-hashing DataParts but **disagree on entity identity** (rebase vs unique IDs) — the one axis that decides whether content-hashing actually converges across replicas.

Unified resolution (written 2026-06-15): **`/home/ubuntu/turingdb/docs/IDENTITY_MODEL.md`**. Three identity regimes — assigned-opaque (`SiteID`, `GraphID`, site-prefixed `NodeID`/`EdgeID`), content-derived (`DataPartID`/`CommitHash` = sha256 of canonical bytes), name-resolved (schema). Site-prefixed entity IDs (`[siteIndex:16b][local:48b]`, `NodeID` stays 64-bit) make union rebase-free AND make content hashes stable across replicas, so content-hashing finally pays off. Serves both GraphHub manual sync and active-active. Key insight: rebase and content-hash dedup fight each other; stable IDs end the fight.

Related: [[project_replication_motivation]], [[project_partitioning]].
</content>
</invoke>
