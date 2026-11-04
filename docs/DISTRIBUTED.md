# Distributed TuringDB — High Availability & Replication Design Notes

> **Status:** design exploration / decision record. Nothing here is implemented yet.
> This document summarizes the alternatives considered for making TuringDB
> highly available and multi-writer, the reasoning behind each fork, and the
> decision reached. **The core architecture is decided** (§2.3); what remains
> open is tactical and listed in §12.

---

## 1. Goals and constraints

The replication / distribution initiative is driven by **two** independent goals, and any
proposal must be evaluated against both:

1. **Scale beyond single-node memory** → forces *partitioning* of the graph.
2. **Write availability across network partitions / regions** → motivates *multi-writer,
   merge-based* replication.

Constraints that shape every decision below:

- **Workload is read-heavy and analytical**, millisecond latency, in-memory column store.
  Writes are a minority of traffic; **RAM is the scarce resource**.
- **Existing substrate is already a version-control machine**: immutable, content-addressed
  **DataParts**; git-like versioning (a **commit DAG**); **snapshot isolation**; reads go
  through a **CommitView** (the set of visible DataParts) via per-`NodeID` property iterators.
- **Partitioning will be METIS-style structural** (edge-cut minimization), *not* hash
  partitioning — customers asked for graph locality, not random scatter.

The single most important observation: **the storage layer is already shaped like a
mergeable VCS.** Most of the design below is "lean on what we already have" rather than
"bolt on a replication subsystem."

---

## 2. The fundamental choice: CP (Raft) vs AP (CRDT/gossip)

### 2.1 The CALM lens (the tool that actually decides this)

The **CALM theorem** (Consistency As Logical Monotonicity) is more useful than CAP here:

> A computation can be made eventually-consistent and **coordination-free if and only if it
> is monotonic** — output only grows as input grows; nothing is retracted.

Mapped onto a graph engine:

- **Monotonic / coordination-free:** appending nodes & edges, bulk ingest, growth.
- **Non-monotonic / needs coordination:** deletes, property updates, aggregation/`count`,
  negation (`NOT EXISTS`), and **all global invariants** (uniqueness, referential integrity,
  schema).

Consequence: **you cannot escape coordination for the non-monotonic parts.** "Pure CRDT,
never coordinate" is not on the menu if uniqueness/referential integrity matter. So the real
question is **not "Raft or CRDT"** — it is **"where do we draw the coordination boundary,"**
and for a read-heavy, append-mostly engine the answer is "most traffic on the cheap path,
a small slice coordinated."

### 2.2 Alternatives, with advantages and disadvantages

| | **Raft / leader election (CP)** | **CRDT / CALM / gossip (AP)** |
|---|---|---|
| Consistency | Linearizable; clean total order; snapshot isolation "just works" | Eventual / causal+; merge-based; no global order |
| Write under partition | **Minority can't write** (contradicts goal 2) | **Always available** (serves goal 2) |
| Write latency (geo) | WAN RTT per commit if replicas span regions | Local commit, async replicate |
| Invariants (unique/ref-integrity) | Enforceable (single serialization point) | Needs coordination anyway (CALM) |
| Cypher non-monotonic ops | Semantically clean | Need tombstones / LWW / MV-registers |
| Memory cost | No per-entity causal metadata, no tombstones | Tombstones + causal metadata (bad for in-memory) |
| Fit with our substrate | Bolts a replicated log onto a merge-shaped store | **Branch/merge IS the substrate** |
| Failure mode | Leader bottleneck + election gap | No leader; GC of tombstones is the residual cost |

### 2.3 Decision (settled)

**Uniform gossip + CRDT with version vectors — AP everywhere. No intra/inter-cluster split.**

We are **not** taking a layered hybrid (strong-consistency inside a cluster + merge between
regions). Every node is a writer; writes are accepted locally and propagate by **gossip**;
conflicts resolve through the CRDT / version-vector machinery in §4–§5 **uniformly** — the same
path whether two writers sit in one rack or different regions. There is **no leader and no
consensus on the data / write path.**

The *only* consensus in the system is a **small embedded Raft (or etcd)** holding **metadata
and schema** (§10.3): the writer roster, the schema + per-type merge-policy registry, and
(later) the METIS partition map. It sits **off the write path** — it changes rarely and never
gates a write — so goal 2 (write availability under partition) stays intact: a partitioned node
keeps writing against its last-known metadata.

This resolves the CP/AP question above in favor of AP, and deliberately rejects the
rebase-to-tip / per-region-tip alternatives (kept for the record in §6).

---

## 3. Chosen direction (overview)

Gossip-propagate immutable, content-addressed DataParts. Resolve conflicts at **read time**
by a fold over visible parts. Track causality at **DataPart grain** (version vectors / the
DAG). Resolve clashes by **per-type merge policies**. Avoid HLC and dangling edges by making
state a **join-semilattice** wherever possible. The hard, residual cost is **GC of
tombstones / old assertions**, which needs a causal-stability signal. Consensus appears in
exactly one place — a small embedded Raft/etcd for **metadata and schema** (§10.3) — and
**never on the write path.**

---

## 4. Data model & causality

### 4.1 Immutable content-addressed parts + commit DAG

- Parts are immutable and content-addressed → identical part = identical hash everywhere →
  gossip is anti-entropy over a **grow-only set of part hashes** (itself a CRDT; converges
  with no clock).
- The commit DAG's parent pointers **are** a causal structure — a version vector encoded as a
  graph. "Ship commits + merge" is most of inter-region replication.

### 4.2 Version-vector granularity: per-DataPart, **not** per-property-value

The key reframe: **causality grain ≠ resolution grain.**

- **Per-cell VVs (per `(entity, propertyType)`)** → precise but *billions* of vectors; fights
  the columnar layout; this is the Automerge memory model. **Rejected.**
- **Per-DataPart VV** (a cached `{writer → seq}` summary of the commit, `O(#writers)`) → cheap,
  matches the gossip/replication unit (as Dynamo/Riak attach VVs per object). **Chosen.**

Coarse causality does **not** cause coarse conflicts, because **cell-level resolution is
recovered at merge time** by a 3-way diff against the common ancestor — and a write-produced
DataPart *already records exactly which cells it changed*, so the part **is** the diff:

```
changedByX ∩ changedByY  →  only the truly overlapping cells need arbitration
non-overlapping cells     →  union, auto-merge, no heuristic
```

Precedent: **Git / Dolt** (commit-grain causality + cell/line-grain 3-way merge, no per-cell
VVs) — the right model for a commit-DAG store. Automerge's per-element metadata exists for
continuous editing without commits; we have commits, so we don't pay it.

### 4.3 Supersede vs concurrent

A version vector answers one question: *when a writer wrote, had it already seen the other
write?* Yes → **supersede** (take it, free). No → **concurrent** (arbitrate).

```
notation: [w1, w2] = (w1 writes reflected, w2 writes reflected)

SUPERSEDE                          CONCURRENT
  true  [1,0]   w1 writes            true  [1,0]   w1 writes (hasn't seen w2)
  false [1,1]   w2 saw true,         false [0,1]   w2 writes (hasn't seen w1)
                then wrote
  [1,1] dominates [1,0] → false      [1,0] ∥ [0,1] → neither dominates → arbitrate
```

The whole distinction is one number (did w2's vector include `w1:1`). A **Lamport scalar
discards it** — which is why a Lamport tie is unresolvable, and worse, why an *unequal*
Lamport pair can silently misorder two concurrent writes as if one superseded.

---

## 5. Conflict resolution

### 5.1 The unifying move: make state a join-semilattice

If every state component is **grow-only / add-wins**, merge = least-upper-bound
(commutative, associative, idempotent) → **order doesn't matter → no clock needed.** This is
what removes HLC: HLC exists to impose an order; semilattice merge doesn't need one.

### 5.2 Why not HLC; Lamport = *causal* recency, not wall-clock

- **HLC** staples physical time so the winner among concurrent writes ≈ wall-clock — but
  that is a *heuristic bounded by clock skew*, not correctness.
- **Lamport** (via the `max`-on-receive rule) gives correct **causal** recency: a write that
  *observed* the value it replaces always wins. For **concurrent** writes it picks
  deterministically-but-arbitrarily — same quality as a writerID tiebreak.
- Decision: **avoid HLC.** Use per-DataPart VVs to *detect* concurrency; resolve concurrent
  cases by per-type policy. If a few fields genuinely need recency, plain **Lamport** suffices
  (HLC's physical component only changes the rare concurrent case, heuristically).

### 5.3 The concurrent-tie problem and the five resolution strategies

A replicated register can only converge five ways. Every technique is one of these:

| Strategy | Resolves `true@w1` vs `false@w2` | Clock? | Loses data? | Systems |
|---|---|---|---|---|
| **A. Total-order tiebreak** `(vv, writerID)` or value-hash | deterministic arbitrary | no | yes | Lamport 1978; Cassandra LWW |
| **B. Physical-time** (HLC/TrueTime) | ≈ physically-later | physical | yes | CockroachDB, Spanner |
| **C. Keep both (MV-Register)** + version vector | store `{true,false}`, surface | no (needs VV) | no | Dynamo, Riak |
| **D. Semantic / typed CRDT** | `true` (enable-wins flag), counters sum, sets union | no | no | Riak DT, Antidote, Automerge |
| **E. Prevent concurrency** (consensus / ownership) | conflict never occurs | no | n/a | Raft, Calvin, rebase-to-tip |

**Chosen mix: C (detect) + D (resolve).** Per-DataPart VV to distinguish supersede from
concurrent; for the truly-concurrent case prefer a **typed merge** where the property has one
(boolean → enable/disable-wins flag, numeric → counter, set → OR-set), fall back to a
**deterministic `(…, writerID)` tiebreak** for opaque values.

Whole-system precedent worth reading: **Bayou** (1995) — gossip + per-app merge procedures +
tentative-then-stable writes + optional primary order. It is almost exactly this design.
**CouchDB** is its living descendant (gossip + deterministic winner pick, losers retained).

### 5.4 Avoiding dangling edges (no clock, no repair pass)

Two distinct causes, two fixes:

1. **Reordering (edge part arrives before its node part)** → **causal-closure visibility**: a
   part is not visible to queries until its content-hash dependency closure is local (git
   won't check out a commit without its ancestors). Eliminates this case entirely. The
   **CommitView is exactly the boundary that enforces this.**
2. **Concurrent delete vs. edge-add** → **add-wins existence** + referent pinning: a
   concurrent edge-add beats a delete; restore a thin "shadow" node rather than drop the edge.
   Never dangling; the residual imperfection is *resurrection* (acceptable — an edge implies
   its endpoints exist).

---

## 6. Considered and rejected: linear history (rebase-to-tip)

> **Rejected** in favor of uniform gossip CRDT (§2.3). Kept on record because it was the main
> alternative and the reasoning is worth preserving.

A GitHub-style **CAS-against-the-tip** rebase serializes writes into one causal chain →
**concurrency is prevented by construction**, and the chain position *is* the order (no clock
needed at all). The catch is the scope:

- **Within one coordination domain** (a region with a single tip authority / Raft group): a
  single linear `main`, zero concurrent writes, free.
- **Across a partition** a single global tip is impossible — the disconnected side can't
  commit (CP, loses goal 2). GitHub's linear history is a *single-origin* property, not a
  distributed one.

**Two variants were on the table:**

- **One global `main`** → strong consistency, *zero* concurrency, no clock — but a minority
  partition can't write (fails goal 2).
- **Per-region `main`, merged async** → preserves multi-writer HA; the only concurrency is
  **branch divergence at heal**, resolved by a **3-way merge** against the common ancestor.

**Why rejected:** both require a per-region *tip authority* (a coordination point) on the
write path — exactly the intra/inter-cluster split we decided against in §2.3. The per-region
variant also collapses into the gossip-CRDT model anyway: its "3-way merge at heal" is just a
less-uniform special case of the read-time fold (§8). We keep one uniform mechanism instead.
The **3-way-merge-against-the-common-ancestor** idea is *retained* and used inside the merge
path (§4.2, §7) — we rejected rebase-to-tip as the *write-path discipline*, not the
ancestor-aware merge itself.

---

## 7. Text / string property merge

Fork first — "text" is three different problems:

1. **Atomic string value** (name, label, status) → it's a *register*; LWW/keep-both like a
   boolean. A text CRDT here is **wrong** (don't Frankenstein two names).
2. **Document-ish text** with a common ancestor → **3-way structured merge** (diff3 +
   histogram, or structural/AST merge for JSON), reusing the DAG ancestor. Zero per-element
   metadata. Owes a deterministic conflict policy to stay coordination-free. (The Dolt model.)
3. **Genuine concurrent collaborative editing** → only this justifies a **sequence CRDT**.

Sequence-CRDT landscape (for case 3 only): OT (legacy, server-bound) → WOOT → Logoot/LSEQ/
Treedoc (id bloat) → **RGA** (workhorse) → **Yjs/YATA** (production, block-compressed) →
**Automerge** (JSON CRDT, columnar). Research frontier: **Fugue** (non-interleaving quality),
**Peritext** (rich text), **Eg-walker** (event-graph replay → *much* lower memory; relevant
to our RAM constraint).

**Decision:** register-LWW for short strings; 3-way merge for documents (default, reuses the
DAG, no metadata); reserve sequence CRDTs for schema-flagged collaborative fields only —
defaulting every string to a per-char CRDT would reintroduce exactly the metadata explosion
we avoided by putting VVs at DataPart grain.

---

## 8. The read path is already the merge engine

The existing per-`NodeID` property iterator that folds over visible DataParts in a CommitView
and **keeps only the last** is *already* a conflict-resolution engine — the local case is the
degenerate (totally-ordered) instance of distributed merge. "Keep last" = LWW where the order
is unambiguous. This is the LSM / Cassandra read-time-reconciliation pattern; we have ~80% of
it. To generalize for the distributed case:

- **Reuse:** the grouping/iteration skeleton (group by `(NodeID, propertyType)`, reduce over
  visible parts). Gossiped parts are just more layers.
- **Must change — the comparator (THE correctness trap):** today "last" = stack/arrival order,
  which differs per replica → divergence. Order layers by a **deterministic function of
  content** (DataPart-grain VV dominance + `writerID`/value-hash tiebreak), **never** by
  arrival order.

  ```
  P1=(w1,true)  P2=(w2,false)   concurrent
  Replica A receives P1,P2 → "keep last" → false
  Replica B receives P2,P1 → "keep last" → true     # divergence!
  fix: sort {P1,P2} by (vv, writerID) → both pick the same winner
  ```

- **Must generalize — the reducer:** `acc = current` → `acc = merge(acc, current)` for typed
  CRDTs (flag/counter/set/text). "Keep last" is the special case `merge = causally-greatest-
  then-tiebreak`.

Payoffs: in the linear common case the frontier is size 1 → bit-for-bit today's "keep last,"
zero overhead; extra work fires only on cells *actually* written concurrently. Merge is
**read-time, lazy, per-node** → no mandatory merge-compaction for correctness (compaction
becomes a perf/GC optimization, like LSM). Caveats: mergeable types can't short-circuit at the
newest layer; git-style 3-way text needs the base layer identified (CRDT reducers don't).

---

## 9. Backups under eventual consistency

Under EC there is no global "now," so **don't back up state — back up the DAG.** This is git's
backup model, and *cleaner* than Cassandra's because immutability means conflict losers stay
recoverable.

**Definition:** a backup is a **causally-closed set of immutable parts + a frontier (the
version-vector / set of writer tips naming the cut).** Restore = install parts, adopt frontier;
the read-fold reconstructs state. Two consequences:

- **Incremental & dedup'd for free** (content-addressed; `git push`/restic model).
- **Merge-policy-independent:** back up *raw parts* (the fold's inputs), never resolved state.
  Two replicas that observed the same cut produce byte-identical backups; changing a merge
  policy later just re-folds — no backup migration.

Separate the two things "backup" bundles:

- **Durability — continuous, coordination-free:** stream each new part to a shared
  content-addressed bucket; the union of causally-closed cuts is causally closed, so the
  bucket *is* the global backup. **It is the union of writers' parts, deduplicated into one
  store — not N copies.** Gossip already replicates each part, so writer stores back each
  other up; the bucket is one more durable cold replica.
- **A restore point — just a recorded frontier VV:**

  ```
  backup-log:
    2026-06-23T18:00  stable  {w1:5, w2:3, w3:7}
    2026-06-24T18:00  stable  {w1:9, w2:6, w3:12}
  ```

Three grades of consistent point (a valid snapshot is a **causal cut**, not a wall-clock
instant — Chandy-Lamport / Mattern consistent cuts):

1. **Any replica's live cut** — always available, no coordination, internally consistent.
2. **The stable frontier** — componentwise *min* across replicas (the low-water mark of
   fully-propagated history); globally agreed, restorable anywhere, lags real-time. **Same
   watermark as tombstone GC** — get it for free. (During a partition it freezes — that *is*
   the last globally-consistent point.)
3. **Any historical cut** via a frontier **reflog** → PITR. The frontier VV names the point;
   restore = make visible parts with `(writer, seq) ≤ VV` and fold.

**The rollback trap:** restoring an old backup into a live gossiping system does **not** roll
back — immutable parts re-merge and newer parts still dominate; deleting a bad part lets gossip
*resurrect* it. Real rollback = commit a **propagating "quarantine these hashes" fact**
(monotonic), never an erasure. Precedents: Git (objects+refs+reflog), Dolt (`dolt backup`),
Datomic (`as-of` the log).

---

## 10. Cluster management / control plane

### 10.1 Two layers (keep them separate)

- **Orchestration / lifecycle** — spawn, configure, restart, scale, upgrade. The "tool that
  spawns a cluster." Infra-level.
- **In-database coordination** — membership, failure detection, authoritative metadata. Must
  match the AP data plane.

### 10.2 Landscape

| Camp | Systems | Mechanism |
|---|---|---|
| **CP / Raft-clustered** | **Neo4j** (Causal/Autonomous Clustering: Raft cores + async read replicas) | leader takes writes; route writes to leader |
| | **Dgraph** | Raft per shard-group + a Raft **Zero** control group (membership/assignment/rebalance) |
| | **ArangoDB** | **Agency** = embedded etcd-like Raft KV; stateless Coordinators + DBServers |
| | **TigerGraph** | ZooKeeper-coordinated; Kafka as the durable update log |
| **AP / gossip-clustered** | **Cassandra/Scylla** | masterless gossip + phi-accrual; consistent hashing + vnodes; *recently added Raft metadata* (TCM / Scylla Raft) |
| | **Riak** | Dynamo ring (gossip + consistent hashing) |
| **Building blocks** | SWIM (memberlist/Serf, Lifeguard); etcd/ZooKeeper/Consul | membership / consensus substrates |
| **Orchestration** | K8s Operators (cass-operator, kube-arangodb, cockroach); Helm; Compose; bespoke CLIs (`cockroach start --join`, `redis-cli --cluster create`) | process lifecycle |

The telling signal: even the canonical gossip systems (Cassandra TCM, Scylla, Kafka→KRaft)
added a **small consensus layer for metadata** because pure-gossip schema/topology had races.

### 10.3 Chosen control plane: gossip + a tiny CP metadata sliver

- **Membership gossip (SWIM, AP):** liveness, failure suspicion, member list, each node's
  frontier VV, load hints. Runs node-to-node on its **own transport** (typically UDP probes +
  TCP state sync), *not* over the HTTP API. "Shares the anti-entropy substrate" means it
  reuses the same gossip *cadence and peer-selection* as the data anti-entropy and piggybacks
  membership deltas on those messages — it does **not** ride the `/cluster/*` endpoints.
- **Tiny CP store (embedded Raft or etcd) for what must agree:**
  - **The writer roster** — this *is* the version-vector dimension set; adding/retiring a
    writer changes the VV space and needs a uniquely-allocated ID. Prime CP datum.
  - **Schema + per-type merge-policy registry** — a wrong policy silently breaks convergence.
  - **The METIS partition map** (once partitioning lands) + rebalancing decisions.

**Three planes, three transports — don't conflate them:**

| Plane | Who talks | Transport | Cadence | Role |
|---|---|---|---|---|
| **SWIM membership** | node ↔ node | own listener: UDP probes + TCP state sync | continuous, tiny | source of truth for liveness / membership |
| **Data anti-entropy** | node ↔ node | TCP (bulk part pulls) | periodic | exchange part-hash digests, pull missing parts |
| **`/cluster/*` HTTP** | operator / CLI ↔ a node | the existing 6666 REST listener | on-demand, request/response | *observe and command* the cluster |

The HTTP `/cluster/*` endpoints sit **on top of** SWIM: `status`/`members` *read* the
membership table SWIM maintains; `join`/`drain` are *admin triggers* (hand a node its seeds and
kick off its SWIM join — the join gossip then flows over SWIM's own transport). They are
neither the membership protocol nor the data gossip.

Two things our architecture makes *easy*: **bootstrap** (a new node learns members via gossip,
then catch-up = "pull missing content-addressed hashes" from peers / the backup bucket — no
leader-streamed snapshot) and **routing** (multi-writer → every node is a valid write target →
routing is locality/load, never correctness, unlike Neo4j's leader routing).

### 10.4 Placement: two distinct concerns

- **Durability / replica layer = computed hash-placement + vnodes.** A part's content hash is
  its ring position; replicas = next `R` distinct machines clockwise; computed locally from
  `(hash, gossiped membership)` — no central map (Dynamo's preference list, applied to
  immutable parts). Scatter is harmless here (you only need bytes safe on `R` machines and
  findable). Because parts are immutable, this reduces to a **content-addressed distributed
  blob store**: a replica has a hash or not, byte-identical, no per-key conflict, no quorum.
- **METIS structural partitioning = agreed metadata.** A *global graph-cut optimization*; it
  **cannot** be computed from a hash (scatter would maximize edge-cut). Lives in the CP store.
  May coexist with hash-placement: METIS decides *what is grouped*, hash-placement can still
  pick *which machines* hold the replicas of those groups.

### 10.5 Starting tool (phased)

The `/cluster/*` HTTP endpoints are the **operator/CLI control surface** (on the 6666
listener) — a *stable façade* over a membership backend that evolves underneath. They are
distinct from the SWIM transport (§10.3); building them first does **not** mean HTTP is doing
membership.

`turingctl` is a **local control tool**: it talks only to the **co-located** turingdb server on
that host (e.g. `POST localhost:6666/cluster/join`). It does **not** reach into other nodes — it
commands the *local* node, which then performs the actual join (SWIM gossip with the seeds,
CP-metadata read, writer-ID allocation, anti-entropy catch-up). This mirrors `cockroach` /
`nodetool` / `etcdctl`, where the CLI drives the node it sits next to.

1. Add `/cluster/{join,members,status}` endpoints to the existing HTTP server
   (`server/DBServerProcessor`, `server/Endpoints.h`) — they **read out** the membership view
   + frontier VVs and **trigger** lifecycle actions. On the existing 6666 listener; no new
   transport *for the API*.
2. Thin `turingctl` CLI driving them (`cluster init`, `node add --join`, `cluster status`,
   `node drain`). Test with Docker Compose / multi-process local. *MVP membership backend:*
   static config (configured peers + naive reachability), so `/cluster/status` returns
   something real before SWIM exists.
3. **SWIM/memberlist-style gossip membership** on its **own transport** (UDP probes + TCP
   state sync), sharing the data anti-entropy *cadence* (not the HTTP API). It replaces the
   static-config backend behind the *same* `/cluster/*` endpoints — the API is unchanged.
4. **Static config first** for CP metadata (writer roster + schema + policies in a file);
   add the embedded Raft/etcd store only when dynamic membership / online schema change /
   partitioning forces it.
5. Defer the K8s operator until the protocol is proven (the operator just encodes
   `turingctl`'s operations).

#### The `/cluster/join` handshake

`turingctl node add --join seedA,seedB` POSTs to the **local** server's `/cluster/join`. That
server runs the join and returns a small **bootstrap bundle** (control plane) — the DataParts
are **not** in the response; they stream in afterward via anti-entropy (data plane).

The bundle (assembled from the seed's SWIM state-sync + a read of the CP metadata store):

- **Membership view** — full member list (IDs, addresses, liveness, each node's frontier VV).
- **CP metadata snapshot** — writer roster, schema version, per-type merge-policy registry,
  partition map (null pre-METIS). The node must agree on these before it can fold correctly.
- **A freshly-allocated `writerId`** — *only if joining as a writer.* This is **the single
  consensus round** in the whole join (it allocates a new VV dimension); read-only replicas
  skip it.
- **A target frontier VV** — "current head," so the node knows what to catch up to.

```json
POST localhost:6666/cluster/join   { "seeds": ["seedA:7900"], "role": "writer" }
→ {
    "nodeId": "…", "writerId": "w4",                    // w4 allocated via the CP store
    "members":  [ { "id": "…", "addr": "…", "status": "alive", "frontierVV": {…} }, … ],
    "metadata": { "schemaVersion": 37,
                  "writerRoster": ["w1","w2","w3","w4"],
                  "mergePolicies": { … }, "partitionMap": null },
    "targetFrontier": { "w1":9, "w2":6, "w3":12, "w4":0 },
    "catchup": { "mode": "anti-entropy", "sources": ["seedA", "s3://bucket"] }
  }
```

After the handshake the node pulls missing parts by hash over anti-entropy (whole graph
pre-METIS; only its vnode arcs once placement is active). It can **accept writes immediately**
(AP — local + gossip-propagated, validated against its last-known metadata); it serves **reads**
only once caught up to a consistent cut, then announces ready.

Phase 1 stays replicated-everywhere (new node pulls the whole graph), so rebalancing is a
non-issue until METIS arrives.

---

## 11. Building-blocks glossary

- **ZooKeeper** — strongly-consistent (ZAB) coordination service; tiny hierarchical KV
  (znodes) with ephemeral nodes (presence) + watches (events). For *metadata only*, CP.
  For us: etcd or embedded-Raft is the more modern equivalent; never on the data plane.
- **Kafka-as-a-log** — durable ordered WAL/ingestion bus. Mostly *redundant* for us: each
  writer's part-chain is already a replayable log; the bucket is its archive.
- **Phi-accrual failure detector** — outputs a continuous suspicion value φ from the recent
  heartbeat-interval distribution (not a fixed timeout); auto-adapts to jitter. Our forgiving
  failure model (false positive just re-routes reads; no election) lets us run it relaxed.
- **Consistent hashing** — keys & nodes on a ring; ownership = pure function of
  `(key, membership)`; join/leave moves only `O(K/N)` keys. Eliminates the consensus-maintained
  partition map — but it's *hash scatter* (wrong for METIS locality).
- **vnodes** — many tokens per physical node → even load, parallel rebalancing, weightable
  capacity. Same appeal & same scatter caveat as consistent hashing.
- **Seed nodes** — a few well-known stable addresses (or a DNS name) a joiner contacts to enter
  the gossip; entry points, not masters. After joining, peer-to-peer.
- **SWIM** — scalable masterless membership + failure detection: random **indirect probing**
  (`ping` → `ping-req` via `k` peers, cuts false positives) + **infection-style dissemination**
  (membership deltas piggybacked on probes) + **suspect→dead** with **incarnation numbers**
  (the membership state is itself a CRDT). Implementations: memberlist/Serf, Lifeguard. For us:
  it *is* the membership layer, sharing the anti-entropy gossip cadence.

---

## 12. Open decisions to pin (the real forks)

> The core architecture is **decided** (§2.3: uniform gossip CRDT + version vectors; consensus
> only for metadata/schema). The CP/AP fork is **closed** in favor of AP. What remains is
> tactical:

1. **The conflict/merge semantics spec per property type** — what it *means* when two writers
   concurrently change the same cell. This is a product/spec decision, not implementation.
   (A good candidate for a Keel-style machine-checked charter.)
2. **HLC anywhere, or never?** Current lean: never; Lamport only on the rare recency-sensitive
   field.
3. **The causal-stability signal for GC** (tombstones, old assertions) — the one piece of
   coordination we can't dodge. How is the stable frontier computed/gossiped?
4. **Replication factor `R`** and rack/region diversity rules for the durability layer.
5. **Authority for the METIS partition map** and the rebalancing protocol.
6. **etcd vs embedded Raft** for the CP metadata sliver.

---

## 13. Precedents / references

- **CALM theorem** — Hellerstein & Alvaro.
- **CRDTs** — Shapiro, Preguiça, Baquero, Zawirski (2011); **Dotted Version Vectors** —
  Preguiça/Baquero et al.
- **Dynamo** (SOSP 2007); **Bayou** (SOSP 1995); **CouchDB**; **Riak**.
- **Antidote / Cure** (ICDCS 2016) — geo-replicated CRDT DB, transactional causal consistency.
- **Dolt** (git-for-data); **Datomic** (immutable log + `as-of`); **TerminusDB**.
- **Spanner/TrueTime** (OSDI 2012); **CockroachDB**; **Calvin** (SIGMOD 2012).
- **Sequence CRDTs**: RGA (2011), Yjs/YATA, Automerge (Kleppmann 2017), Fugue (2023),
  Peritext (2022), Eg-walker (2024).
- **Consistent snapshots**: Chandy-Lamport; Mattern's consistent cuts.
- **SWIM** (Das/Gupta/Motivala 2002) + **Lifeguard** (HashiCorp).
- **Cluster control planes**: Neo4j Causal/Autonomous Clustering, Dgraph Zero,
  ArangoDB Agency, Cassandra TCM, Kafka KRaft.
