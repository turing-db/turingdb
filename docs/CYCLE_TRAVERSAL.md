# Cutting Cycle Traversal in a Columnar Nested-Loop Graph Engine — a Literature Review

**Audience:** TuringDB engine developers designing the operator set of the MLIR/`query/ir` nested-loop executor.
**Question:** on cyclic fraud graphs (money-transfer rings, dense hubs) TuringDB's pure-WALK traversal explodes as hop count grows. What are the state-of-the-art options — semantics, algorithms, data structures — to cut cycle traversal, at high performance, that fit a chunk-at-a-time nested-loop model?

> **Provenance & verification depth.** Sections 1–6 are backed by an adversarially-verified web literature sweep (106 agents, 24 primary sources, 130 extracted claims). Every load-bearing claim was extracted from a primary source with a verbatim supporting quote. The **heaviest adversarial verification (3-vote refutation) concentrated on the standards/complexity axis** — §1 (path semantics) and §2 (complexity) — where 19 of 20 checked claims survived 3-0 (the one correction is noted in §1). The **systems, fraud-enumeration, and optimizer claims (§3–§5) rest on single-primary-source extraction with lighter cross-verification** — they are quoted accurately from the papers but were not each put through the full refutation panel, and several come from a single source apiece; treat their *engineering* conclusions as well-sourced-but-not-triangulated. A known bias: the most-verified sources cluster around one author group (Martens, Vrgoč, Libkin, Bonifati, Francis) and the ISO GQL committee, so the review is strongest on *what semantics to adopt* and lighter on *how to engineer them fast*. Citation keys resolve in **References**. Section 7 (construction-time indexing — the loop-head / SCC angle) rests on canonical CS results stated from domain knowledge; the dedicated verification agent hit a session limit before re-confirming them against primary sources, so treat §7 citations as "standard, not re-fetched this session" and confirm venues against DBLP before quoting. Section 8 (engine mapping) is my own engineering analysis grounded in the repo (`query/ir`, `query/pipeline/processors/PathExplorerProcessor.*`, `storage/`) and the cited literature — note that the verified algorithmic evidence is BFS/frontier product-graph (PathFinder/MillenniumDB), so whether product-graph/PMR enumeration composes cleanly with TuringDB's indices-lineage nested-loop chunks is an open engineering question no source settles directly; the product-state dedup idea (§3) is the piece that transfers most cleanly.

---

## 0. TuringDB's current state (repo-grounded)

- **The NL engine (`query/ir`) is pure WALK everywhere.** Fixed-length chains (`MATCH (a)-->(b)-->(c)`) lower to statically nested `nl.for` loops. Each hop op (`nl.get_out_edges` and siblings) fills chunks `(indices, edgeIDs, edgeTypeIDs, tgtIDs)`; `runEdgeLoopSteps` (`NLExecutor.cpp:762`) gathers the input node column and every *carried* column through the `indices` lineage vector, so a surviving row at hop *k* carries its whole path prefix as parallel ID columns. **No uniqueness check of any kind runs** — not even Cypher's per-`MATCH` "no repeated relationship" rule. On a cyclic graph every hop is a pure fan-out multiplier; *k*-hop cost grows ~degreeᵏ.
- **A v1 variable-length processor already implements TRAIL** for `*min..max` (`PathExplorerProcessor.cpp`). It is BFS-layered over a persistent parent-pointer tree (`FrontierEntry{node, edge, parentIdx, sourceIdx}`), enforces per-path **edge** uniqueness with an O(depth) parent-chain walk (`edgeUsedInPath`, line 139), and materializes paths only at output (`reconstructPath`). It carries an explicit **unbounded-memory** warning: a depth window can exceed chunk size (line 209).
- **Storage is friendly to the fixes below.** `NodeID = ID<uint64_t,1>` (`storage/ID.h:108`), dense from 0 per graph; adjacency is `std::span<const EdgeRecord>` per DataPart; DataParts are **immutable and sealed at commit**. Dense per-node arrays (visited bitmaps, depth stamps, distance labels, SCC IDs) are therefore cheap and natural, and commit is a well-defined index-build point.

The rest of this document is what the field knows about doing better.

---

## 1. Path-semantics landscape — WALK / TRAIL / SIMPLE / ACYCLIC and the finiteness rule

**The base semantics of a "path" in GQL/SQL-PGQ is WALK** (repeated nodes *and* edges allowed); the standards committee explicitly uses "path" for what graph theory calls a *walk* [GPML22]. On a cyclic graph an unrestricted `Transfer*` pattern has **infinitely many matches** — the committee's own motivating example pumps a transfer loop through a bank-transfer graph [GPML22]. This is exactly TuringDB's failure mode, named by the people who wrote the standard.

**The standards make finite results mandatory by construction.** GQL/SQL-PGQ define **27 usable path modes** from 6 selectors (`ANY`, `ANY SHORTEST`, `ALL SHORTEST`, `ANY k`, `SHORTEST k`, `SHORTEST k GROUPS`) × 4 restrictors (`WALK`, `TRAIL`, `SIMPLE`, `ACYCLIC`), *plus* the 4 bare restrictors — **minus bare `WALK`, which is prohibited** because it is the one combination that can return infinitely many paths on cycles [Martens23-PMR, RD23-Digest]. The rule that guarantees termination:

> **Every unbounded quantifier (e.g. `*`) must be in the scope of a restrictor *or* a selector (or both).** [GPML22]

The restrictors are post-filters on the walk semantics of a pattern [RD23-Digest]:
- **TRAIL** — no repeated *edge*. Finite because a graph has finitely many edges [Martens23-PMR].
- **ACYCLIC** — no repeated *node*.
- **SIMPLE** — no repeated node *except* first = last (i.e. simple cycles are allowed) [RD23-Digest].

Selectors bound the result a different way: they partition matches by endpoint pair and keep a finite set per pair. `ANY` returns *one arbitrary* path per (source, target) pair (need not be deterministic); `SHORTEST` keeps minimum-length paths per pair [RD23-Digest]. This per-pair-representative definition is the formal license to **collapse enumeration into a BFS/reachability computation** for `ANY`/`ANY SHORTEST` (see §3, §5, §8-C).

> **Verified correction (adversarial pass).** An earlier draft claim said unbounded Kleene is legal *only* under a restrictor (`SHORTEST`/`TRAIL`/`ACYCLIC`). That is wrong: a bare **selector** such as `ANY` also makes the result finite (`ANY π{1,}` is well-formed — it selects one path per endpoint pair even when the underlying set is infinite). The correct rule is restrictor **or** selector [GPML22, RD23-Digest, Fig. 2].

**Semantics vs. cap are two orthogonal knobs — don't conflate them.** The *path mode* (WALK/TRAIL/SIMPLE/ACYCLIC) decides **which paths are valid answers** (a correctness question); a *hop cap* (`*1..k`) decides **how far the traversal may go** (a termination/cost question). They combine independently, and only one combination is pathological:

| | Terminates on cycles? | Explodes? |
|---|---|---|
| **WALK, no cap** ← TuringDB today | **No** (infinite) | yes |
| WALK + cap (Kuzu: default 30) | yes | yes, up to ~degreeᵏ |
| TRAIL, no cap | yes | bounded by #edges |
| **TRAIL + cap** | yes | bounded *and* small |

Note the two are *not* substitutes: a cap alone only **bounds** the blow-up — WALK + cap `k` still enumerates every loop-around up to length `k` (~degreeᵏ rows) — whereas TRAIL/SIMPLE is what removes the **redundancy**. The cheap move is to leave the top row: TRAIL default + a defaulted cap.

**A subtle scoping trap if you port Cypher to GQL:** GQL `TRAIL` is scoped *per path pattern*; Cypher's default edge-uniqueness is scoped *per `MATCH`* across all patterns, which corresponds to GQL's separate `DIFFERENT EDGES` match mode, **not** `TRAIL`. `MATCH TRAIL ()-[e1]->(), TRAIL ()-[e2]->()` can bind `e1 = e2`; the Cypher form cannot [RD23-Digest]. Decide the scope deliberately.

### What production engines actually chose (and whether hop bounds are mandatory)

| Engine | Default path semantics | Unbounded hop allowed? | Notes / source |
|---|---|---|---|
| **Neo4j** | **ALL TRAIL** (edge-uniqueness per MATCH) | Yes, but docs warn of exponential blow-up and recommend a finite cap e.g. `{,10}` | Legacy `-[*1..3]->` retained but flagged **not GQL-conformant**; mitigation is inline predicate pushdown into quantified patterns [Neo4j-VLP, PathFinder] |
| **NebulaGraph** | ALL TRAIL | — | [PathFinder] |
| **Kuzu** | **WALK** (deliberate divergence from Neo4j) | **Effectively no** — default upper bound **30 hops** silently applied to guarantee termination | `is_trail`/`is_acyclic` are *post-hoc filters*; recommends `SHORTEST`/`ALL SHORTEST` "if paths are not needed" [Kuzu-Diff] |
| **Memgraph** | Algorithm-selected in syntax (`*BFS`, `*WSHORTEST`, `*ALLSHORTEST`, DFS) | Yes (`-[*]->` legal); bounding is a user optimization, not required | Inline filtering during expansion [Memgraph] |
| **TigerGraph (GSQL)** | **ALL SHORTEST** by default | `+` needs no bound (shortest is inherently finite) | [PathFinder] |
| **Oracle PGQL** | `ANY`/`ANY SHORTEST`/`ALL SHORTEST`/`TOP k SHORTEST`/`(TOP k) CHEAPEST` | `ALL` mode **requires** an upper bound (`{1,4}` not `+`) | [PathFinder] |
| **DuckPGQ** | `ANY SHORTEST` only (initially) | Refuses `ALL` Kleene* — "typically exponential in a large component" | Future `ALL` only under TRAIL/SIMPLE/ACYCLIC with *strongly bounded* quantifiers compiled to unions/joins/filters [DuckPGQ] |
| **PathFinder** (research, on MillenniumDB) | All **27** GQL modes | per mode | first engine to implement all 27 [PathFinder] |
| **TuringDB (today)** | **WALK** | **Yes, and no cap** ⟵ the outlier | this repo |

**Takeaways for the operator set:** (1) TuringDB is the only engine in this table with WALK-by-default *and* no hop cap — that combination is precisely what nobody ships. (2) A **mandatory or defaulted hop upper bound** is the near-universal safety valve (Kuzu 30, Oracle `ALL`, GQL rule). (3) Two credible defaults exist: **ALL TRAIL** (Cypher/Neo4j parity — and TuringDB's v1 var-length processor already does this) or **WALK + mandatory cap** (Kuzu). Pick TRAIL for Cypher compatibility; keep WALK reachable by explicit opt-in.

---

## 2. Complexity foundations — why WALK is the theoretical default and SIMPLE is not

This is the "why" behind every engine's choice above.

- **Regular *simple* path queries (RSPQ) are NP-complete in general** [MW95]. The regular *walk* RPQ is polynomial in *combined* complexity, whereas RSPQ is **NP-complete even for fixed, trivial languages** like `(aa)*` (even-length paths) or `a*ba*` [BBG13]. "From a theoretical viewpoint the former [walk] has overridden the latter [simple], mainly for complexity reasons" [BBG13]. That single sentence explains the whole landscape in §1.
- **Trichotomy** [BBG13]: for every fixed regular language *L*, RSPQ evaluation is exactly one of **AC⁰ / NL-complete / NP-complete** in data complexity. There is a maximal tractable class **trC** (all first-order/aperiodic), polynomial inside, NP-complete for everything outside — and it is *also* the maximal class for which shortest-simple-path is tractable. In principle a planner could classify each pattern's language and pick a tractable simple-path algorithm exactly when one exists; the cost of *deciding* trC membership is NL-complete (DFA) / PSPACE-complete (NFA or regex) [BBG13].
- **Enumeration delay** [MT18]: walk and shortest-path results enumerate with **polynomial delay**; simple-path enumeration is "much more intricate." Two disjoint paths in a digraph is W[1]-hard parameterized by one path's length — so bounded-hop simple-path is *not* FPT in general [MT18].
- **The hop-bound escape hatch.** Regular simple path finding is **FPT in the path length** [BBG13]. A mandatory `*..k` upper bound therefore confines the exponential to *k*, not to graph size — the theoretical reason small hop caps (fraud queries use *k*≈3–6) keep even simple/trail semantics feasible. And empirically, simple-path semantics "is feasible for the **vast majority** of RPQs used in practice" despite worst-case hardness [MT18].
- **Path-materialization blow-up is unavoidable under *every* mode.** There is a family with 3n+1 nodes / 4n edges where two nodes have **2ⁿ shortest paths, all simultaneously trails and simple paths** [Martens23-PMR]. So any engine that materializes paths as a flat table faces worst-case exponential intermediate results even under SHORTEST TRAIL. This motivates *compact* path representations (§3, PMR) and *not returning paths at all* when the query doesn't need them (§5).
- Counting/enumeration lineage (foundational): Read & Tarjan (1975) and **Johnson (1975)** for elementary-circuit enumeration; Birmelé et al. **SODA 2013** for optimal listing of cycles and st-paths with per-path cost proportional to path length [Birmele13]. Johnson's algorithm processes **one SCC at a time**, anchored at the least vertex — the hook into §7.

**Design consequence:** default to WALK/SHORTEST semantics in the *hot* operator; treat TRAIL/SIMPLE as either (a) a cheap incremental filter under a small hop bound, or (b) a separate, DFS-friendly enumeration path. Do not build the core hot loop around exact simple-path semantics on unbounded patterns.

---

## 3. Systems techniques for variable-length expansion in vectorized engines

The common core across Neo4j, Kuzu, Memgraph, DuckPGQ is **frontier-based BFS**, a.k.a. *Iterative Frontier Extensions* (IFE) [Kuzu-RecJoin25]. The engineering is all in the frontier representation, the dedup structure, and how (or whether) paths are materialized.

**Kuzu recursive-join operator** [Kuzu-RecJoin25, Kuzu-CIDR23]:
- **Dense frontier** = one boolean per vertex in the whole graph; switches to a **Ligra-style sparse** frontier when the next frontier is < 1/8 of all nodes. Columnar CSR adjacency, morsel-driven parallelism (Leis et al. 2014).
- **Shortest-path cycle cutting = a single global dense visited array** (one boolean/vertex): each node can be active in at most one frontier, so re-expansion is suppressed with an O(1) branch-light test — this is the reachability collapse (§5).
- **Path materialization = a parent-pointer structure**: a dense pre-allocated array of 8-byte pointers/vertex shared across threads, CAS-updated, **24 bytes per stored path edge** (nodeID, edgeID, next-parent pointer). This is precisely TuringDB's `FrontierEntry` parent tree, but SoA/columnar and lock-minimal.
- **Parallelization is a morsel-dispatch design space**: source-morsels (1T1S, Neo4j v5.16 style), frontier-morsels (nT1S, Ligra style), hybrid **nTkS** — the recommended robust default (11.5–15.5× at 32 threads on 64-source shortest-path; Neo4j showed ~1.0× i.e. no parallel speedup on the same workloads).
- **Crucially: this SOTA operator is WALK-only.** Trail and acyclic semantics under parallel execution are explicitly deferred to future work [Kuzu-RecJoin25]. Even the state of the art hasn't solved fast parallel per-path uniqueness — a realistic scoping signal for TuringDB.

**DuckPGQ multi-source BFS** [DuckPGQ, MSBFS]:
- Implements `ANY SHORTEST` with **MS-BFS** (and Bellman-Ford for weighted), *not* recursive-CTE translation, because a PGQ Kleene* binds endpoints to *sets* (up to all-pairs) — bulk multi-source is the right primitive for a vectorized engine.
- **Bit-parallel visited/frontier:** `seen`/`visit`/`next` hold one **512-bit bitset per vertex** (one bit per concurrent search); OR/AND/NOT/zero-test via **AVX-512**, so 512 searches share each instruction and each CSR access. DuckDB feeds it batches of 1024 (src,dst) pairs.
- **Refuses unbounded `ALL`** — "typically exponential in a large connected component."

**MS-BFS "The More the Merrier"** [MSBFS] — the primitive DuckPGQ adopted:
- Runs many independent BFSs concurrently on one core, per-vertex BFS membership as bit fields in wide registers, **no locks/atomics**. Set-union = `OR`, set-difference = `AND-NOT` — this *is* the per-source visited-bitmap dedup, branch-light.
- Memory is linear and predictable: `P × 3 × N × ω` bits (e.g. 11.4 GB for 32,768 concurrent BFSs on 1M vertices).
- **Pays off exactly on short-diameter, hub-dense graphs** (fraud/money-transfer): >70% of vertex explorations in BFS levels 3–4 are shareable across ≥100 concurrent BFSs on the LDBC social graph; 12.1–88.5× over textbook and direction-optimized BFS. Little benefit on long-diameter graphs, and its footprint is prohibitive **when returning paths** (536 B/node upfront; 128 GB for 2 multi-source morsels on 120M-node Graph500-28) [Kuzu-RecJoin25].

**MillenniumDB / PathFinder — product-graph BFS with state-level dedup** [MDB-ASP, PathFinder, Martens23-PMR]:
- Runs BFS/DFS over the **product graph** (data graph × NFA states); the visited set is keyed on **(data-node, automaton-state)** pairs. **A data node may be revisited; a product node may not.** Cycle control lives at the product-graph level → `ANY`/`ANY SHORTEST WALK` in **O(|A|·|G|)** with output-linear delay.
- `ALL SHORTEST` stores per-state **prevLists** (all shortest-path predecessors) forming a **DAG** — a generalization of a single parent-pointer tree to a predecessor-list DAG — from which all shortest paths enumerate by DFS. Total time **O(|q|·(|V|+|E|) + |O|)**, memory bounded by the product graph, *not* by the number of paths.
- **Path Multiset Representations (PMR)** [Martens23-PMR]: represent the (possibly infinite) walk result as a graph *R* + homomorphism to *G* + start/target sets. Computable in **linear combined complexity O(|q||G|)** for an unambiguous automaton — vs exponential tabular enumeration. `SHORTEST` applies to a PMR in O(k|R|); but computing a PMR under SIMPLE/TRAIL in polytime would imply P=NP.
- **Empirics** (WDBench Wikidata, 364M nodes / 1.26B edges): PMR answered 313 of 315 path queries sub-second where Neo4j timed out on all; up to **4 orders of magnitude** speedup from representing paths instead of enumerating them. PathFinder ALL TRAIL had 11–13 timeouts vs Neo4j's 134; despite TRAIL's intractability, brute-force pruned enumeration ran close to WALK on real data, and **DFS is the strategy of choice when trails are numerous**.

**TRAIL/SIMPLE/ACYCLIC evaluation** in PathFinder [PathFinder]: because even *checking one* conforming simple/trail path is NP-complete, it does pruned exhaustive enumeration over the product graph with an `ISVALID` check per one-edge extension — and, importantly, **the restrictor is checked against the path in the original graph, not the product graph.** Worst case O((|A|·|G|)^|G|), stated as the best known.

### Dedup / visited-set data structures — the menu

| Structure | Semantics it delivers | Cost per extension | Memory | Vectorizes? | Used by |
|---|---|---|---|---|---|
| **Global dense visited bitmap** (1 bit/node) | node-reachability (each node once) — shortest/`ANY` | O(1) test+set | O(V) bits | yes | Kuzu shortest [Kuzu-RecJoin25] |
| **Per-source/lane bitmaps** (ω bits/node) | many concurrent BFS, node-visited | O(1) bitwise, ω sources/instr | P·3·N·ω bits | **AVX-512 / registers** | MS-BFS, DuckPGQ [MSBFS, DuckPGQ] |
| **(node, automaton-state) visited set** | RPQ WALK, product-graph cycle control | O(1) hash/array | O(|A|·V) | partially | MillenniumDB/PathFinder [MDB-ASP] |
| **Parent-pointer tree / prevList DAG** | path *materialization* (trail via ancestor walk) | O(depth) walk to check/emit | O(#frontier entries) or O(paths) | poorly (pointer chase) | TuringDB v1, Kuzu (24 B/edge) [Kuzu-RecJoin25] |
| **Per-vertex barrier / distance label** | hop-bounded simple-path pruning | O(1) compare after BFS prepass | O(V) | yes (gather+compare) | BC-DFS, PathEnum (§4) |
| **Per-path 64-bit fingerprint** (Bloom of edges/nodes) | *approximate-negative* uniqueness fast path | O(1) common, exact walk on hit | 8 B/frontier entry | yes | *(engineering; no single canonical cite)* |

The literature strongly favors **dense bitmaps for node-reachability** and **distance/barrier labels for hop-bounded pruning**; parent-pointer structures are for *materializing* paths, not for cheaply *deciding* uniqueness. TuringDB's v1 uses the parent-pointer walk for both, which is the expensive combination.

---

## 4. Hop-constrained path & cycle enumeration — the fraud workload

This is the literature written *for* TuringDB's use case, and it converges on a clear recipe.

**The workload, precisely** [PathEnum, Peng20, Qiu18]: hop-constrained **s-t simple path enumeration** (HcPE) — all vertex-distinct paths of length ≤ *k*. Cycle detection reduces to it: a new edge e(v,v′) closes a cycle iff a path exists v′→v of length k−1, i.e. query q(v′,v,k−1). Fraud semantics is **hop-bounded SIMPLE-cycle**: short cycles are the money-laundering-relevant ones (longer paths raise fraudster cost/risk), and small *k* is *mandatory* — Alibaba's finance risk-control uses **k=6**; unbounded enumeration "overwhelms the system with false alarms" [PathEnum, Peng20, Qiu18].

**PathEnum — per-query distance index + join** [PathEnum, SIGMOD 2021]:
- Build a lightweight index from **two BFSs** (distances from *s* and to *t*), O(|V|+|E|), O(1) lookups. Prune every vertex/edge that cannot lie on any ≤k path via `S(s,v) ≤ i ∧ S(v,t) ≤ k−i`.
- Two enumeration strategies chosen by a **cost-based optimizer**: index-guided DFS, and a **join-based** method that splits the query in the middle and joins (meet-in-the-middle).
- **1.9–240.7× query-time and 14.2–358.5× response-time** over the prior SOTA across 15 real graphs. Its argument: prior polynomial-*delay* algorithms pay so much per-step pruning that overhead offsets the search-space reduction — **one upfront index build beats per-step pruning.**

**Peng et al. BC-DFS — barrier pruning** [Peng20, PVLDB 2020]:
- Per-vertex **barrier** `u.bar` = lower bound on distance u→t avoiding the current DFS stack (`sd(u,t|S) ≥ u.bar`); block any branch whose remaining budget < barrier. Initialize barriers with exact distances from a hop-bounded BFS from *s* and a **reverse-graph** hop-bounded BFS from *t*.
- Polynomial delay **O(k·m)** per path (matches theoretical best) but up to **2 orders of magnitude** faster in practice than the optimal-delay T-DFS/T-DFS2.
- The **JOIN** variant cuts at the ⌈h/2⌉-th "middle vertex" and concatenates forward/reverse partial paths — O(k·m·α) — best for large *k*. **Warning for join-based engines:** combining *simple* sub-paths does **not** yield *simple* cycles; non-simple combinations can dominate (600× more non-simple than simple cycles on askubuntu at k=20) [Blanusa23].

**GraphS — real-time, incrementally-indexed cycle detection at Alibaba** [Qiu18, PVLDB 2018]:
- Production dynamic fraud graph: 519M vertices / 2.09B edges (~73 GB), >20K edge updates/s, reports all constraint-satisfying cycles with **99.9%-ile latency 20 ms**. Representative query: **6-hop cycles over a 48-hour sliding window** with attribute filters.
- **Hub handling is the tail-latency determinant:** at k=3, **93%** of enumerated paths pass through a vertex of out-degree ≥ 40; at k=5, **99%**.
- **HP-Index**: continuously maintain all ≤k paths between every pair of **hot points** (hubs) that avoid other hot points. Answer each new edge with forward DFS from source + reverse-graph DFS from destination, **suspending any branch that reaches a hot point or exceeds the budget**, then joining suspended branches through precomputed hub-paths. Index maintenance is a by-product of search → order-of-magnitude tail-latency win over plain visited-array DFS. (This is a *construction/maintenance-time* structural index — see §7.)

**Blanuša/Atasu/Ienne — fast parallel constrained cycle enumeration** [Blanusa23, ACM TOPC 2023]:
- **Fine-grained** (task-per-subtree, work-stealing) parallelization of Johnson and Read-Tarjan is provably scalable (fine-grained Read-Tarjan is work-efficient *and* strongly scalable); **coarse-grained** (one task per start vertex/edge) is provably *not* scalable on power-law graphs due to imbalance — directly relevant because fraud graphs are power-law.
- Parallelizes BC-DFS by giving each thread private path/barrier state, copied on steal with recursive unblocking so stolen barriers are reused. Up to **40×/61×** over coarse-grained on 1024 threads (2SCENT temporal / BC-DFS hop-constrained), on graphs to 10M/34M incl. an AML dataset. Open source: `github.com/IBM/parallel-cycle-enumeration`.

**GeaFlow — industrial evidence for the diagnosis** [GeaFlow, SIGMOD 2023]:
- A distributed streaming graph system at Ant Group for financial risk control. Its whole motivation: expressing multi-hop traversal as **cascades of relational joins causes intermediate-state space explosion**; graph-native traversal state is the fix. (Not a cycle-enumeration algorithm — the dedicated streaming-cycle system is GraphS.) Confirms, from a second production shop, that **join-cascade multi-hop blows up on fraud graphs** and graph-aware traversal state is the remedy.

**LDBC FinBench — what the benchmark actually specifies** [FinBench, PVLDB 2025]:
- Recursive queries carry **small explicit hop bounds and return DISTINCT endpoints/aggregates, not enumerated paths.** Flagship TCR1: `-[e1:transfer *1..3]->` … `RETURN DISTINCT other.id, length(p)-1`. 12 complex reads mostly ≥3 hops (1 s latency target); 6 simple reads ≤2 hops. **No WALK/TRAIL/SIMPLE mode is ever named** — cycle control comes from the hop bound + DISTINCT projection + monotonic-timestamp path filters.
- **Cycle detection is a bounded fixed-shape pattern**, not unbounded enumeration: TCR4 finds 3-account transfer cycles in a time window; **TRW1 inserts a transfer edge in a transaction and aborts if it closes a cycle** — real-time cycle detection *gates write commits*.
- **Hub truncation is the canonical countermeasure**: a specified edge sort order (e.g. descending timestamp) + a max edges traversed per vertex (e.g. 1000), passed as a query parameter at a given hop. FinBench states ISO GQL, Cypher, and SPARQL **lack native truncation** and cites a proposed GQL `TRUNCATING` construct.
- The generator reproduces real Ant Group graph pathology absent from social benchmarks: **unbounded hub degree** (up to 150M vs SNB's ~5000 cap) and pervasive **edge multiplicity** (many parallel transfer edges per account pair, itself power-law) — so parallel-edge dedup and hub handling are first-order, not polish.
- **Recursive path filters** (`isAsc`, `isDesc`, `head`, `last`, `minInList`, `maxInList`) need "regular expressions with memory." Pushing a derived filter *into* the expansion (maintain per-path state, e.g. max-timestamp-so-far, prune during traversal) instead of post-filtering cut TCR1/2/5 by **70.3% / 87.4% / 83.3%** [FinBench].

**The fraud recipe, distilled:** small mandatory hop bound + SIMPLE/TRAIL uniqueness + **bidirectional (meet-in-the-middle) search** + **distance/barrier pruning from a two-sided BFS** + **explicit hub handling** (suspend-at-hot-point, truncation, or degree caps) + **push per-hop predicates into expansion**. Return endpoints/aggregates, not paths, whenever the query allows.

---

## 5. Recursive-query / Datalog angle — when to legally collapse enumeration to reachability

The biggest performance win is often **not doing path enumeration at all.** Semi-naïve fixpoint with per-key dedup = reachability semantics; it terminates on cycles because the frontier stops growing once no new keys appear.

**When is the collapse legal?** When the path binding is not observable in the result — i.e. the path variable is dead and the query only needs (a) reachability/existence, (b) DISTINCT endpoints, or (c) aggregation over endpoints. GQL's `ANY`/`SHORTEST` selectors formalize exactly this "one representative per endpoint pair" license [RD23-Digest] (§1). FinBench's real fraud queries fall in this bucket — `RETURN DISTINCT other.id` [FinBench] (§4).

**Optimizer rules that do it in production:**
- **Neo4j pruning var-expand** [Neo4j-Prune]: when the *path* is unused and only distinct endpoint nodes matter, `VarLengthExpand(All)` + `Distinct` is rewritten to **`VarLengthExpand(Pruning)`** / a BFS-pruning cursor that never materializes each path and dedups reachable nodes during expansion — turning O(paths) into O(reachable nodes). This is the single most directly portable optimizer rule for TuringDB.
- **DuckDB `USING KEY` recursive CTEs** [DuckKEY] (CIDR 2023 "A Fix for the Fixation on Fixpoints" + SIGMOD 2025): replace append-only union accumulation with a **keyed** recurring table — a new row whose key (e.g. node pair) matches an existing entry **overwrites** it in place. Intermediate state is bounded by #distinct keys, not #walks. On a 424-node/1446-edge graph, shortest-path went from ~605M rows (near-OOM) to **19,213 rows — ~31,500× fewer.** It also removes vanilla-CTE "amnesia": the recursive step can read all accumulated keyed state, which is what makes visited-set / distance-table algorithms expressible in SQL. A relational-engine proof that keyed dedup collapses walk explosion on cyclic graphs — mirror it as a **keyed recursive operator**.
- **DuckPGQ** compiles `ANY SHORTEST` to MS-BFS rather than recursive SQL [DuckPGQ] (§3); recursive-CTE shortest path in **Umbra OOM'd** on the LDBC bulk workload where MS-BFS scaled to SF3000 [DuckPGQ]. Lesson: even a world-class recursive-CTE engine loses to a purpose-built multi-source BFS on this shape.
- **Efficient Path Query Processing in Relational DBs** [RelPathQ26] (arXiv 2026) — recent work on evaluating path queries inside relational engines; relevant to how SQL/PGQ path modes lower to joins/recursion. *(Fetched in sweep; details not individually claim-verified — read before citing.)*

**Design consequence:** a `RETURN`/analyzer rule that detects "path variable dead ∧ endpoints consumed by DISTINCT/aggregate/exists" and lowers `-[*..k]->` to a **frontier BFS with a global dense visited bitmap** (Kuzu-style, §3) instead of the enumerating loop. This is the highest-leverage change for the actual FinBench-shaped fraud queries.

---

## 6. Recent SOTA (2023–2026) worth tracking

- **Robust Recursive Query Parallelism in GDBMS** — Chakraborty & Salihoğlu, **PVLDB 18 (2025)** / arXiv:2508.19379 [Kuzu-RecJoin25]. The current reference design for a parallel frontier-based recursive operator in a columnar vectorized GDBMS: dense/sparse frontiers, parent-pointer path DAG, hybrid **nTkS** morsel dispatch, MS-BFS lane packing. **WALK only**; trail/acyclic deferred.
- **PathFinder / Evaluating RPQs in GQL and SQL/PGQ** — Farías, Martens, Rojas, Vrgoč, arXiv:2306.02194 [PathFinder]. First implementation of all 27 modes; product-graph BFS + lazily-built PMR with pipelined early output.
- **Representing Paths in Graph Database Pattern Matching (PMR)** — Martens et al., **PVLDB 16(7), 2023** [Martens23-PMR]. The compact-representation theory (linear-combined-complexity PMR; SIMPLE/TRAIL ⇒ P=NP hardness; 2ⁿ-shortest-paths blow-up).
- **RPQs under All-Shortest-Paths semantics** — MillenniumDB, arXiv:2204.11137 [MDB-ASP]. Output-linear-delay all-shortest with prevList DAG; pipelined "early output" fit for a streaming executor.
- **DuckDB USING KEY** — CIDR 2023 + **SIGMOD 2025** [DuckKEY]. Keyed recursive CTEs (§5).
- **LDBC FinBench** — Qi et al., **PVLDB 18, 2025** [FinBench]. The fraud/AML benchmark; hub truncation; recursive path filters (§4).
- **Fast Parallel Cycle Enumeration** — Blanuša/Atasu/Ienne, **ACM TOPC 10(3), 2023** [Blanusa23] (§4).
- **Factorized / list-based intermediate results** — Kuzu CIDR 2023 [Kuzu-CIDR23] + Gupta, Mhedhbi, Salihoğlu, PVLDB 14(11), 2021. Represent a many-to-many join's k² tuples as a compressed **f-representation** instead of materializing flat — the factorized alternative to TuringDB's flat carried-column lineage. *(Note: Kuzu CIDR 2023's "cyclic joins" means cyclic query **patterns** (triangles/cliques) via worst-case-optimal join, not data-path cycles — do not conflate.)*

---

## 7. Construction-time cycle detection & loop-head marking (the requested angle)

Everything above pays the cycle-control cost *per query*. The alternative is to **pay once at graph-build time** — precompute *where cycles can occur* and let queries skip or restrict work. This fits TuringDB unusually well: **DataParts are immutable and sealed at commit**, giving a natural index-build moment, and versioned snapshots get versioned structural indexes.

> Citations in this section are canonical CS results stated from domain knowledge (the dedicated verification agent hit a session limit before re-confirming them). Verify venues/years against DBLP before quoting.

**What to compute at commit (from a single Tarjan SCC pass, `O(V+E)` [Tarjan72]):**
- per-node **`sccID`** (dense u32/u64 column).
- per-node **`inCycle`** bit: |SCC|>1 or has a self-loop. A node with `inCycle=false` can never lie on a directed cycle.
- per-edge **`intraSCC`** bit: `src.sccID == tgt.sccID`. **Only intra-SCC edges can ever be part of a directed cycle** — every cross-SCC edge points "down" the condensation DAG.
- per-node **`isLoopHead`** bit: targets of DFS back edges. **Every directed cycle contains ≥1 back edge w.r.t. any DFS forest** (CLRS white-path corollary), so back-edge targets form a hitting set for all cycles. Loop-nesting-forest theory (Tarjan intervals; Havlak, TOPLAS 1997 [Havlak97]; Ramalingam, "Identifying loops in almost linear time," TOPLAS 1999 [Rama99]) gives near-linear identification. Minimum feedback vertex/arc set is NP-hard (Karp 1972 [Karp72]), so use the DFS back-edge set (a hitting set, not minimum) — cheap and sufficient.

**Query-time uses, in decreasing generality:**

1. **Acyclic fast path (the big one).** While every node on the current path prefix has `inCycle=false`, a uniqueness check *cannot* fail — so **skip TRAIL/ACYCLIC filtering entirely for those rows.** One vectorized bit-gather per hop splits the chunk into fast rows (no check) and slow rows (full check). On mostly-acyclic graphs this removes nearly all overhead; on fraud graphs it **confines the cost to the cyclic core**, which is what actually needs it.

2. **SCC-restricted ring search.** A directed cycle lies entirely within one SCC. For `(a)-[*..k]->(a)`, expand only `intraSCC` edges within `a.sccID` — sound and complete for cycle-back-to-source. Johnson's classic enumeration already anchors per-SCC at the least vertex [Johnson75]; this is the columnar version of the same idea.

3. **Condensation-DAG reachability pruning for s-t queries.** Label the SCC DAG at commit — GRAIL (Yıldırım/Chaoji/Zaki, PVLDB 2010) or 2-hop labels (Cohen/Halperin/Kaplan/Zwick, SODA 2002 / SICOMP 2003) — and prune any frontier row whose SCC cannot reach the target's SCC (at all, or within the remaining hop budget). Combine with two-sided distance labels (pruned landmark labeling, Akiba/Iwata/Yoshida, SIGMOD 2013) for the BC-DFS/PathEnum barrier (§4).

4. **Loop-head anchoring for enumeration.** Enumerate each ring once anchored at its head/least vertex per SCC (Johnson-style) instead of rediscovering every rotation from every seed row.

5. **(Non-standard semantics — flag clearly.)** "Each loop head at most once per path" guarantees termination with only a small head-set membership test per extension (since *G* minus the head set is acyclic, any head-free segment is a DAG path). The result set is a *superset* of simple paths and *subset* of walks — cheaper than exact trail checks but **not GQL/Cypher semantics**; would need explicit opt-in syntax. Offer only if a customer wants the speed/coverage tradeoff.

**Maintenance asymmetry — why the write-path cost is bearable:**
- **Edge inserts can only *merge* SCCs, never split** → union-find over SCC IDs + incremental cycle detection (Bender/Fineman/Gilbert/Tarjan, TALG 2016 [BFGT16]; Haeupler/Kavitha/Mathew/Sen/Tarjan, TALG 2012 [HKMST12]), or just recompute per commit given TuringDB's read-heavy analytical profile.
- **Edge deletes can *split* SCCs** → but a stale `inCycle=true` is **sound** (it only costs an unnecessary check); a stale `inCycle=false` would be **unsound**. So: exact update on insert, **lazy/deferred rebuild on delete.** This asymmetry is what makes an incrementally-maintained structural index practical, and GraphS's HP-Index [Qiu18] is living proof that maintaining such a structural index *on edge arrival* works at 20K updates/s (§4).

**The honest caveat — the giant SCC.** Financial-transaction and web/social graphs famously have one **giant SCC** (the web "bow-tie," Broder et al., WWW 2000 [Broder00]; FinBench's real graphs show giant connected components [FinBench]). If most of the graph is one SCC, `inCycle` is true almost everywhere and SCC-restriction prunes little — the acyclic fast path and DAG-reachability degrade to no-ops on the core. **Mitigations:** (a) compute SCCs on the *typed projection* actually traversed (`MATCH ()-[:TRANSFER*]->()`) rather than the whole multigraph — the TRANSFER subgraph is usually far less cyclic than the union of all edge types; consider **per-frequent-edge-type SCC indexes**. (b) Inside the giant SCC, fall back to the §4 machinery (two-sided distance barriers + hub suspension). Construction-time marking is a powerful *filter for the acyclic majority and a router into the right per-query strategy*, not a silver bullet for the dense core.

### Hierarchical refinement: Bourdoncle's weak topological ordering (WTO)

Flat SCC decomposition gives only a binary `inCycle` and collapses the giant SCC to one structureless blob. **Bourdoncle's weak topological ordering** ([Bourdoncle93]) refines it: a Tarjan-derived construction that decomposes the graph *recursively* — find an SCC, designate its DFS-entry vertex as the **head**, remove the head, re-decompose the remainder (exposing nested inner SCCs), and recurse. The output is a nesting like `A ( B ( D E ) C )` — a head at every level — i.e. a **recursive condensation** rather than the flat one of §9. It is the clean Tarjan-derived form of the loop-nesting forests already cited here ([Havlak97, Rama99]); §7's flat `inCycle` + `isLoopHead` is its one-level shadow.

Three things it adds:

1. **Structure *inside* the giant SCC (the main draw).** The flat condensation throws away all intra-SCC structure; WTO keeps a recursive one. So a ring query `(a)-[:TRANSFER*..k]->(a)` can be restricted to the **innermost component containing `a`** — often far smaller than the whole SCC — and s-t reachability/distance pruning (§8-D, §9-Rescue-1) can be applied **level by level** over the recursive condensation instead of over one flat DAG. This is an axis orthogonal to typed projection (§9-Rescue-2): projection dissolves the SCC *by edge type*, WTO decomposes whatever remains *by nesting*; stack them.

2. **The correct iteration order for a semi-naïve / frontier fixpoint (Option C, §5).** If TuringDB evaluates recursive patterns as a fixpoint over a cyclic graph, the propagation *order* does not change the answer but does change the iteration count (chaotic-iteration theory). WTO is the established good order: **saturate each inner component to its local fixpoint before resuming the enclosing one** (otherwise every outer lap re-disturbs a half-finished inner loop and the iteration count multiplies with nesting depth), and **test convergence only at each component's head** (the entry vertex — also the natural widening/acceleration point). *Scope, honestly:* this pays off for recursive computations whose per-node value **tightens repeatedly** — shortest/min-hop **distance labels** (the Rescue-1 barrier, Bellman-Ford-shaped) and monotone **recursive aggregates** (min/max/sum along paths) — and is largely **overkill for plain boolean reachability / DISTINCT endpoints**, where a visited-set BFS already touches each node once (§8-C is effectively one-pass-optimal there).

3. **A finer termination mode.** The non-standard "each loop head at most once" mode (point 5 above) refines to "each head at most once *per nesting level*" — same termination guarantee (*G* minus the per-level head set is a DAG), a result set between simple paths and walks, but a tighter bound than the flat head-once.

**Caveats (why it is a frame, not a free win):**
- **DFS-order-dependent and non-canonical for irreducible graphs.** Like all loop-header decompositions, WTO — and the head choice — depends on DFS order when the graph is irreducible, and transaction graphs are highly irreducible (multi-entry cycles). The heads are *one* valid hierarchical feedback set, not canonical or minimal (min-FVS is NP-hard, [Karp72]).
- **The giant core may decompose *shallowly*.** WTO buys structure only when the cyclic core genuinely nests. A densely irreducible mixing core can collapse to a near-flat one-level component (one head, millions of siblings), giving little more than flat SCC. **Measure the WTO nesting-depth distribution on a real graph before betting an operator on it.**
- **Not a correctness mechanism.** Same layering as §9 — inside a non-trivial component you still run the exact per-path check; WTO only decides *where* and *in what order*.
- **Build/maintenance.** Construction is Tarjan-order, but incremental maintenance is *harder* than flat SCC — nesting complicates the insert-merge / delete-split story above.

**Position:** treat WTO as the general form of this whole section — it unifies §7's loop-head marking, §9's SCC-restriction, and Option C's iteration order into one recursive object, and it is the most principled answer to "give me structure inside the giant SCC." Its payoff on the fraud core specifically hinges on that core having non-trivial nesting depth — the one thing to validate empirically first. Efficient/parallel WTO construction is active work ([KVT20]; *verify venue before citing*).

---

## 8. Mapping to TuringDB's operator set — recommended options

Grounded in `query/ir` (NL dialect + `NLExecutor`) and the v1 `PathExplorerProcessor`.

**A. Vectorized TRAIL/ACYCLIC filter on carried columns (fixed-length chains).**
At hop *k* the new `edgeIDs` chunk and each carried edge-ID column are row-aligned *after the gather* in `runEdgeLoopSteps`. A trail check is *k*−1 SIMD `!=` compares + one selection-compaction of the live columns — O(k) per row, no hashing, no allocation. **Filter before the gather** to skip re-aligning doomed rows. This alone kills the worst BOTH-direction multiplier (`a-[e]-b` then `b-[e]-a`). IR: either a `nl.row_distinct(%eids_k, {%eids_1..k-1}) -> mask` + `nl.select`, or fuse a `unique_with {…}` operand onto the hop op. ACYCLIC = same on node-ID columns. This is what gives TuringDB **Cypher parity** on plain `MATCH` (Neo4j default ALL TRAIL, §1), which it lacks today.

*Why this needs no hashing (a common worry).* TRAIL is **per-path (per-row), not a global set** over the result — two different result rows may reuse the same edge freely, so there is no growing set spanning the whole match. And the check is **incremental**: maintain the invariant that a surviving row's carried edges `e₁…e_{k-1}` are already all-distinct, so at hop *k* you only compare the *new* `e_k` against those *k*−1 carried edges — plain `uint64` equality, SIMD, no hash-set, no allocation. (v1's `edgeUsedInPath` already does exactly these compares, as an O(depth) parent-chain walk — no hashing.) A membership structure only earns its place at **large or unbounded *k***, where carrying *k* edge columns and the O(k²)-per-path compares start to hurt — and even then the vectorization-friendly answer is the per-path **64-bit fingerprint** of Option B (one `u64`, O(1) per extension, exact walk only on a bit-hit), never a per-row hash set (which does not vectorize). The one scoping subtlety: Cypher's per-`MATCH` uniqueness (GQL `DIFFERENT EDGES`, §1) compares the new edge against *all* segments' carried edge columns, not just the current path segment's — still per-row, still plain compares, just a wider carried set.

**B. Columnar variable-length op for path-returning queries.**
Keep v1's frontier-tree design but make it **SoA/columnar** (`node`/`edge`/`parent`/`source` columns, not `vector<struct>`) and chunk-driven, exposed as an iterator op: `nl.expand_paths(%srcs, min, max, semantics) -> iter`. Adopt Kuzu's **24-byte-per-edge parent-pointer DAG** for materialization (§3). Accelerate the uniqueness test with a **per-path 64-bit edge/node fingerprint column**: `fp[child] = fp[parent] | bit(hash(edge))`; if `fp[parent] & bit == 0` extend with **no walk** (exact negative), only do the O(depth) chain walk on a bit-hit. **Bound memory** by replacing global BFS layers with **chunked DFS** (a stack of per-depth chunk cursors) so footprint is O(maxHops · chunkSize · width) — fixing v1's unbounded-memory warning while staying vectorized and composing with the existing limit-state early-exit unwinding.

**C. Reachability collapse when the path is dead (highest leverage for fraud).**
Analyzer rule (§5): path variable unused ∧ endpoints consumed by DISTINCT/aggregate/exists ⇒ lower `-[*..k]->` to a **frontier BFS with a global dense visited bitmap** (`NodeID` is dense — a bitmap is a flat `O(V)`-bit array), Kuzu-style. Reuse the existing `nl.distinct_filter` machinery for the streaming dedup. This is exactly the FinBench `RETURN DISTINCT other.id` shape (§4) and mirrors Neo4j pruning-var-expand [Neo4j-Prune] and DuckDB USING KEY's ~31,500× row reduction [DuckKEY]. Optionally pack 64 source lanes per node (MS-BFS) for multi-source fraud sweeps — beneficial precisely on the short-diameter hub-dense fraud graphs [MSBFS].

**D. Barrier / bidirectional pruning for anchored s-t and ring queries.**
For `(a{id})-[*..k]->(b{id})` or `(a)-[*..k]->(a)`: a **reverse-graph BFS prepass** from the target to depth *k* fills a dense `dist_to_target` u8 array (`nl.distances` op); forward expansion filters rows where `depth + dist[v] > k` (vectorized gather+compare). This is PathEnum's two-BFS index / BC-DFS barriers (§4) — orders of magnitude on AML ring queries. **Meet-in-the-middle** (expand both ends k/2, join the middle) is the stronger variant and the join is natural in the NL model — but heed Peng's warning that joining simple sub-paths yields non-simple cycles (§4).

**E. Construction-time SCC / loop-head marking (§7).**
Compute `sccID`, `inCycle`, `intraSCC`, `isLoopHead` at DataPart seal. Query-time: the **acyclic fast path** (skip uniqueness for `inCycle=false` prefixes), **SCC-restricted ring search**, **condensation-DAG reachability pruning**, all on dense per-node columns TuringDB can store cheaply. Exact on insert (SCCs only merge), lazy rebuild on delete (stale `inCycle=true` is sound). Prefer **per-edge-type SCC indexes** to dodge the giant-SCC problem.

**Cross-cutting, do regardless:**
- **Surface GQL path modes** (`WALK|TRAIL|SIMPLE|ACYCLIC` + `ANY`/`SHORTEST` selectors); make **TRAIL the default** (Cypher parity, and v1 already does it); keep WALK as explicit opt-in.
- **Mandatory or defaulted hop upper bound** (Kuzu's 30, Oracle's `ALL`-requires-bound, the GQL finiteness rule). This is the cheapest single guardrail against "spinning in cycles."
- **Parallel-edge dedup per hop** for endpoint semantics (fraud multigraphs have many parallel transfers — FinBench edge multiplicity).
- **Hub handling** for tail latency: degree-aware suspension / FinBench-style **truncation** (bounded fan-out per vertex at a hop) — 93–99% of fraud paths hit a hub (§4).
- **Push per-hop predicates into expansion**, not post-filter (Neo4j inline predicates, FinBench 70–87% wins).

### Comparative table of implementation options

| Option | Semantics delivered | Per-extension cost | Extra memory | Vectorization | Adopting systems / basis |
|---|---|---|---|---|---|
| **A. Carried-column trail/acyclic filter** | TRAIL / ACYCLIC on fixed chains | O(k) SIMD compares + compaction | none | **excellent** (row-parallel `!=`) | Cypher parity; novel-columnar |
| **B. Columnar frontier-tree + fingerprint** | TRAIL/ACYCLIC/SIMPLE with path return | O(1) fingerprint common; O(depth) walk on hit | 8 B fp/entry + 24 B/path-edge; **bounded** via chunked DFS | good (fills), poor (walk) | Kuzu parent-DAG [Kuzu-RecJoin25]; TuringDB v1 |
| **C. Reachability collapse (dense visited)** | node-reachability / DISTINCT endpoints / aggregates (`ANY`) | O(1) bit test+set | O(V) bits (×ω lanes if MS-BFS) | **excellent** (bitmap / AVX-512) | Neo4j pruning-expand, Kuzu shortest, DuckDB USING KEY, MS-BFS/DuckPGQ [Neo4j-Prune, Kuzu-RecJoin25, DuckKEY, MSBFS, DuckPGQ] |
| **D. Two-sided distance/barrier pruning** | hop-bounded SIMPLE s-t / cycles | O(1) gather+compare after BFS prepass | O(V) dist labels | **excellent** | PathEnum, BC-DFS/Peng, GraphS [PathEnum, Peng20, Qiu18] |
| **E. Construction-time SCC / loop-head marks** | acyclic fast-path; SCC-restricted rings; DAG-reachability prune | O(1) bit-gather per hop | O(V) sccID/flags + optional DAG labels | **excellent** | Johnson per-SCC, GRAIL/2-hop, HP-Index (incremental) [Johnson75, Tarjan72, Qiu18] |
| **Product-graph (node,state) dedup** | RPQ WALK / shortest, compact PMR | O(1) per product node | O(\|A\|·V) | partial | MillenniumDB/PathFinder [MDB-ASP, PathFinder] |
| *(baseline)* **v1 parent-chain walk** | TRAIL, path return | O(depth) walk **every** extension | **unbounded** frontier | poor | TuringDB `PathExplorerProcessor` |

**Recommended sequencing:** (1) mandatory hop cap + TRAIL default + surface modes — cheap, removes the outlier behavior. (2) Option A — Cypher parity on fixed chains, trivial in the NL model. (3) Option C — the real fix for FinBench-shaped fraud queries (endpoints/counts). (4) Options D + E together for anchored ring/s-t detection on the cyclic core. (5) Option B to make v1 path-returning queries bounded-memory and faster.

---

## 9. Query-time uniqueness vs. construction-time structure — how they compose

The per-path check (§8-A/B) and construction-time loop-head/SCC detection (§7) are often mistaken for alternatives. They are **different layers that compose**, and getting the relationship right is what tells you when each is worth building.

- **Per-path uniqueness is a *correctness* mechanism.** It decides *which paths are valid answers* (TRAIL/SIMPLE), it is exact, and it (with a cap) guarantees finiteness. Drop it on a TRAIL query and you get *wrong answers* (WALK, not TRAIL). It runs blind to graph structure.
- **Construction-time detection is a *performance* mechanism.** SCC/loop-head marks never change the answer — a node marked `inCycle` only *might* be on a cycle; a node marked not-`inCycle` provably *cannot* be. It is a static over-approximation of "where repeats are possible."

So the first-order relationship is asymmetric: **the construction index cannot replace the per-path check for correctness** (one exception below) — it can only *eliminate the need to run it* where it proves a repeat is impossible.

**How they compose — the acyclic fast path.** While a path prefix stays in acyclic territory (all `inCycle=false`, or a "downhill" cross-SCC edge on the condensation DAG), no edge or node can repeat on *any* continuation, so the TRAIL/SIMPLE check is guaranteed to pass — skip it, and stop carrying the edge columns / fingerprint for those rows. Arm the exact check only when the traversal *enters* a non-trivial SCC. The coarse static filter routes the acyclic majority onto the cheap no-check path; the exact per-path machinery fires only in the cyclic cores.

| | Per-path uniqueness (query-time) | Loop-head / SCC index (construction-time) |
|---|---|---|
| **Delivers** | exact semantics (correctness) | pruning/skipping (performance) |
| **Optional?** | no — required for TRAIL/SIMPLE correctness | yes — pure accelerator (except the "head-once" mode below) |
| **Cost paid** | every query, per row, per hop | once at commit; amortized over all queries |
| **Guarantees termination?** | yes (TRAIL, or a cap) | no, by itself |
| **Precision** | exact (sees the real path) | structural over-approximation |
| **Best case** | short capped paths (fraud k≈6) — cheap anywhere | mostly-acyclic graphs / the acyclic fringe |
| **Worst case** | none (always correct, O(k)/row) | **giant SCC — prunes almost nothing** |
| **Maintenance** | stateless | maintain on writes (insert = merge, exact; delete = split, lazy) |

**The one case where loop-head detection genuinely substitutes** for the per-path check is the non-standard mode of §7-point-5: "visit each loop head at most once." Because the DFS-back-edge head set *H* is a hitting set for all cycles, *G*−*H* is a DAG, so a head-bounded walk terminates with only a tiny membership test against the (small) head set per extension — no edge fingerprint, no O(depth) walk. The catch: the result is a *superset* of simple paths and a *subset* of walks — **not GQL/Cypher semantics** — so it substitutes only under explicit opt-in.

### The giant-SCC reversal (why this matters for fraud)

Here is the tension you must not miss. On *mostly-acyclic* graphs the construction index is a huge win (it skips the check nearly everywhere). But **fraud/transaction graphs are the opposite** — they have one giant SCC (the bow-tie, [Broder00]). So `inCycle=true` almost everywhere on the interesting part, the acyclic fast-path prunes almost nothing, and the index degenerates to "run the per-path check everywhere anyway." **The flag-based construction index is weakest exactly on the workload that motivated the question**, while the per-path check is at its most necessary precisely inside that giant SCC. Two commit-time techniques rescue the construction-time side — and they attack the giant SCC from opposite directions. (A third, orthogonal refinement — Bourdoncle's hierarchical WTO decomposition, which exposes nested structure *inside* the SCC so ring search can target the innermost component containing the anchor — is in §7.)

**Rescue 1 — distance labels prune on an axis orthogonal to cyclicity.** `inCycle` answers "could a repeat happen here?" — useless when the answer is "yes" everywhere. A *distance label* answers a different question — "how far is `v` from the target `t`?" — which has nothing to do with cyclicity. For a hop-bounded query the necessary condition for `v` to lie on any answer is `d + shortestDist(v, t) ≤ k` (already `d` hops spent, budget `k`): if `v` is farther from `t` than the remaining budget, **no continuation can reach `t` in time — drop the row now.** This is the PathEnum/BC-DFS barrier (§4). Crucially, `shortestDist(v,t)` is a real number whether or not `v` is in a cycle, so a giant SCC does not weaken it: even inside a 10M-node SCC, with `k=6` only the nodes within 6 hops of `t` can be on an answer — a thin shell of maybe thousands, out of millions. The pruning power comes from *the hop budget being tight relative to distances*, not from acyclicity, which is why this is the construction-time index that survives.
  - *Where the distances come from* is a separate engineering choice: a commit-time distance oracle (2-hop labels [2HOP] / pruned landmark labeling [Akiba13] — reusable across queries, heavier to build and store) **or** a per-query two-BFS index (from `s`, and to `t` on the reverse graph — PathEnum's choice, lighter, no maintenance). Same barrier either way; "construction-time" is just the precomputed incarnation.

**Rescue 2 — the giant SCC may be an artifact of over-broad indexing (typed-projection SCCs).** *Why* is there a giant SCC? Because Tarjan ran over the *whole* multigraph — every edge type at once (TRANSFER, SIGN_IN, OWNS, HAS_DEVICE, HAS_PHONE…). The union is massively strongly-connected (two accounts share a device, which links a phone, which links another account…), so almost everything lands in one SCC. But a fraud ring query walks **one edge type**: `MATCH (a)-[:TRANSFER*1..6]->(a)` — the cycle you care about is a cycle *in the TRANSFER subgraph alone*, which is far sparser and far less strongly-connected than the union. So compute SCCs on the **typed projection** — the TRANSFER-only subgraph — not the whole graph:
  - *whole-graph SCC:* ~all accounts in one giant SCC → `inCycle=true` for ~100% → fast-path never fires for a TRANSFER query.
  - *TRANSFER-only SCC:* most accounts are not in a money-flow cycle (money passes through, not back) → say `inCycle_TRANSFER=true` for ~2% → the fast-path skips the uniqueness check on ~98% of rows and pays the exact check only inside the genuinely-cyclic 2%.

  "The giant SCC is an artifact of over-broad indexing" means exactly this: index the subgraph the query actually walks and the giant SCC fragments into many small ones, restoring the cheap flag-based fast-path. The catch: one SCC index per frequently-traversed edge type (more memory/maintenance), and it cleanly helps only single-edge-type traversals — but fraud ring queries are overwhelmingly single-type (`:TRANSFER`), so it is well-targeted.

**How the two rescues relate:** typed projection tries to *dissolve* the giant SCC so the cheap flags become selective again (best when the typed subgraph really is mostly acyclic); distance labels *keep* the giant SCC but prune on the orthogonal reachable-within-budget axis (the fallback when even the typed core is a dense strongly-connected blob). Try typed-projection SCCs first (cheap, often enough); lean on distance/barrier pruning where even the TRANSFER core stays densely cyclic.

**Net recommendation:** make **per-path uniqueness (with a cap) the always-on correctness baseline** — it is cheap for capped fraud queries and it is the thing that actually works inside the cyclic core. Layer the **construction-time index on top as an optimizer**, and be precise about which part you are buying: `inCycle`/acyclic-fast-path (real win on general graphs, near-useless on the giant SCC), SCC-restriction + cross-SCC pruning (kills "downhill" edges that can never return), and — the one that keeps pruning inside the giant SCC — **precomputed distance labels feeding Option D's barrier**, not cycle flags.

---

## References

1. **[MW95]** Mendelzon, Wood. *Finding Regular Simple Paths in Graph Databases.* SIAM J. Comput. 24(6):1235–1258, 1995 (VLDB 1989 conf. version). https://eprints.bbk.ac.uk/id/eprint/46335/
2. **[BBG13]** Bagan, Bonifati, Groz. *A Trichotomy for Regular Simple Path Queries on Graphs.* PODS 2013 (+ journal). https://arxiv.org/pdf/1212.6857
3. **[MT18]** Martens, Trautner. *Evaluation and Enumeration Problems for Regular Path Queries.* ICDT 2018, LIPIcs 98. https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ICDT.2018.19
4. **[Birmele13]** Birmelé, Ferreira, Grossi, Marino, Pisanti, Rizzi, Sacomoto. *Optimal Listing of Cycles and st-Paths in Undirected Graphs.* SODA 2013. https://epubs.siam.org/doi/abs/10.1137/1.9781611973105.134
5. **[RD23-Digest]** Francis, Gheerbrant, Guagliardo, Libkin, Marsault, Martens, Murlak, Peterfreund, Rogova, Vrgoč. *A Researcher's Digest of GQL.* ICDT 2023, LIPIcs 255, Art. 1. https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ICDT.2023.1
6. **[GPML22]** Deutsch, Francis, Green, Hare, Li, Libkin, Lindaaker, Marsault, Martens, Michels, Murlak, Plantikow, Selmer, van Rest, Voigt, Vrgoč, Wu, Zemke. *Graph Pattern Matching in GQL and SQL/PGQ.* SIGMOD 2022. https://arxiv.org/pdf/2112.06217
7. **[PathFinder]** Farías, Martens, Rojas, Vrgoč. *Evaluating Regular Path Queries in GQL and SQL/PGQ (PathFinder).* arXiv:2306.02194. https://arxiv.org/pdf/2306.02194
8. **[Martens23-PMR]** Martens, Niewerth, Popp, Rojas, Vansummeren, Vrgoč. *Representing Paths in Graph Database Pattern Matching.* PVLDB 16(7):1790–1803, 2023. https://www.vldb.org/pvldb/vol16/p1790-martens.pdf
9. **[MDB-ASP]** Vrgoč et al. *Evaluating Regular Path Queries under the All-Shortest Paths Semantics* (MillenniumDB). arXiv:2204.11137. https://arxiv.org/pdf/2204.11137
10. **[Neo4j-VLP]** Neo4j Cypher Manual — Variable-length paths. https://neo4j.com/docs/cypher-manual/current/patterns/variable-length-paths/
11. **[Neo4j-Prune]** Neo4j path-pattern optimization — VarLengthExpand(Pruning)/BFSPruningVarExpand. https://deepwiki.com/neo4j/neo4j/3.3-path-pattern-optimization ; Neo4j KB cardinality: https://neo4j.com/developer/kb/understanding-cypher-cardinality/
12. **[Kuzu-Diff]** Kuzu docs — Differences between Kuzu and Neo4j. https://docs.kuzudb.com/cypher/difference/
13. **[Kuzu-CIDR23]** Feng, Jin, Chen, Liu, Salihoğlu. *KÙZU Graph Database Management System.* CIDR 2023. https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf
14. **[Kuzu-RecJoin25]** Chakraborty, Salihoğlu. *Robust Recursive Query Parallelism in Graph Database Management Systems.* PVLDB 18, 2025. https://www.vldb.org/pvldb/vol18/p4465-chakraborty.pdf ; arXiv:2508.19379
15. **[Memgraph]** Memgraph — Deep Path Traversal Capabilities. https://memgraph.com/blog/memgraph-deep-path-traversal-capabilities
16. **[DuckPGQ]** ten Wolde, Szárnyas, Boncz. *DuckPGQ: Efficient Property Graph Queries in an analytical RDBMS.* CIDR 2023. https://www.cidrdb.org/cidr2023/papers/p66-wolde.pdf
17. **[MSBFS]** Then, Kaufmann, Chirigati, Hoang-Vu, Pham, Kemper, Neumann, Vo. *The More the Merrier: Efficient Multi-Source Graph Traversal.* PVLDB 8(4):449–460, 2015. https://www.vldb.org/pvldb/vol8/p449-then.pdf
18. **[PathEnum]** Sun, Chen, et al. *PathEnum: Towards Real-Time Hop-Constrained s-t Path Enumeration.* SIGMOD 2021. https://dl.acm.org/doi/pdf/10.1145/3448016.3457290
19. **[Peng20]** Peng, Zhang, Lin, et al. *Towards Bridging Theory and Practice: Hop-Constrained s-t Simple Path Enumeration.* PVLDB 13(4):463–476, 2020. http://www.vldb.org/pvldb/vol13/p463-peng.pdf
20. **[Qiu18]** Qiu, Cen, Qian, Peng, Zhang, Lin, Zhou. *Real-time Constrained Cycle Detection in Large Dynamic Graphs* (GraphS). PVLDB 11(12):1876–1888, 2018. https://www.vldb.org/pvldb/vol11/p1876-qiu.pdf
21. **[Blanusa23]** Blanuša, Atasu, Ienne. *Fast Parallel Algorithms for Enumeration of Simple, Temporal, and Hop-Constrained Cycles.* ACM TOPC 10(3), 2023. https://dl.acm.org/doi/full/10.1145/3611642
22. **[GeaFlow]** Pan, Wu, Zhao, Zhou, et al. *GeaFlow: A Graph Extended and Accelerated Dataflow System.* SIGMOD 2023 (PACMMOD 1(2)). https://dl.acm.org/doi/10.1145/3589771
23. **[FinBench]** Qi et al. *The LDBC Financial Benchmark: Transaction Workload.* PVLDB 18, 2025. https://www.vldb.org/pvldb/vol18/p3007-qi.pdf
24. **[DuckKEY]** DuckDB — *USING KEY in Recursive CTEs* (May 2025); backed by CIDR 2023 "A Fix for the Fixation on Fixpoints" & SIGMOD 2025. https://duckdb.org/2025/05/23/using-key
25. **[RelPathQ26]** *Efficient Path Query Processing in Relational Database Systems.* arXiv:2604.02553, 2026. https://arxiv.org/pdf/2604.02553

**§7 canonical references (standard results; verify venues against DBLP):** [Tarjan72] Tarjan, *Depth-first search and linear graph algorithms*, SIAM J. Comput. 1(2), 1972 · [Johnson75] Johnson, *Finding all the elementary circuits of a directed graph*, SIAM J. Comput. 4(1), 1975 · [GRAIL] Yıldırım, Chaoji, Zaki, *GRAIL*, PVLDB 3(1), 2010 · [2HOP] Cohen, Halperin, Kaplan, Zwick, SODA 2002 / SICOMP 32(5), 2003 · [BFGT16] Bender, Fineman, Gilbert, Tarjan, TALG 12(2), 2016 · [HKMST12] Haeupler, Kavitha, Mathew, Sen, Tarjan, TALG 8(1), 2012 · [Havlak97] Havlak, TOPLAS 19(4), 1997 · [Rama99] Ramalingam, TOPLAS 21(2), 1999 · [Karp72] Karp, 1972 · [Broder00] Broder et al., *Graph structure in the web*, WWW 2000 · [Akiba13] Akiba, Iwata, Yoshida, *Pruned Landmark Labeling*, SIGMOD 2013 · [2SCENT] Kumar, Calders, PVLDB 11(11), 2018 · [Bourdoncle93] Bourdoncle, *Efficient chaotic iteration strategies with widenings*, Formal Methods in Programming and their Applications, LNCS 735, pp. 128–141, 1993 · [KVT20] Kim, Venet, Thakur, *Deterministic Parallel Fixpoint Computation*, POPL 2020 (PACMPL 4) — efficient/parallel WTO construction; verify before citing.
