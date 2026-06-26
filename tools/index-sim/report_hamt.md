# Improving the HAMT for strings — and when it beats a lock-free multiversion hash

A follow-up to [`report.md`](report.md). That study compared five versioned-index designs and found
the **lock-free multiversion hash** wins HEAD reads (165 ns) and memory, while the **HAMT** is the
conservative persistent default but trails on read (328 ns), submit (18 µs), and memory (13 MiB) —
yet *structurally* beats the hash on time-travel (flat vs 164 µs) and is wait-free without any
lock-free machinery.

This report asks the next question: **for string keys specifically, how far can we close the HAMT's
HEAD-read / submit / memory gap to the lock-free multiversion hash — without giving up the
structural wins that make a persistent structure the right fit for a git-like commit model?**

All numbers below are from `index_sim` (`--help` for flags); reproduce with
`g++ -std=c++23 -O2 -o index_sim index_sim.cpp && ./index_sim`. New flag: `--key-len N` (string-key
length in bytes). It remains a *cost model*, not a benchmark — the shapes are the message.

> **Validated against real timings.** The three core structures were since built and microbenchmarked
> (`index_bench.cpp`, [`report_bench.md`](report_bench.md)). The structural conclusions below hold —
> time-travel and long-key reads favor the persistent tries — but two cost-model claims do **not**
> survive measurement: absolute read latency is ~5× lower for cache-resident trees (the 80 ns hop is a
> cold/large-tree figure), and for **short** keys the multiversion hash is the fastest reader, not ART
> (ART's read win starts at ~48-byte keys). ART's copy-on-write write/memory cost also proved its
> *worst* axis, not its best. Read the modeling below with that correction in mind.

> **Bottom line up front.** Three HAMT tunings (fold the commit, widen the fan-out, compact the
> nodes) shrink the gap, but the decisive move for *strings* is to stop hashing: a **persistent
> Adaptive Radix Tree (ART)** navigates by key bytes, so it is shallower, smaller, ordered, and —
> because it **never hashes** — reads at the multiversion hash's latency *minus the hashing cost*:
> parity-to-better at short keys (169 ns vs 180 ns in the tested regime) and **decisively ahead as
> keys lengthen** (203 ns vs 256 ns at 100-byte keys). It keeps every structural advantage the hash
> gives up: flat time-travel, wait-free reads, no version-reclamation problem, and no concurrent
> string-interning bottleneck.

## What "for strings" changes

The prior model charged a flat `hash = 5 ns` and `compare = 2 ns`. For real string keys both are
**O(length)**, and *where the bytes live* drives memory and the concurrency story:

- **Hashing** a key is `≈ fixed + len·perByte`. A hash trie pays it **once** per lookup (at the top,
  then it slices the hash); but the multiversion hash pays it on **every** slot probe, and a
  dictionary pays it again to assign a code.
- **Comparing** two keys on a collision or at a leaf is also O(length).
- **A radix trie hashes nothing** — it descends by the key's bytes, so the trie structure *is* the
  comparison. This is the structural reason ART pulls ahead as keys get longer.
- **Interning** strings into dense integer codes makes internal compares cheap and dedups the bytes,
  but assigning a code to a *new* string needs a concurrent dictionary — a write-side bottleneck that
  hits the lock-free hash hardest (see the head-to-head).

With a cache-miss hop at 80 ns, the per-byte string costs only start to rival a hop for long keys:

| key length | string hash | string compare | (one hop) |
|-----------:|------------:|---------------:|----------:|
| 16 B | 11 ns | 9 ns | 80 ns |
| 64 B | 35 ns | 29 ns | 80 ns |
| 256 B | 131 ns | 105 ns | 80 ns |

So for short keys the **hop count (tree depth) dominates** and the string overhead is secondary; for
long keys the *hashing itself* becomes a first-order cost — which is exactly what ART avoids.

## Four ways to improve the HAMT

### 1. Fold the commit — transient (batch) mutation

A persistent map normally path-copies `depth` nodes **per write**. Applying a 32-write commit as 32
independent path-copies re-copies the shared upper nodes 32 times. The **transient** technique
(Hickey, Clojure 1.1) tags each node with an owner/*edit* token (`AtomicReference<Thread>`): a write
mutates the node in place if the tokens match, else copies-once-and-retags, and `persistent!` nulls
the token in **O(1)** to freeze the batch. A node shared by several writes in the commit is copied
**once**. Reported at **~1.5–4× faster bulk builds**; the only standing cost is "one more reference
per node" (~3 % memory, as measured by the CHAMP authors). This is *literally* TuringDB's commit
model — apply a write-set to a private edit, then freeze and publish.

Modeled as the distinct nodes touched (balls-in-bins) instead of `writes·depth`:

| writes/commit | naive path-copy | folded (transient) | submit saving |
|--------------:|----------------:|-------------------:|--------------:|
| 32 (default) | 22.0 µs | **14.6 µs** | 34 % |
| 1000 (bulk) | 688 µs | **285 µs** | 59 % |
| 10000 (bulk) | — | — | ~76 % |

Folding also cuts **retained memory** the same way (fewer new nodes per commit): HAMT memory at 2000
parts drops from the prior 13.1 MiB to **7.9 MiB**. The win scales with write-set density — small for
a 32-write commit, large for a bulk load — and it costs nothing in read latency or correctness. It is
fully compatible with full version retention: the freeze is the commit's single root publish.

### 2. Widen the fan-out — fewer, costlier nodes

Read latency is dominated by cache-miss hops, i.e. **tree depth**. Wider nodes mean a shallower tree
(fewer hops) but more occupied slots to copy on each path-copy (`copy ∝ width`) and more memory.
There is a real Pareto frontier — and the default branch=32 is *not* on it:

| branch | depth | coded read | folded submit | note |
|-------:|------:|-----------:|--------------:|------|
| 32 | 4 | 422 ns | 14.6 µs | default |
| **64** | **3** | **342 ns** | **11.9 µs** | **dominates 32 on *both* axes** |
| 128 | 3 | 342 ns | 16.5 µs | |
| 256 | 3 | 342 ns | 25.0 µs | copy volume climbing |
| 512 | 2 | **262 ns** | 20.9 µs | reaches multiversion-hash read parity |
| 1024 | 2 | 262 ns | 37.9 µs | copy volume dominates |

In this cost model **branch = 64 dominates branch = 32** — the depth drop from 4→3 cuts hops *and*
total path-copy work. To reach multiversion read parity you need branch ≈ 512 (depth 2), but then each
write copies a 512-wide node and submit balloons. Take the 64-way win; go wider only for a
read-dominated, write-rare column.

(Honesty check: Bagwell's classic HAMT picks **32** as the sweet spot, and there is no published
controlled 32-vs-64 benchmark for *immutable* maps — this is a model-derived suggestion, sensitive to
the `copySlot` and `hop` constants, and worth validating before adopting. The robust claim is the
*shape*: depth is the read lever, copy-volume is the write penalty, and 32 is below the read-optimal
for a 100 k-key map.)

### 3. Compact the nodes — CHAMP layout

CHAMP (*Compressed Hash-Array Mapped Prefix-tree*, Steindorfer & Vinju, OOPSLA 2015) uses **two
bitmaps** (`datamap` / `nodemap`) to split inline data entries from sub-node references into one
shared array (data-left, nodes-right), killing the per-slot type check and the wasted empty slot per
sub-node, plus compact-on-delete canonicalization. Point-lookup hop count is unchanged, but nodes are
smaller and denser: the paper reports **−64 % / −52 % memory** (maps / sets vs Scala), **iteration
1.3–6.7×**, **equality 3–25.4×** (sub-linear, via the bitmaps + reference short-circuit), and LLC
misses cut ~3.2×. It underlies **Scala 2.13's** immutable `HashMap`/`HashSet` and the Java `capsule`
library. The sub-linear **structural equality** is the standout for a git model — it makes *diffing
two commits' indexes* cheap, short-circuiting on shared sub-trees.

(Caveat: those headline figures are JVM-specific — boxing and object headers make the JVM baseline
fat. The *layout* ideas port to C++, but the magnitude shrinks; the model below uses a conservative
40 B vs 64 B node, ~38 %, not the paper's 64 %.)

Modeled as a smaller per-node size on top of folding, retained memory at 2000 parts:

| | HAMT (naive) | HAMT (folded) | CHAMP (folded) |
|---|---:|---:|---:|
| memory @ 2000 parts | 13.1 MiB | 7.9 MiB | **4.9 MiB** |

Folding + CHAMP together cut the HAMT's memory by ~60 %, below the multiversion hash's 2.7 MiB only
once paired with coding/ART (next). Fully compatible with persistence — CHAMP *is* a persistent map.

### 4. Dictionary-code the keys — but not for the read you'd think

It is tempting to read the prior report's "key by dictionary code" as a read-latency win. For a HAMT
it is **not** — and the model makes that explicit. Coding adds an *intern* step (hash the string,
probe the dictionary) on the read boundary, and a HAMT only does **one** compare per lookup (at the
leaf), so coding trades a ~9 ns string-compare for an ~80 ns intern hop — a **net loss on reads**:

| (16-byte keys, HEAD read) | string-keyed | dictionary-coded |
|---|---:|---:|
| HAMT | **340 ns** | 422 ns |
| multiversion hash | **180 ns** | 265 ns |

Coding earns its keep elsewhere: it **dedups the bytes** (memory), makes the many compares of a
**B-tree** descent or a **hash-collision** probe cheap, and yields **ordered int keys** for range
scans. But it introduces a concurrent **code-assignment** dependency for never-seen strings — which,
as the head-to-head shows, penalizes the lock-free hash more than the single-publish HAMT. Net: code
for memory/ordering, not for the point read; and a hash trie barely benefits on the read path at all.

*(Cheap companion: **hash memoization** — cache the key's hash in the leaf — removes repeated
O(length) rehashing on collision walks and rebuilds. Near-free, and it matters more the longer the
keys; it does not change the asymptotics but trims the string constant.)*

## The decisive move for strings: a persistent Adaptive Radix Tree

ART (Leis, Kemper & Neumann, ICDE 2013) is a byte-wise radix tree (span 8) with **adaptive node
sizes** — Node4 / 16 / 48 / 256 (52 / 160 / 656 / 2064 B incl. header), SIMD-searched at Node16 — so
sparse nodes stay tiny, plus **path compression** and **lazy expansion** to collapse non-branching
runs. For string keys this is structurally better than a hash trie:

- **No hashing.** It descends by key bytes; the structure is the comparison. A 256-way fan-out means
  depth ≈ `log₂₅₆(keys)` ≈ 2–3 for 100 k keys — shallower than a 32-way HAMT (depth 4). Lookup is
  O(key length), **independent of n**.
- **Ordered.** Range and **prefix** queries come for free — and TuringDB's existing `StringIndex` is
  literally a prefix trie built for exactly that, so ART matches the access pattern already in use.
- **Prefix-shared + adaptive** → the smallest memory of any design here: a proven ≤ 52 B/key (≈ 34
  B/key typical); path compression cut the TPC-C string-index height from 40 to 8.1.
- **Persistent-capable, and not hypothetically.** Version-retention copy-on-write ARTs already ship:
  **PART** (Dave et al., UC Berkeley — path-copying + per-node refcounting, with union/intersect/range),
  **VART** (SurrealDB, Rust CoW), and **PermART** (SIGMOD 2026, multiversion). A CoW ART path-copies
  nodes exactly like a HAMT, keeping the structural wins: one atomic root publish per commit, wait-free
  reads, free time-travel.

The one ART-specific cost to respect: because node *types* are adaptive, a copy-on-write update can
trigger a **node-type reallocation** (a Node16 that gains a 17th child is reborn as a Node48), so some
writes copy *and grow* a node. This is the real tension in persistent ART and is what the
fan-out-aware submit cost below captures (dense upper nodes are Node256-wide to copy).

Simulated against the lock-free multiversion hash, string keys:

| (HEAD read) | 16-byte keys | 100-byte keys |
|---|---:|---:|
| **persistent ART** | **169 ns** | **203 ns** |
| multiversion hash (string-keyed) | 180 ns | 256 ns |
| HAMT (string-keyed, branch 32) | 340 ns | 416 ns |

| @ 2000 parts | submit (32 writes) | retained memory |
|---|---:|---:|
| persistent ART | 11.2 µs | **1.9 MiB** |
| HAMT (folded) | 14.6 µs | 7.9 MiB |
| multiversion hash | 4.5 µs | 2.7 MiB |

ART reads at the hash's latency **minus the hashing cost** — `depth·hop + compare` vs the hash's
`2·hop + hash + compare`. The catch is `depth`: at the tested cardinality (≤ 65 k distinct → ART
depth 2) ART wins outright for *any* key length; at higher cardinality ART deepens to 3 hops and the
hash's O(1) reclaims up to one hop for **short** keys (crossover ≈ 150 bytes). But ART keeps the lead
for long keys — the margin **widens with key length** regardless, because the hash must digest every
byte while ART never does — and it wins every *structural* axis below no matter the regime. Its memory
is the smallest of all. Its submit (11 µs at 100 k keys; it grows with cardinality as dense upper nodes
promote to Node256 and cost as much to copy as a wide HAMT node) is its weakest axis — still 2–3× the
hash, the price of path-copy persistence — but for bulk commits even that inverts: at 1000
writes/commit ART's folded submit (99 µs) **beats** the hash (140 µs).

## Head-to-head: improved persistent trie vs lock-free multiversion hash

| axis | lock-free multiversion hash | improved persistent trie (ART/CHAMP + fold) | winner |
|------|------------------------------|----------------------------------------------|:------:|
| HEAD point read, short keys | 180 ns | **169 ns** (ART) | trie † |
| read, long keys (256 B) | 256 ns | **203 ns** | trie |
| submit (32 writes) | **4.5 µs** | 11–15 µs | hash |
| submit (bulk, 1000) | 140 µs | **99 µs** (ART) | trie |
| retained memory | 2.7 MiB | **1.9 MiB** (ART) | trie |
| **time-travel (as-of N back)** | 164 µs — filters every newer version | **flat ~85 ns** — jump to retained root | **trie** |
| **write concurrency** | needs lock-free CAS + backoff; hot-key retry storms | one root-publish CAS per commit | **trie** |
| **read/writer contention** | wait-free only if lock-free | always wait-free (single publish) | **trie** |
| **version reclamation** | must GC old version nodes (hazard ptrs / epoch) — **conflicts with full retention** | immutable; old roots are just shared structure | **trie** |
| **new-string interning** | concurrent dictionary = write bottleneck | done inside the single-writer commit | **trie** |
| range / prefix scans | no (unordered hash) | **yes** (ART is ordered) | **trie** |

† Short-key point read is the one regime-dependent row: the trie wins while ART is depth-2 (≤ 65 k
distinct) and for any long key, but the hash's O(1) reclaims up to one hop for short keys once ART
deepens to depth 3. Every other row holds regardless of cardinality.

The hash's only durable advantage is **uncontended submit latency**. Everything else either favors the
persistent trie outright or is a HEAD-read lead that the trie tunings erase. And the hash's four losses
are **inherent, not tunable**:

1. **Time-travel degrades by construction.** Newest-to-oldest version ordering makes a HEAD read O(1),
   but an as-of read N commits back must walk past every newer version in the slot's chain —
   O(versions newer than the snapshot), unbounded for a churned key. The entire MVCC field keeps
   chains *short on purpose* because traversal cost grows with length (Wu & Pavlo, VLDB 2017, measure
   2.4–3.4× from version ordering/length alone); full retention forfeits exactly that lever. Persistent
   trees jump to a retained root and stay flat. *What wins the present loses the past.*
2. **Reclamation fights retention — the keystone.** Every lock-free reclaimer (hazard pointers, EBR,
   RCU) frees a node once no *in-flight* reader can reach it; the MVCC watermark for that is "the oldest
   active transaction." Full time-travel pins that watermark at t₀ **forever**, so no version is ever
   dead and memory grows Θ(total writes). Böttcher et al. (PVLDB 2019) call the bounded version of this
   the "vicious cycle of garbage"; under full retention it never resolves. Persistence sidesteps it
   entirely — old roots aren't garbage, they're the shared structure old commits still point to.
3. **Lock-free resize is genuinely hard.** Open addressing physically moves every element on grow, and
   you can't move N items under one CAS — so you need freeze + cooperative-help + double-buffer; growt's
   "cluster theorem" trims the overhead to ~10 % but cannot remove it. The one design that *dissolves*
   resize — Shalev & Shavit's split-ordered lists — does so by **never moving items**, i.e. the same
   structural-sharing idea the trie is built on.
4. **The string-interning bottleneck.** Assigning a dense code to a never-seen string is a synchronous,
   serializing next-code handoff; JVM `String.intern` degrades ~7,000,000× (to ~650 ms/op) at 1 M
   distinct strings. Column stores only *sidestep* it with per-thread/delta dictionaries merged later —
   never solve it — and that deferral is itself a small versioned-merge problem. A single-writer commit
   pays this once, locally; a lock-free table pays it on the hot path.

The concurrency point cuts the other way too: a persistent trie need not even *concede* lock-free
operation to claim these wins. **Ctrie** (Prokopec et al.) is a lock-free concurrent HAMT with
single-word CAS updates and **O(1) atomic snapshots** — the trie family already does wait-free reads,
lock-free writes, *and* constant-time versioning together.

## Recommendation for TuringDB

Grounded in the current code: TuringDB has **no dictionary for string property *values*** (only
`LabelMap` for ~128 labels; `StringContainer` stores values un-deduplicated), and its string index
today *is* design #5 — a **per-DataPart prefix trie** (`StringIndex`) collected across all parts at
read, i.e. the O(parts) fan-out the prior report flagged. Against that baseline:

1. **A persistent trie fits the commit model exactly.** Each TuringDB commit already publishes an
   immutable snapshot; a persistent index makes that publish a single atomic root swap, gives
   **wait-free reads** and **free as-of queries** (read at any commit = read its retained root), and
   has **no version-reclamation problem** — old roots are just shared structure held by old commits.
   This is the conservative default for string properties whose reads span history.

2. **For strings, prefer a radix trie (ART) over a hash trie (HAMT/CHAMP).** It drops hashing
   (cheaper, and far cheaper as keys lengthen), is shallower, uses the least memory, and is **ordered**
   — which lines up with the **prefix/approximate matching `StringIndex` already provides**, something
   a hash structure cannot do at all. Apply the HAMT tunings that transfer directly: **fold each
   commit** (transient batch — biggest win on TuringDB's bulk-ish DataPart builds), a **64-way**
   default fan-out, **CHAMP-style compact nodes**, and **hash memoization** for long keys.

3. **Reserve the lock-free multiversion hash for the narrow slice it actually wins:** low-cardinality,
   stable, HEAD-read-only string properties (status/type/label-like) where as-of reads are rare and
   the chain stays short. Even there it wants a concurrent string dictionary and an epoch/hazard
   reclaimer that a full-retention store can never let run to completion — so it buys an uncontended-
   submit edge at the cost of the very time-travel guarantee the rest of the engine provides.

4. **Dictionary-code values for memory and ordering, not for point reads.** Interning is worth it to
   dedup repeated strings and to get int-ordered range scans, but it does not speed a trie point
   lookup, and its concurrent code-assignment is a write-side cost — cheap inside a single-writer
   commit, expensive in a lock-free table.

5. **Think in two index levels — they compose.** TuringDB already has both: a *per-DataPart* local
   index and a *cross-part* read-time collection. A persistent ART is the natural **cross-part global**
   structure (it kills the O(parts) fan-out with one logical map that shares history across commits).
   The **per-DataPart** index, by contrast, is *static once built* — the ideal case for a **succinct
   trie** (SuRF / FST, ~10 bits/key): an order of magnitude smaller than a pointer-based trie, and a
   range-capable replacement for the per-part Bloom/zone-map prune the prior report recommended. Global
   persistent ART for the as-of/HEAD logical map; succinct static tries per immutable part for the
   prune — each plays to a structure's strength.

In one line: **the best way to "improve the HAMT for strings" is to fold + widen + compact it, and
then to recognize that for byte-structured keys the persistent *radix* trie (ART) is the better
member of the same family — it closes the hash's HEAD-read lead while keeping the time-travel,
wait-free, retention-friendly properties that make a persistent structure the right backbone for a
versioned store.**

## Method & caveats

- **Fixed-cost model.** Operation counts come from executing the workload (distinct keys, tree depth,
  nodes touched per commit via balls-in-bins); latencies from the constants (`--help`). Retune and
  rerun for your hardware. String costs are `fixed + len·perByte` (`--key-len` sets the length).
- **Node-copy is width-aware.** Path-copying a node costs `hop + alloc + width·copySlot`; upper nodes
  are charged full-width, which is what makes wide fan-out's write cost (and ART's Node256 upper
  nodes) visible. `copySlot ≪ hop` (a streaming memcpy, not a random chase).
- **Folding via balls-in-bins.** A commit's distinct nodes touched at level d uses W writes into
  min(branch^d, keys) bins — an expectation under uniform churn; skewed write-sets share more (folding
  helps more).
- **ART depth is the 256-way bound** (`log₂₅₆ keys`); real path compression can make it shallower, so
  the ART read/submit here is, if anything, conservative. ART submit grows with cardinality as dense
  upper nodes promote to Node256.
- **Multiversion as-of / reclamation are the worst case** for a churned key (chain depth = lag), as in
  the prior report; a stable low-cardinality key stays flat — which is why that slice remains the
  hash's niche.

## Files

- `index_sim.cpp` — the simulator (now with `runHamtVariants`, `runFanoutSweep`, `runStringFrontend`).
- `results_hamt_variants.csv`, `results_fanout.csv`, `results_string_frontend.csv` — the new curves.
- `report.md` / `report.html` — the original five-design study this builds on.
