# Wall-clock validation — HAMT vs ART vs multiversion hash

`index_sim` is a *cost model*: it charges every pointer hop a flat 80 ns and every node copy a flat
slot cost. `index_bench.cpp` builds the three structures for real and times them, to see which of the
model's conclusions survive contact with a CPU. All three index the **same** replayed workload over a
**shared** key pool; correctness is cross-checked (every present key's HEAD value, plus an as-of read)
before any timing is trusted. The query key is hashed *inside* the timed read for the hash structures
(ART hashes nothing) — the fair comparison.

Reproduce: `g++ -std=c++23 -O2 -o index_bench index_bench.cpp && ./index_bench`.

## What got built

Three persistent, versioned, string-keyed indexes (a retained root/version per commit):

- **Multiversion hash** — open-addressed table, each slot a newest-first version chain. HEAD read =
  hash + probe + head; as-of read walks the chain.
- **HAMT** — 32-way bitmap-compressed, copy-on-write path-copy per write.
- **ART** — adaptive radix tree (Node4/16/48/256) + pessimistic path compression, copy-on-write. No hashing.

Caveat up front: the tries use **functional** copy-on-write (clone every node on the path), not the
transient/folded batch the model's best-case assumed — so the measured trie **submit and memory are
upper bounds**; a transient implementation would lower both. Single-threaded; the lock-free/contention
axis is not exercised here. These are minimal prototypes, not tuned implementations.

## Headline numbers

Default workload: 100 k key space, 32 writes/commit, 2000 commits (~46 k distinct), 16-byte keys.
"Warm" = this default (live tree fits L2/L3). "Cold" = ~1.26 M distinct (live tree ≫ cache).

| metric | multiversion hash | HAMT | ART |
|--------|------------------:|-----:|----:|
| HEAD read, 16 B, **warm** | **29 ns** | 68 ns | 61 ns |
| HEAD read, 16 B, **cold** | **136 ns** | 359 ns | 341 ns |
| HEAD read, **100 B**, warm | 107 ns | 158 ns | **70 ns** |
| as-of read (hot key, 2000-deep chain) | 3 500 ns | 13 ns | **3 ns** |
| submit / write (warm) | **53 ns** | 350 ns | 1 188 ns |
| retained memory (functional COW) | **11.5 MiB** | 22.9 MiB | 107 MiB |

## What the cost model got right

1. **Time-travel is the multiversion hash's structural wound — confirmed, and bigger than modeled.**
   Reading a churned key in the past, the hash walks its version chain: **3 500 ns** at a 2000-deep
   chain, 105 ns at 50-deep — cost ∝ chain depth, exactly as predicted. The persistent tries jump to
   the retained root and read in **3–21 ns flat**, regardless of how far back. This is a ~1000× gap and
   it is the most robust result in the whole study.

2. **ART's no-hashing advantage is real and grows with key length — confirmed.** ART's read is
   essentially **flat in key length** (~60 ns from 16 to 64 B) because it never digests the key; the
   hash structures rise linearly as they hash every byte. ART overtakes the multiversion hash at
   **~48-byte keys** and beats it outright by 100 B (70 ns vs 107 ns). The model's *mechanism* was right.

   | key length | mvcc | HAMT | ART |
   |-----------:|-----:|-----:|----:|
   | 16 B | 29 | 68 | 61 |
   | 32 B | 41 | 80 | 59 |
   | 48 B | 64 | 96 | 61 |
   | 64 B | 76 | 119 | 63 |

3. **The HAMT submit estimate was close.** Model folded-submit ≈ 14.6 µs/commit; measured (functional,
   warmish) ≈ 11.2 µs. The cold per-write cost (779 ns) is in the model's ballpark too.

4. **HAMT cold read ≈ the model.** At 1.26 M distinct (DRAM-bound), HAMT reads **359 ns** vs the model's
   340 ns — the 80 ns-hop model is essentially a *cold/large-tree* model, and there it lands.

## What the cost model got wrong

1. **Absolute latency: ~5× too high for warm working sets.** The model charges every hop 80 ns (a DRAM
   miss). In reality a 46 k-key tree is L2/L3-resident and hops cost a few ns — warm reads are 29–68 ns,
   not 169–340 ns. The 80 ns hop only holds once the live tree exceeds cache (the "cold" column). **Which
   regime you are in matters more than which structure you pick.**

2. **Short-key read ranking is inverted: the multiversion hash wins, not ART.** The model put ART ahead
   at 16 B (169 vs 180 ns). Measured, the hash wins short-key reads in **both** regimes (29 < 61 warm;
   136 < 341 cold) — its O(1) single probe simply touches the fewest cache lines, where ART pays 2–3
   node hops plus a final full-key compare. ART's read win is **specifically a long-key phenomenon**, not
   a general one. HAMT is never the read winner.

3. **ART's write and memory cost is its real weakness — the model was far too kind.** Model: ART submit
   ~11 µs and **smallest** memory (1.9 MiB). Measured: ART submit **38 µs/commit** (1 188 ns/write) and
   the **largest** memory (107 MiB) — the exact inversion. Cause: copy-on-write must clone whole adaptive
   nodes, and a dense subtree's upper nodes are **Node256 (2 KB)**; cloning those on every write path is
   expensive in time and retains a fat trail across versions. This is the "adaptive node types make CoW a
   node-type reallocation per version" tension the literature flagged — and it dominates. (A transient
   batch commit + structural sharing would cut both, but the wide-node clone cost is intrinsic to
   persistent ART.)

4. **The model undercounted the multiversion hash's memory** — it ignored the open-addressed slot table
   (a ~256 k-slot table at 100 k keys is ~10 MiB, dwarfing the version nodes). Measured 11.5 MiB vs
   modeled 2.7 MiB.

## Reading a snapshot that's now in the past (TuringDB's normal case)

The headline "HEAD read" is the multiversion hash's *best* case: read the absolute latest version, no
concurrent writers. TuringDB doesn't usually read like that. A session opens a snapshot at some commit,
then other sessions submit and advance HEAD — so by the time it reads, the snapshot is `lag` commits in
the past. For the multiversion hash that is an **as-of read**: walk the slot's chain past every version
newer than the snapshot. For the persistent tries it is just reading an older retained root — the same
cost as HEAD. "Version" here = commit; the chain a reader must skip = *how many times that key was
rewritten between the snapshot and HEAD.*

Measured read latency vs snapshot lag (ns) — **both columns stream a shuffle over many distinct keys, so
both are cache-realistic** (the earlier single-hot-key version was an L1-resident artifact; see note):

| lag (commits behind HEAD) | high-cardinality key — mvcc / hamt / art | churned set (depth ≈ 29) — mvcc / hamt / art |
|--------------------------:|-----------------------------------------:|---------------------------------------------:|
| 0 (true HEAD) | 30 / 62 / 57 | 11 / 34 / 12 |
| 100 | 33 / 61 / 58 | 25 / 34 / 12 |
| 1000 | 46 / 53 / 39 | **68** / 31 / 15 |
| 1999 | 49 / 23 / 9 | **126** / 18 / 6 |

- **The tries are flat in lag** — a snapshot read is `root[snapshot]`, identical cost to HEAD however far
  back (they even get *faster* at extreme lag, because an old snapshot's tree is smaller).
- **The multiversion hash climbs with lag × churn.** A churned key's read grows from 11 ns at HEAD to
  126 ns at 2 000 behind — and the slope steepens with churn depth: at depth ≈ 118 (a more frequently
  rewritten set) the same sweep reads **9 → 285 → 642 ns**. For a high-cardinality key (rarely rewritten)
  the chain stays short, so lag barely hurts (~30–50 ns) — that is the slice where the hash's fast read
  survives concurrency.
- **At HEAD the churned read is cheap for everyone** (mvcc 11 ≈ art 12 < hamt 34 — the churned set is a
  small, warm working set). The hash's penalty is *entirely* the lagging-snapshot walk, not the read itself.

So the hash's read advantage holds only **at/near HEAD, or for non-churning keys**. Under snapshot
isolation with submit activity, reads of churned keys fall into the as-of regime where the hash crosses
over to *slower* than both tries by roughly lag 100–1000, and the gap widens with churn.

> **Note on the single-key artifact.** An earlier version read *one* hot key repeatedly, which kept its
> whole path in L1 and removed all cache misses — so those numbers (mvcc 7 → 3 599 ns; art a flat 3 ns)
> measured *compute*, not memory, and aren't comparable across structures. The benchmark still prints that
> single-key line, explicitly labeled "COMPUTE-only", because it cleanly isolates the as-of *slope* and
> shows ART's no-hashing edge with misses removed. The table above is the cache-realistic replacement.

## Read/write locking

The benchmark above is single-threaded, so it does not *measure* contention — but the structures'
locking requirements differ in a way worth stating precisely:

- **Full retention removes the reader's lock.** Because no version is ever freed, the multiversion hash's
  reads can be **lock-free and wait-free**: a writer only *prepends* at the chain head, never mutates an
  existing node's `older` pointer, so a reader that atomic-loads the head walks a stable, never-freed
  suffix. No hazard pointers / epoch reclamation are needed *because* nothing is reclaimed. So "read/write
  locking" is, surprisingly, not the multiversion hash's reader problem here — the as-of walk above is.
- **What writers still pay:** concurrent writers contend on **per-slot chain heads** (a CAS each; a
  *hot key* turns every writer onto the same slot → a CAS-retry storm), and the table **resize** is the
  genuine lock-free hazard — growing an open-addressed table moves every element, so a reader mid-probe
  needs the old table kept alive (reclamation) or a lock during the swap. The `index_sim` cost model
  *derives* the writer-contention curve (global mutex grows linearly in writers; striped/lock-free stay
  flat until a hot key); that part remains modeled, not yet measured.
- **The persistent tries** make a reader hold a root pointer (one atomic load) and traverse immutable
  nodes — **wait-free, no reclamation, and no as-of cost.** Writers serialize only at the single
  root-publish (one CAS per commit, or a serialized commit), never per key, and there is no global resize
  (the tree grows by path-copy). This is why a versioned store built on snapshots leans naturally to a
  persistent structure: the thing readers do constantly — read a snapshot under concurrent writes — is
  the tries' free case and the multiversion hash's expensive one.

A real multi-threaded harness (writer threads submitting while readers read, measuring CAS-retry cost on
hot keys and reader latency during a resize) is the natural next measurement; it would put numbers on the
writer-contention and resize costs that are argued above and modeled in `index_sim`.

## Revised bottom line

The microbenchmark **upholds the report's two structural conclusions** — persistent tries crush the
multiversion hash on time-travel, and ART's no-hash advantage is real for long keys — while **demoting
the point-read claim**: for short keys the multiversion hash is the fastest reader, and ART's edge
appears only past ~48-byte keys.

It also surfaces ART's genuine cost that the cost model hid: **under copy-on-write, ART's wide nodes
make writes and retained memory its worst axis**, not its best. So the practical recommendation sharpens:

- **Reach for a persistent radix trie when the column has long string keys, needs ordered/prefix scans,
  or is read across history** — its read scales flat in key length and its time-travel is free. Pair it
  with **transient (folded) commits** to contain the write/memory cost the benchmark exposed; do not run
  it functional.
- **The multiversion hash is the fastest reader only *at HEAD* and only for short, non-churning keys** —
  and the lightest writer. But under snapshot isolation with concurrent submits, reads land in the as-of
  regime; a churned key on a far-behind snapshot crosses over to *slower* than both tries (cache-realistic:
  ~7× at chain depth 29, ~40–90× at depth 118 by a 2 000-commit lag, and it keeps growing with lag × churn).
  Its disqualifiers are time-travel / snapshot-lag and the reclamation-vs-retention conflict — not raw
  point-read speed.
- **Validate the cache regime before trusting any of these latencies.** For a graph whose live index
  fits in L3, everything is 5× faster than the model and the structures are within ~2× of each other on
  reads; the gaps the model drew at 80 ns/hop only open up once the index goes to DRAM.

## Files

- `index_bench.cpp` — the prototypes + harness (`--help` for flags: `--keys --writes --parts --key-len
  --reads --branch`).
- `report_hamt.md` / `report_hamt.html` — the cost-model study this validates.
- `index_sim.cpp` — the cost model itself.
