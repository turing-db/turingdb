# Implementing the known ART read optimizations — and measuring them

`report_art.md` decomposed ART's ~2× HEAD-read deficit (high-cardinality, 16-byte keys, low lag) into
named causes and surveyed the literature's speed-ups. This report **implements those speed-ups for real**
and measures each one, isolated, on the same workload. The harness is `index_opt.cpp`
(`g++ -std=c++23 -O2 -march=native -o index_opt index_opt.cpp && taskset -c 0 ./index_opt`), built on the
same `MvccHash` / `Art` / workload as `index_bench.cpp`, with the same inline hardware counters. Cycles/op
is the primary, frequency-invariant metric (the box scales 0.8–5.2 GHz); ns ≈ cyc / 5.2. Numbers below are
the median of 3 pinned runs; correctness is cross-checked (every variant's probe checksum must match).

## What was implemented

| technique | source | fidelity |
|-----------|--------|----------|
| **SIMD Node16 search** | Leis et al., ICDE 2013 §III-C | faithful — SSE2 `_mm_cmpeq_epi8` + `movemask` + `tzcnt`, replacing the linear scan (N4 vectorised too) |
| **Value-in-pointer / pointer tagging** | Leis §III-D; DuckDB | faithful — leaf children become tagged 8-byte values in the parent slot (no leaf node); see correctness note |
| **Path compression + lazy expansion** | Leis et al., ICDE 2013 | faithful — the ART already has it; measured against a no-compression ablation (`insertNC`, one node per byte). *Also fixed a latent bug:* the original capped the inline prefix at 16 B and mis-branched past it; now chains prefix nodes |
| **AMAC / MLP** | Kocberber 2015; Cuckoo Trie, SOSP 2021 | faithful — 8-way software-pipelined descent, prefetch each lane's next node |
| **HOT-family height reduction** | Binna et al., SIGMOD 2018 | *representative* — a compact stride-trie (consume S bytes/level, lazy expansion, compact open-addressed nodes). Captures "fewer, wider, cache-dense nodes"; not bit-granular discriminative bits |
| **Masstree-family slices** | Mao et al., EuroSys 2012 | *representative* — the same stride-trie at S=8 (8-byte slices). Captures slice-based descent; not the full ordered B+tree-of-tries |

## Workload A — high-cardinality 16-byte keys (the case ART "lost" by ~2×)

Descent shape unchanged from the prior report: ART baseline = 4.0 dependent loads (N256→N256→N16→leaf);
MVCC = 2. cyc/op, median of 3:

```
  variant                                          | cyc/op | ins | CPI  | LLC/op | br.miss | vs ART base
  -------------------------------------------------+--------+-----+------+--------+---------+------------
  MVCC baseline                        (reference) |   175  | 174 | 1.00 |  ~0.4  |  0.065  |
  ART baseline                                     |   300  | 152 | 1.99 |  ~0.4  |  1.10   |   1.0x
  ART + SIMD Node16                                |   132  | 143 | 0.93 |  0.15  |  0.067  |   2.3x
  ART + value-in-pointer (tagged leaf)             |   184  | 129 | 1.43 |  0.04  |  1.09   |   1.6x
  ART + SIMD + value-in-pointer                    |    65  | 118 | 0.55 |  0.04  |  0.062  |   4.6x
  stride-trie S=8  (Masstree-family, compact)      |   119  | 158 | 0.75 |  0.07  |  0.40   |   2.5x
  stride-trie S=4  (compact)                       |   100  | 126 | 0.79 |  0.06  |  0.41   |   3.0x
  stride-trie S=2  (HOT-family, compact)           |   146  | 160 | 0.91 |  0.06  |  0.72   |   2.1x
  ART + AMAC 8-way pipelined                       |   109  | 205 | 0.53 |  ~0.2  |  1.39   |   2.8x
  ART + AMAC + SIMD + value-in-pointer  (ALL)      |    43  | 150 | 0.28 |  0.04  |  0.092  |   7.0x
  MVCC + AMAC 8-way                    (reference) |    63  | 109 | 0.59 |  ~0.2  |  0.097  |
```

**The headline: the ~2× deficit was an artifact of an unoptimized ART, and it reverses.**

1. **SIMD Node16 alone is the single biggest lever: 300 → 132 cyc (2.3×), and it alone puts ART (132)
   ahead of the multiversion hash (175).** It works by killing the branch: the N16 *data-dependent linear
   scan* mispredicted ~1.0 branch/lookup (total 1.10 → 0.067, essentially gone). The win (168 cyc) is far
   larger than a naive "1 mispredict × ~18 cyc" — because that mispredicted branch sat **on the critical
   path to issuing the next dependent load**, so removing it both deletes the misprediction *and* unblocks
   the load pipeline (CPI 1.99 → 0.93). This is the cleanest result in the study: the prior report's factor
   3 (branch misprediction) was undervalued — it was gating factor 1 (the dependent loads).

2. **Value-in-pointer alone: 300 → 184 cyc (1.6×).** It eliminates the cold leaf load — LLC misses
   collapse 0.4 → 0.04 — but the N16 scan's branch mispredict remains (1.09), capping the gain. Removes the
   prior report's factor 1 leaf load, confirming the leaf was the one real LLC miss.

3. **SIMD + value-in-pointer (both, still serial): 300 → 65 cyc (4.6×)** — now **2.7× faster than the hash
   baseline** and tied with the fully-pipelined hash. The two are complementary: SIMD removes the branch
   stall, tagging removes the cold leaf load, and CPI collapses to 0.55.

4. **AMAC pipelining alone: 300 → 109 cyc (2.8×)** — the latency-hiding result from the prior report,
   reproduced.

5. **Everything stacked (AMAC + SIMD + value-in-pointer): 300 → 43 cyc (7.0×)** — CPI 0.28, **1.5× faster
   than the fully-optimized hash (63) and ~4× faster than the hash baseline (175).**

> **Correctness note on value-in-pointer.** A radix descent here discriminates the key in ~3 bytes, so 13
> bytes are un-verified at the leaf. The tagged-value variants return the value **without verifying the
> un-discriminated suffix** — correct for *present-key* lookups (the checksums match) but it would give
> false positives on absent keys. So the 65 / 43 cyc figures are the present-key upper bound. The
> **absent-key-safe** optimized numbers are the ones that still verify: **SIMD-only 132 cyc** (loads and
> compares the leaf) and **stride-trie S=4 100 cyc** (verifies at its leaf) — *both already beat the hash
> baseline of 175.* A correct value-in-pointer needs an inline suffix/fingerprint (DuckDB inlines the
> single rowid; cf. `report_art.md` idea #4), which costs one verify load back.

### Safe point read — the multiversion hash vs the absent-key-safe ART variants

Value-in-pointer skips verifying the un-discriminated key suffix, so it is only correct for present keys
(every one of the 200 k absent-near lookups — a present key with one suffix byte changed — got a *wrong*
value). The hash, by contrast, is inherently absent-safe: it compares the full key on every probe. So the
fair comparison is the hash against the ART variants that **also** verify. Both columns are measured with
**hashing inside** for the hash (a hash index must digest the query key) — note this makes the hash's AMAC
number ~140, not the ~63 from the present-only table, which used a *precomputed* hash. HIT = present,
MISS = absent-near; cyc/op, median of runs; all safe variants confirmed 0 false-positives:

```
  variant (all verify the full key unless noted)   |  HIT  |  MISS | false-pos /200k
  -------------------------------------------------+-------+-------+----------------
  MVCC hash, serial (hash + probe + key compare)   |  171  |   75  | 0
  MVCC hash + AMAC 8-way (hash inside)             |  140  |   73  | 0
  ART + SIMD + leaf-verify, serial                 |  145  |  152  | 0
  ART + SIMD + leaf-verify + AMAC 8-way            |   82  |   90  | 0
  ART full-consume stride S=8 (verify-free)        |  160  |  148  | 0
  ......................................................... UNSAFE contrast ........
  ART + SIMD + value-in-pointer (NO verify)        |   66  |   47  | 200000  ← all WRONG
```

- **Fair and safe, ART still wins on hits — serially (145 < 171) and more so batched (82 < 140).** With the
  hash digested inside (the honest cost), the hash's per-byte FNV (~94 cyc) is its dominant term and AMAC
  cannot hide it (it is ALU work, not a memory stall), so MVCC+AMAC only reaches 140. ART has no hash, so
  AMAC takes it to 82 — **~1.7× faster than the hash, absent-safe.**
- **The hash's real edge is misses.** An absent key scatters to a mostly-empty/mismatching slot, so the hash
  short-circuits at the probe: **MISS 73–75 cyc, cheaper than its own hit.** ART pays the opposite — an
  absent-near key shares the discriminating prefix, so ART descends all the way to the present leaf and only
  then fails the compare: **MISS 152 cyc serial (≈ its hit).** So on a **miss-heavy** workload (many absent
  lookups that collide on prefixes — e.g. existence checks that usually fail) the hash's cheap negative
  lookup is a genuine advantage; AMAC narrows it (ART miss 90 vs hash 73).
- **Verifying costs the leaf load back: value-in-pointer 66 → 145 cyc (serial).** For 16-byte keys the cost
  is the leaf *cache-line load*, not the compare, so an inline fingerprint would not help (still a cache-line
  load; fingerprints pay off only for long stored keys). The verify-free full-consume stride (160 cyc) does
  not beat plain verification here — its open-addressed hash nodes (a multiply-mix + probe per level) are
  slower than ART's direct-indexed arrays; the concept could approach the unsafe speed on ART-quality nodes,
  but this prototype does not show it.
- **Correction to the present-only headline:** the earlier 43 / 65 cyc "ALL" / "stacked" ART figures were
  *present-key, no-verify* (value-in-pointer) — and the 63 cyc MVCC+AMAC used a *precomputed* hash. Put both
  on a fair, absent-safe footing and the matchup is **ART 82 vs hash 140 batched, ART 145 vs hash 171
  serial** — ART still ahead, but by less, and the hash reclaims the miss-heavy case.

**Stride tries refute the prior report's wide-root negative result.** The earlier "collapse the upper hops
into a 65 536-entry array" failed because a 512 KB flat array is cache-sparse (LLC misses jumped to ~0.8).
The **compact** stride tries — open-addressed nodes sized to their children — do the same height reduction
with **LLC misses of 0.06**, and all three beat both ART baseline and the hash. There is an **optimum
stride ≈ 4** (100 cyc): S=8 collapses to a single hop but its lone root node is large and colder; S=2 stays
tiny but pays 2 hops + more in-node work. This is `report_art.md`'s idea #1 ("compact, not array")
confirmed, and it locates the knob: stride 4, not 2 or 8.

## Workload B — 56-byte keys with a 40-byte shared prefix (path compression / lazy expansion)

Random 16-byte keys diverge in byte 1, so path compression does nothing there; this workload exercises it.

```
  [path-compressed ART]   descent = 6.0 dependent loads   (N4,N4,N256,N256,N16,leaf)
  [no compression]        descent = 44.0 dependent loads  (one N4 per prefix byte, then branch)

  variant                                          | cyc/op | ins  | CPI  | LLC/op | br.miss
  -------------------------------------------------+--------+------+------+--------+--------
  ART path compression + lazy expansion (ON)       |   271  |  317 | 0.85 |  ~1.0  |  1.6
  ART NO compression (one node per byte)           |   485  | 1323 | 0.37 |  0.06  |  2.1
  ART compressed + SIMD                            |   264  |  309 | 0.85 |  ~0.9  |  0.6
  stride-trie S=8 (8-byte slices, NO compression)  |   321  |  636 | 0.51 |  0.07  |  0.7
```

- **Path compression collapses 44 dependent loads → 6** (the literature's "40 → 8 average height", measured
  as 44 → 6 here), **485 → 271 cyc (1.8×)** and **instructions 1323 → 317 (4.2×)**. Lazy expansion +
  compression are not optional for long/structured keys — they are the difference between a 6-deep and a
  44-deep tree.
- **Stride-widening and path-compression are orthogonal.** The S=8 stride trie has no path compression, so
  it pays a hop per 8-byte slice *through the shared prefix* (6 hops) and lands at **321 cyc — worse than
  the compressed ART (271).** Wider strides help when the discriminating bytes are spread out; path
  compression helps when there is a long shared run. A real index wants **both** (this is exactly what HOT
  and Masstree do — wide nodes *and* prefix/slice skipping).
- SIMD adds little here (264 vs 271) — only one N16 level exists on this workload, so there is almost no
  linear scan to vectorise.

## Bottom line

The "ART is ~2× slower than the multiversion hash" result was **specific to an unoptimized prototype** — the
one `report_bench.md` built, with a linear N16 scan and a separate leaf node, i.e. exactly the two costs the
literature's first two optimizations remove. Implemented and measured:

- **SIMD Node16 alone flips the verdict** (132 vs 175 cyc) by eliminating the branch mispredict that was
  also stalling the load pipeline — the prior report's most underweighted factor.
- **SIMD + value-in-pointer → 65 cyc** (present-key), or **stride-trie S=4 → 100 cyc / SIMD-only → 132 cyc**
  if absent-key verification is required — all faster than the hash.
- **+ AMAC batching → 43 cyc**, 4× the hash baseline, for the batched-probe path an analytical engine
  naturally has.

So for TuringDB's high-cardinality, short-key, HEAD-read slice, a *properly optimized* ART is not a ~2×
loser — it is faster than the multiversion hash **and** keeps the trie's structural wins (flat-in-lag reads,
ordered/range/prefix scans, free time-travel). The earlier recommendation to route this slice to the hash
should be revisited: the hash's remaining edge is its *write* simplicity and lock-freedom, not point-read
latency. For long/structured keys, path compression (44 → 6 loads) is mandatory and the hash's per-byte
digest loses outright (cf. the key-length crossover in `report_art.md`).

## Production follow-up (2026-06-26): arena kept, height reduction discarded

These optimizations were folded into the production `db::AdaptiveRadixTree` (SIMD Node16, leaf fingerprint,
full-leaf verify, AMAC `findBatch`, path compression). Two further ideas were then tried against it on this
box (Google Benchmark, high-cardinality 16-byte keys; the ART measured is the absent-safe SIMD + fingerprint
+ verify variant), aimed at the one cell where it still trailed the multiversion hash: the **serial
single-key hit at 1M keys** (DRAM-bound).

**Arena allocation — kept.** Nodes and leaves now bump-allocate from a per-tree `BumpPtrAllocator` instead of
individual `new`, co-locating a tree's memory. Build dropped ~5.7× at 1M (no per-node malloc/free), batched
probes overtook the hash, and serial hits improved in the cache-resident range (e.g. 128K). It did **not**
move the 1M serial hit — that cell is bound by dependent-miss *count*, which locality does not change.

**Height reduction — discarded.** To attack the miss *count* we prototyped this report's stride trie against
the production arena ART (a Google Benchmark stride sweep linking `db::AdaptiveRadixTree`), sweeping the
stride to isolate the height effect (allocation scheme held constant). Serial HIT, M lookups/s
(g++/libstdc++ prototype, directional — compare within the table):

| structure | hops | 128K | 1M |
|-----------|-----:|-----:|---:|
| arena ART (1-byte, SIMD) | ~3–4 | 16.2 | 5.4 |
| stride trie S=2 | 2.0 | 10.7 | 6.2 |
| **stride trie S=4** | **1.0** | **18.8** | **8.8** |
| stride trie S=8 | 1.0 | 15.8 | 7.6 |
| multiversion hash | — | 10.6 | 5.6 |

This *confirms* the cell is miss-count-bound — but the fix is not an ART. Cutting 3→2 hops barely moved 1M
(5.4 → 6.2); the ~1.6× win arrives only at **1 hop** (S=4), and a 1-hop stride trie over 1M keys *is* a
single ~1M-entry node keyed on the first 4 bytes — a **hash on the key prefix**, not a radix tree. It
forfeits exactly what justifies the trie over the hash (ordered/range/prefix scans, adaptive node sizing;
the root becomes one ~32 MB node), and S=8 is already worse than S=4 (fatter, colder node). So the earlier
"optimum stride ≈ 4" knob, read at the production level, is the *degenerate* (hash) end of the spectrum:
there is no useful **moderate** height reduction here — you either stay a real ART (and accept the
serial-single-key-at-1M deficit) or become a hash, at which point the multiversion hash is the simpler
structure the design already routes point-read/write-dominated slices to.

**Verdict: discard height reduction as a production ART optimization.** After the arena the ART wins build,
batched probes (the recommended columnar path), misses, and warm hits, trailing only on the discouraged
serial-single-key path at deep-DRAM scale; closing that one cell is not worth degenerating the structure.
For a point-read-only-at-scale column, route it to the hash rather than widening the ART's stride. (The
hash-front + radix-tail *hybrid* of `report_art.md` idea #1 is a distinct structure — it keeps a radix tail
with ordered properties — and was not what this prototype tested.)

## Files
- `index_opt.cpp` — implements and measures all six techniques (`--keys N --parts P`).
- `report_art.md` / `index_decompose.cpp` — the decomposition this builds on.
