# ART String-Property Index — Design Specification

This document specifies the design of TuringDB's **string-property index**: a versioned, in-memory
**Adaptive Radix Tree (ART)** used to map string property values to the entities/rows that hold them, so
that `MATCH (n) WHERE n.prop = "…"` resolves by index lookup instead of a scan-and-filter (see
[`indexing.md`](indexing.md) for the surrounding index framework).

The design is settled. The chosen read path is:

> **ART + SIMD Node16 search + leaf-fingerprint miss-reject + full-leaf verify + AMAC 8-way batched
> probing**, persistent (copy-on-write with structural sharing), with path compression + lazy expansion,
> and transient/folded commits.

Every clause of that sentence is a deliberate, measured choice; this document states each one and why.

## Table of Contents

1. [Why ART](#1-why-art)
2. [Scope — when this index applies](#2-scope--when-this-index-applies)
3. [Data structure](#3-data-structure)
4. [Read path](#4-read-path)
5. [Versioning / MVCC](#5-versioning--mvcc)
6. [Correctness invariants](#6-correctness-invariants)
7. [Measured performance](#7-measured-performance)
8. [Rejected alternatives](#8-rejected-alternatives)
9. [Implementation requirements](#9-implementation-requirements)
10. [References](#10-references)

---

## 1. Why ART

TuringDB is column-oriented, versioned (git-like commits with snapshot isolation), and read-intensive.
A string-property index must therefore serve, on top of fast point lookups:

- **Reads of historical snapshots** (a session opens a snapshot, others advance HEAD) — must be cheap
  regardless of how far behind HEAD the snapshot is.
- **Ordered / range / prefix scans** (`STARTS WITH`, range predicates, `ORDER BY`).
- **Lock-free concurrent reads** under concurrent writers.

A radix trie delivers all three intrinsically: a snapshot read is just "read an older retained root"
(flat in lag, free time-travel), iteration is in key order, and a reader holds an immutable root pointer
so it never blocks. The one place a trie is naively weak — short-key point-lookup latency — is closed by
the optimizations below; measured, an optimized ART **beats the multiversion hash on point reads — hits
*and* misses** (§7), the latter once the leaf fingerprint (§3.4) gives the trie the hash's short-circuit on
absent keys. ART specifically (vs a plain radix tree) keeps memory bounded via adaptive node sizing.

## 2. Scope — when this index applies

**Use this index for** string-property columns, across the full cardinality range, for HEAD and historical
reads, and for point **and** range/prefix access.

**Do not** assume it dominates everywhere. The multiversion hash retains one advantage (§7, §8):

- **Write-simplicity-dominated paths** — the hash's append-a-version write is simpler/lighter than ART's
  copy-on-write.

It no longer wins **misses**. An absent key once forced the trie to descend to a leaf before its verify
failed (the hash short-circuits at an empty slot); the leaf fingerprint (§3.4) now rejects it at the leaf
edge without the leaf load, so ART beats the hash on misses too (§7).

For dictionary-encoded (dense integer code) keys, the trade-offs change again and a hash on the code may
win; this spec covers the **string-keyed** index.

## 3. Data structure

A persistent ART over byte-string keys. Inner nodes are one of four adaptive sizes; a separate leaf type
holds the full key and the payload.

### 3.1 Node types

All inner nodes share a fixed header — `type`, `numChildren`, `prefixLen`, and an inline `prefix[]` byte
buffer (pessimistic path compression, see §3.3). They differ only in how the next byte → child mapping is
stored:

| Node | Children | Child lookup | Notes |
|------|----------|--------------|-------|
| **Node4**   | 2–4   | up to 4 keys in a byte array, parallel child array | smallest branching node |
| **Node16**  | 5–16  | 16-byte key array, **searched by SIMD** (§4.2) | the key array is exactly one 128-bit register |
| **Node48**  | 17–48 | a 256-entry `childIndex[byte] → slot+1`, then a 48-pointer array | two indexed loads, no scan |
| **Node256** | 49–256| direct `child[byte]` | one indexed load, no scan |

Nodes grow/shrink between types as children are inserted/removed. The adaptive sizing is what bounds
memory (a sparse node never costs a full 2 KB Node256).

### 3.2 Leaf

A leaf stores the **full key** and the payload value. Storing the full key is mandatory: it is what makes
the index **absent-key-safe** — the descent only inspects *discriminating* bytes, so the leaf must confirm
the entire key on lookup (§4.3, §6). The value is the entity/row reference (or version-chain head under
MVCC, §5).

### 3.3 Path compression and lazy expansion

Both are **required**, not optional — they bound tree height for long or structured keys.

- **Lazy expansion** — an inner node exists only where it is needed to distinguish ≥2 keys; a unique key is
  a leaf directly, no chain of single-child nodes down to it.
- **Path compression (pessimistic)** — a run of single-child bytes is folded into the parent's inline
  `prefix[]`. When a shared prefix exceeds the inline buffer (`ART_MAX_PREFIX`), it is carried by a **chain
  of prefix nodes**, each holding up to `ART_MAX_PREFIX` prefix bytes plus one branch byte.

Measured effect (40-byte shared prefix, §7): **44 dependent loads → 6**. This is the difference between a
usable and an unusable index on long/structured keys.

> **Known prototype bug, fixed:** the `index_bench.cpp` prototype caps the inline prefix at 16 bytes and
> *mis-branches* once a shared prefix exceeds it (its keys never did, so it was latent). The production
> design **must** chain prefix nodes past the cap; `index_opt.cpp` implements the fix.

### 3.4 Leaf fingerprint (miss-reject)

Each **leaf edge** — a child slot whose target is a leaf — carries a small fingerprint of the leaf's full
key (a 16-bit CRC32), stored in the child pointer's spare bits: the top 16 bits (x86-64 user-heap pointers
are 48-bit) hold the fingerprint, and bit 0 (heap nodes are ≥8-aligned) marks the child as a leaf. It costs
**no extra memory and no extra load** — the pointer is already fetched from the (warm) parent during the
descent.

It exists for one reason: **misses**. A prefix-colliding absent key descends to the present leaf it
collides with and only the full-key verify fails, so it pays the cold leaf load for nothing (§7). Before
chasing a leaf the descent compares the query key's fingerprint to the stored one; a **mismatch** proves
the key absent (different fingerprint ⇒ different key) and returns `NOT_FOUND` **without the leaf load** —
the short-circuit the hash had and the trie lacked. It is **not** value-in-pointer (§8): on a fingerprint
**match** the leaf is still loaded and the full key still verified, so the §6.1 invariant is untouched — it
is a pre-filter, never a replacement for the verify (§4.5). The fingerprint is the *whole* key's, so it is
position-independent: it rejects a divergence anywhere, not only in a chosen byte.

## 4. Read path

### 4.1 Descent (point lookup)

```
lookup(root, key):
    cur ← root ; depth ← 0 ; qfp ← fingerprint(key)            # one CRC32 of the whole key (§3.4)
    loop:
        if cur is Leaf:
            return (cur.key == key) ? cur.value : NOT_FOUND     # full-key verify (§4.3)
        if cur.prefixLen > 0:                                   # path compression
            if key[depth : depth+cur.prefixLen] != cur.prefix: return NOT_FOUND
            depth += cur.prefixLen
        slot ← findChild(cur, key[depth])                       # SIMD for Node16/4 (§4.2)
        if slot is empty: return NOT_FOUND
        if slot is a leaf edge and fingerprint(slot) ≠ qfp: return NOT_FOUND   # miss-reject (§3.4), no leaf load
        cur ← *slot ; depth += 1
```

For random 16-byte keys the descent is ~4 dependent loads (Node256 → Node256 → Node16 → leaf).

### 4.2 SIMD Node16 search

`findChild` on a Node16 must use a SIMD equality search, **not** a linear scan. The 16 child bytes are one
128-bit register: broadcast the query byte (`_mm_set1_epi8`), compare all 16 lanes in one instruction
(`_mm_cmpeq_epi8`), extract a 16-bit match mask (`_mm_movemask_epi8`), mask to the valid child count, and
take the first match (`tzcnt`). Node4 is vectorised the same way; Node48/Node256 are already directly
indexed and need no search.

This is the **single highest-leverage optimization**: the linear scan's data-dependent loop mispredicts
~1 branch per lookup, and that branch sits on the critical path to issuing the next dependent load — so
removing it both deletes the mispredict and unblocks load pipelining. Measured: ~1.10 → 0.07
branch-misses/lookup, and the point read more than halves (§7).

### 4.3 Full-leaf verify (absent-key safety)

The descent matches only the *discriminating* bytes; under lazy expansion the leaf is reached before the
whole key is consumed. The leaf lookup **must compare the full key**. This is non-negotiable for
correctness: an absent key that shares a present key's discriminating prefix descends to that present
leaf, and without the compare it would return the present key's value — a **false positive**. (Measured:
without the verify, 200000/200000 prefix-colliding absent lookups returned a wrong value.) The leaf
fingerprint (§3.4, §4.5) rejects almost all such absent keys *before* this load, but never replaces the
compare: it is a pre-filter, and a fingerprint match still runs the full verify.

### 4.4 AMAC 8-way batched probing

Point reads from analytical queries arrive in **batches** (join probes, `IN` filters, key-column scans).
For these, the index exposes a **batched lookup** that runs ~8 independent descents interleaved in
software-pipelined (AMAC) fashion: each round advances every in-flight lane one level and prefetches its
next node, so the dependent-load latency of one lane is hidden behind the others' work.

A trie descent is latency-bound (a serial chain of dependent loads), so it benefits enormously from this —
the batched path roughly halves the per-lookup cost (§7). **Scalar per-key loops must not be used at
batch-probe sites.** Single-key lookups still use the serial path (§4.1).

### 4.5 Leaf-fingerprint miss-reject

The fingerprint (§3.4) is checked on the descent, at the leaf edge, before the leaf load. It is **sound**:
0 false-negatives (a present key always equals its own stored fingerprint, so it always reaches the verify)
and 0 false-positives (the full-key verify on a fingerprint match is preserved — §4.3, §6.1). It affects
*speed*, never *correctness*. The query fingerprint is one CRC32 of the whole key, computed once per lookup
(≈12 cyc, mostly hidden under the descent's loads in the batched path); a fixed-position byte tag would be
free but fragile (a divergence outside the chosen byte slips past), so the full-key CRC is used. Measured:
it collapses the serial miss 152 → 53 cyc and the batched miss 90 → 40 cyc, taking ART past the hash on
misses (§7).

## 5. Versioning / MVCC

The index is **persistent (functional copy-on-write)**:

- Each commit retains a **root pointer**; a write path-copies only the O(log₂₅₆ n) nodes from the root to
  the changed leaf and **shares** all untouched subtrees with prior versions.
- A snapshot read is `root[snapshot]` — identical cost to a HEAD read, **flat in lag**. There is no
  as-of / version-chain walk (the multiversion hash's structural wound).
- **Readers are lock-free and wait-free:** a reader atomically loads a root pointer and traverses
  immutable nodes. Writers publish a new version with a single root swap; no reader ever blocks, no
  reclamation is needed while versions are retained.
- **Transient / folded commits are required** to contain write cost: apply a commit's writes in place
  against a private, not-yet-published version (no node is shared yet), and only path-copy when a node is
  actually shared with a published version. This makes batched commit cost approach in-place mutation and
  avoids cloning wide upper nodes (the 2 KB Node256 copy-on-write cost) on every write.
- **The leaf fingerprint (§3.4) needs no separate versioning.** It is immutable node payload carried in the
  child pointer, so a path-copy carries it for free and a value update (same key) leaves it unchanged —
  unlike a front Bloom/filter, which would be a second versioned structure (§8).

## 6. Correctness invariants

1. **Full-key verify at the leaf on every lookup** (§4.3). Value-in-pointer / leaf elision that skips the
   suffix verify is **prohibited** in the production read path (it is present-key-only — see §8).
2. **Prefix chaining past the inline cap** (§3.3): a shared prefix longer than `ART_MAX_PREFIX` is carried
   by chained prefix nodes; the discriminating byte is taken at the correct depth.
3. **Node-type growth/shrink preserves the byte→child mapping** across N4→N16→N48→N256 transitions.
4. **Versions are immutable once published**; a writer never mutates a node reachable from a published
   root (transient writes happen only on unshared nodes — §5).
5. **The leaf fingerprint is a pre-filter only** (§3.4, §4.5): it may reject a key only on a fingerprint
   *mismatch*; a fingerprint *match* must still run the full-key verify (invariant 1). It never returns a
   value on the strength of the fingerprint alone.

## 7. Measured performance

All figures are from `tools/index-sim/` (`index_opt.cpp`, `index_decompose.cpp`, `index_miss.cpp`),
high-cardinality, 16-byte keys, warm (L2/L3-resident) working set, on an Intel Core Ultra 7 265.
**Cycles/op is the metric** (the box scales 0.8–5.2 GHz, so ns wobbles); all variants are absent-key-safe
(full-key verify, 0 false-positives) unless noted. HIT = present key, MISS = absent key sharing a present key's prefix.

| variant (all verify the full key) | HIT (cyc) | MISS (cyc) |
|------------------------------------|----------:|-----------:|
| Multiversion hash, serial          | 171 | 75 |
| Multiversion hash + AMAC 8-way     | 140 | 73 |
| ART + SIMD + leaf-verify, serial   | 145 | 152 |
| ART + SIMD + leaf-verify + AMAC 8-way | 82 | 90 |
| ART + SIMD + leaf-verify + fingerprint, serial | 155 | 53 |
| **ART + SIMD + leaf-verify + fingerprint + AMAC 8-way** (this design) | **87** | **40** |

- **On positive point reads the chosen design beats the multiversion hash fairly and absent-safe — 82 vs
  140 cyc batched (~1.7×), 145 vs 171 serial.** The hash must digest the whole key per probe (FNV ≈ 94 cyc,
  ALU work AMAC cannot hide); ART hashes nothing and AMAC hides its dependent-load latency.
- **The leaf fingerprint (§3.4) wins misses back** — serial 152 → 53, batched 90 → 40 — taking ART past the
  hash on misses too (53 vs 75 serial, 40 vs 73 batched). It rejects a colliding absent key at the leaf edge
  without the cold leaf load (the entire miss cost). On *random* (non-colliding) absent keys ART already
  beat the hash — it short-circuits in a few warm upper hops with no hashing — so the hash's miss edge was
  only the prefix-colliding tail, which the fingerprint removes. **With it, ART dominates the hash on every
  point-read cell — hit and miss, serial and batched.**
- **Fingerprint hit tax:** the query CRC costs ≈12 cyc/key; on a hit there is no leaf load to skip, so it is
  pure overhead (serial +~10, batched +~5, the latter mostly hidden by AMAC) — ART still wins hits with it
  on (87 vs 140 batched). Measured: 0 false-positives on every absent distribution, and 100% of colliding
  absent keys rejected before the leaf load.
- **Optimization attribution** (from the unoptimized 300-cyc baseline): SIMD Node16 alone 300→132 (kills
  the branch mispredict); AMAC alone 300→109 (hides latency); together with leaf-verify, 82.
- **Path compression**: long keys with a 40-byte shared prefix — 44 → 6 dependent loads, 485 → 271 cyc.
- **Cache-regime caveat:** these gaps hold for a working set in L2/L3. If the live index exceeds cache
  (DRAM-bound), every structure is ~5× slower and the relative gaps shrink — validate the regime before
  trusting absolute latencies.

## 8. Rejected alternatives

- **Value-in-pointer / pointer tagging (leaf elision)** — inlines the value in the parent's child slot,
  skipping the leaf load (present-key read ~65 cyc). **Rejected for production:** it omits the suffix
  verify, so it false-positives on every prefix-colliding absent key (200000/200000 measured). Restoring
  the verify costs the leaf load back (~145 cyc), erasing the gain. It is a present-key-only
  micro-optimization, not the design. (The *sound* use of the same spare-pointer-bits mechanism — storing a
  key *fingerprint* that still verifies, not a value that skips the verify — is adopted for miss-reject, §3.4.)
- **Front Bloom / quotient filter for misses** — would give the hash's short-circuit, but re-introduces a
  full-key hash on *every* lookup (the cost ART exists to avoid) and needs its own COW-versioned structure.
  The leaf fingerprint (§3.4) gets the same miss-reject locally, in the existing pointer, with no separate
  structure — so the filter is rejected in its favor.
- **Multiversion hash as the primary string index** — simplest write, but loses positive-lookup latency
  once the key must be hashed (§7), no longer wins negative lookups either (the leaf fingerprint matches it
  on misses — §3.4, §7), and has no ordered/range/prefix access, pays an as-of version-chain walk for
  historical reads, and contends on writers/resize. Kept only as the routing target for **write-dominated**
  slices (§2).
- **HAMT / COW B-tree** — viable persistent tries, but ART's adaptive nodes + path compression give better
  point-read and memory behavior for this workload; a COW B-tree remains the better choice only where
  range scans dominate over point reads.
- **Verify-free full-key-consuming trie** — consuming the whole key removes the need to verify (absent-safe
  without a leaf compare), but deepens the tree; the prototype did not beat plain verify and is not adopted.
- **Height reduction (multi-byte stride / HOT-family wide nodes) as a production optimization** — consume
  several key bytes per level to cut descent height, and the dependent-load count that bounds the deep-DRAM
  serial point read. **Rejected as an ART change** (prototyped against the production arena-backed ART;
  `tools/index-sim/report_art_opt.md`, 2026-06-26 follow-up): the only stride that helps (S=4, ~1.6× at 1M)
  collapses the upper tree into a single ~1M-entry node keyed on the key prefix — a hash, not a radix tree —
  forfeiting the ordered/range/prefix scans and adaptive node sizing the ART exists for; a *moderate* stride
  (S=2) gives almost nothing and S=8 is worse than S=4. Route point-read-only-at-scale slices to the
  multiversion hash (§2) instead of widening the ART's stride.

## 9. Implementation requirements

A conforming implementation must:

1. Implement adaptive Node4/16/48/256 with header-carried `prefix[]`, plus a full-key Leaf (§3).
2. Search Node16/Node4 with SIMD (SSE2 baseline; AVX2 where available) — never a linear scan (§4.2).
3. **Verify the full key at the leaf on every lookup** (§4.3, §6.1). No value-in-pointer.
4. **Carry a per-leaf-edge fingerprint of the full key** (e.g. a 16-bit CRC32 in the child pointer's spare
   bits) and reject an absent key on a fingerprint *mismatch* before the leaf load; on a *match*, still run
   the full-key verify (§3.4, §4.5, §6.5).
5. Provide an **AMAC-style batched lookup** API and use it at all batch-probe sites; reserve the scalar
   path for single lookups (§4.4).
6. Implement path compression + lazy expansion, **chaining prefix nodes past the inline cap** (§3.3, §6.2).
7. Be persistent (copy-on-write, retained root per commit, structural sharing) with **lock-free readers**
   and **transient/folded commit** application (§5).
8. Route only **write-dominated** string-property slices to the multiversion hash; point-read routing
   (including miss-heavy) is no longer needed — ART wins hits and misses (§2, §7).

## 10. References

- `tools/index-sim/report_art_opt.md` — implements and measures every optimization here (SIMD, value-in-
  pointer, AMAC, path compression, HOT/Masstree-family stride), plus the absent-key-safe comparison table.
- `tools/index-sim/report_art_miss.md` + `index_miss.cpp` — implements and measures the leaf fingerprint
  (§3.4): the miss-reject that takes ART past the hash on absent keys, with the false-positive / soundness audit.
- `tools/index-sim/report_art.md` + `index_decompose.cpp` — first-principles decomposition of why an
  unoptimized ART trailed the hash, and the key-length crossover.
- `tools/index-sim/report_bench.md` + `index_bench.cpp` — the original wall-clock validation (time-travel,
  submit/memory cost) the above build on.
- `tools/index-sim/index_opt.cpp` — the reference prototype for the chosen read path and the fix in §3.3.
- [`indexing.md`](indexing.md) — the generic index framework this plugs into.
- Literature: Leis et al., *The Adaptive Radix Tree* (ICDE 2013); *The ART of Practical Synchronization*
  (DaMoN 2016, lock-free readers); Binna et al., *HOT* (SIGMOD 2018); Mao et al., *Masstree* (EuroSys
  2012); Kocberber et al. / Zeitak & Morrison *Cuckoo Trie* (SOSP 2021, AMAC/MLP).
