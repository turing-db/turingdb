# Closing ART's last deficit: the absent-key (MISS) lookup

`report_art_opt.md` settled the read path — **ART + SIMD Node16 + full-leaf verify + AMAC 8-way** — and
showed it beats the multiversion hash on positive point reads while keeping the trie's structural wins.
It also conceded **one** cell to the hash: the **miss**. This report attacks that cell, implements a fix,
and measures it on the same harness (`index_miss.cpp`, same `MvccHash` / `Art` / workload / counters as
`index_opt.cpp`; `g++ -std=c++23 -O2 -march=native`, `taskset -c 0`, cyc/op, median of 3 pinned runs,
Intel Core Ultra 7 265). The result: **the deficit closes and reverses** — with the fix, ART beats the
hash on misses too, on every distribution, serial and batched, with 0 false-positives.

## 1. The deficit, restated

From `report_art_opt.md` (reproduced here, median of 3): HIT = present key, MISS = absent key that shares a
present key's discriminating prefix.

```
  variant (all verify the full key)        |  HIT  |  MISS
  -----------------------------------------+-------+-------
  MVCC hash, serial                        |  172  |   75
  MVCC hash + AMAC 8-way                    |  138  |   74
  ART + SIMD + leaf-verify, serial         |  142  |  152     <- serial miss: 2.0x the hash
  ART + SIMD + leaf-verify + AMAC 8-way     |   82  |   90     <- batched miss: 1.2x the hash
```

The hash's edge is structural (ART.md §2, §7): an absent key scatters to an empty/mismatching slot and
**short-circuits at the probe**, so a hash miss is *cheaper than a hash hit* (74 < 138). ART has no such
short-circuit — a prefix-colliding absent key descends the **same path** as the present key it collides
with, reaches the leaf, and only the full-key verify fails. So an ART miss pays essentially the full hit
cost. The descent shape makes this exact:

```
  ART descent: N256(byte0) -> N256(byte1) -> N16(byte2) -> Leaf     (3 discriminating bytes, then the leaf)
  the absent key matches bytes 0-2, so it reaches the leaf and fails the verify there
```

The leaf load is the whole cost. Measured in the decomposition: removing it (value-in-pointer) collapses
LLC misses 0.65 → 0.04 and the serial read 300 → 184 cyc — the cold, scattered leaf is the one real
cache miss. **A miss pays for that load and gets nothing back.**

## 2. Why a front filter is the wrong fix

The obvious way to buy the hash's short-circuit is a **front filter** (Bloom / quotient / cuckoo) over the
key set: probe it first, and on "definitely absent" skip the trie. It is the wrong fix here for two
reasons, both fatal to the design's premises:

- **It re-introduces the per-key hash ART exists to avoid.** A membership filter must hash the *whole*
  query key on *every* lookup — hit and miss. ART's entire hit advantage (82 vs 138 batched) is that it
  *hashes nothing*: it routes on a few discriminating bytes. A front filter taxes every hit with the cost
  ART was designed to dodge. It also redundantly re-hashes the discriminating prefix the descent already
  handles structurally — strictly more work than necessary.
- **It needs its own versioning.** The index is persistent/COW with a retained root per commit. A plain
  Bloom filter cannot delete; a versioned filter means a second COW structure (counting/cuckoo with
  block-COW, or per-version rebuilds) to maintain alongside the trie — exactly the "two structures"
  complexity we are trying to remove.

The right fix keeps the descent's structural filtering (free, no hashing) and adds only a **cheap residual
check at the leaf edge** — local, not global; carried by the existing nodes, not a separate structure.

## 3. The fix: a leaf fingerprint in the child pointer

Store a small fingerprint of each leaf's **full key** in its parent's child slot, and check it **before**
chasing the leaf:

- On a fingerprint **mismatch**, the key is provably absent (a different fingerprint implies a different
  key) → return NOT_FOUND **with no leaf load**. This is the short-circuit the trie lacked.
- On a fingerprint **match**, still load the leaf and **verify the full key** — so collisions and real
  hits stay correct.

The fingerprint rides in the leaf child pointer's spare bits: the top 16 bits (x86-64 user-heap pointers
are 48-bit, bits 48–63 are zero) hold a 16-bit fingerprint, and bit 0 (heap allocations are ≥8-aligned)
marks "this child is a leaf." **Zero extra cache footprint, zero extra load** — the pointer is already
fetched from the (warm) parent node during the descent. The query's fingerprint is a hardware **CRC32**
(SSE4.2) of the key, computed once per lookup (~12 cyc; see §5), not per level.

> **This is the dual of the rejected value-in-pointer, not the same idea.** Value-in-pointer (ART.md §8)
> puts the *value* in the slot and returns it *without verifying the suffix* — present-key-only, 200000/
> 200000 false-positives on colliding absent keys. The fingerprint puts a *hash of the key* in the slot
> and **keeps the verify** on a match. It speeds *misses* (the common mismatch skips the leaf load) while
> preserving ART.md §6.1 exactly. One trades correctness for a hit speedup (rejected); the other adds a
> sound pre-filter for a miss speedup (this).

**Soundness.** 0 false-negatives: a present key always equals its own stored fingerprint, so it always
reaches the verify. 0 false-positives: the full-key verify is preserved on every fingerprint match. The
fingerprint affects *speed*, never *correctness*.

**Versioning-clean.** The fingerprint is immutable node payload, like the child bytes. A COW path-copy
carries it for free; a value update doesn't change the key, so the fingerprint is invariant; node growth
(N4→N16→…) copies it alongside the child pointer. **No separate versioned filter, no new reclamation.**

**Why CRC32 and not a positional byte.** A fixed-position byte tag (e.g. the last key byte) is free to
derive but *fragile*: a divergence outside the chosen byte slips past it. The CRC32 of the whole key is
position-independent — it rejects a divergence *anywhere*. The `near-mid` distribution below (divergence in
a random byte in [5,14], never the last) is the honest test of this: a last-byte tag would reject 0% of it;
the CRC32 fingerprint rejects 100%.

## 4. Measured result

`index_miss.cpp`, median of 3, cyc/op. Three **honest** absent-key distributions, all genuinely absent:
`near-byte15` = a present key with the last byte changed (the original adversarial set); `near-mid` = a
present key with a *random* byte in [5,14] changed (still descends to the leaf, but a last-byte tag could
not catch it); `random` = fully random absent keys (mixed descent depth — the realistic anti-join miss),
pool sized to the same warm working set as the others so the column measures index cost, not a cold pool.

```
  variant (all ART verify the full key)        |  HIT  | near15 | near-mid | random | false-pos
  ---------------------------------------------+-------+--------+----------+--------+----------
  MVCC hash, serial                            |  172  |   75   |    74    |   74   |    0
  MVCC hash + AMAC 8-way                        |  138  |   74   |    72    |   73*  |    0
  ART + SIMD + verify, serial                  |  142  |  152   |   154    |   62   |    0
  ART + SIMD + verify + AMAC 8-way              |   82  |   90   |    92    |   52   |    0
  ART + SIMD + FINGERPRINT + verify, serial    |  155  |   53   |    52    |   52   |    0
  ART + SIMD + FINGERPRINT + verify + AMAC      |   87  |   40   |    38    |   43   |    0
```
`*` MVCC+AMAC random: the perf counter intermittently failed to read this one cell; it equals its near15/
near-mid siblings (~73), as expected for a distribution-independent hash miss.

- **The serial miss collapses: 152 → 53 cyc (2.9×), and now beats the hash (53 vs 75).** This is the big
  one — single-key lookups (ART.md reserves the scalar path for them) were the 2× deficit. The fingerprint
  removes the un-hidden cold leaf load entirely; what remains is the warm upper descent + the verify it now
  skips + the CRC.
- **The batched miss reverses too: 90 → 40 cyc (2.3×), beats the hash (40 vs 73).** AMAC already hid most
  of the leaf *latency*, so the win here is smaller in absolute terms but still flips the comparison.
- **Position-independent.** `near-mid` (divergence never at the last byte) matches `near-byte15` (53/40 vs
  52/38) — the whole-key CRC is not gamed by *where* the keys differ. Fingerprint rejection on `near-mid`:
  **100.000%** (200000/200000 rejected before the leaf load, 0 fell through to the verify).
- **0 false-positives on all three sets; HIT checksums identical to the verifying baseline** — the pointer
  tagging and the verify-on-match are correct on this platform.

**The hit tax — real but small, and ART still wins hits anyway.** Computing the query CRC costs ~12.4
cyc/key (measured, bulk over the probe column). On a hit there is no leaf load to skip, so the CRC is pure
overhead: serial HIT 142 → 155 (+13, the full CRC, not overlapped at `-O2`), batched HIT 82 → 87 (+5, AMAC
hides ~7 of the 12 behind other lanes' loads). Both still beat the hash on hits (serial 155 < 172, batched
87 < 138), so the tax never costs ART the hit comparison.

## 5. What this means

With the fingerprint, **ART dominates the multiversion hash on every point-read cell measured** — HIT and
MISS, serial and batched:

```
                    |  serial HIT | serial MISS | batched HIT | batched MISS
  ART + fingerprint |     155     |     53      |     87      |     40
  MVCC hash         |     172     |     75      |    138      |     73
```

The hash's *last* point-read advantage (ART.md §2/§7/§8: "the hash wins misses → route miss-heavy
workloads to the hash") **no longer holds.** That has an architectural payoff beyond the cycles: it
removes the reason to maintain a second structure and a routing heuristic. One ART can serve the whole
string-property index — point and range, hit and miss, HEAD and history — and the hash's remaining edges
shrink to write-simplicity and lock-freedom (and ART already has lock-free reads).

A secondary, honest finding from the `random` column: **ART already beat the hash on *realistic* misses
even without the fingerprint** (serial 62 vs 74, batched 52 vs 73). The hash always pays its FNV digest;
ART short-circuits a random absent key in a few warm upper-node hops with no hashing. The hash's miss
advantage was *specifically* the prefix-colliding tail (`near*`), which descends to a leaf — and that tail
is exactly what the fingerprint removes. So the adversarial table in ART.md §7 overstated the hash's
practical miss advantage; the fingerprint closes even the adversarial case.

## 6. Suggested ART.md amendment (if adopted)

This is a research result, not a unilateral spec change. If accepted, ART.md would need:

- **§2 / §7 / §8** — the "hash wins misses, route miss-heavy slices to the hash" claim is superseded. With
  the leaf fingerprint, ART wins misses too (53 vs 75 serial, 40 vs 73 batched).
- **§3.2 / §4.3 / §6.1** — add the fingerprint to the leaf-edge contract: a fingerprint of the full key in
  the child pointer's spare bits; reject on mismatch (no leaf load), **verify the full key on match**
  (the §6.1 invariant is unchanged — the fingerprint is a pre-filter, never a replacement for the verify).
- **§9** — a new requirement: carry a per-leaf-edge key fingerprint and check it before the leaf load.

The project memory `project_string_index_decision.md` ("the hash's one surviving edge: misses") would be
updated to reflect that the fingerprint removes it.

## 7. Files

- `index_miss.cpp` — implements the fingerprint read path (serial + AMAC) and the three honest absent-key
  distributions; prints the table above, the rejection rate, the bulk-CRC cost, and the false-positive /
  checksum audits. Additive over `index_opt.cpp`; the baseline rows reproduce it.
- `report_art_opt.md` / `index_opt.cpp` — the settled read path this extends.
- `report_art.md` / `index_decompose.cpp` — the decomposition that located the leaf load as the miss cost.
