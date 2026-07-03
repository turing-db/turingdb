---
name: String-property index decision — ART + SIMD + leaf-fingerprint + leaf-verify + AMAC 8-way
description: DECISION — the string-property index is an ART read path of SIMD Node16 + leaf-fingerprint miss-reject + full-leaf verify + AMAC 8-way batched probing. Absent-key-safe; now beats the multiversion hash on MISSES too (the fingerprint removed the hash's last point-read edge). Supersedes the "route miss-heavy to the hash" note and the value-in-pointer pick.
type: project
originSessionId: feab39c4-7c6c-41c5-a782-e4256785aca3
---
# DECISION (2026-06-25): the string-property index is **ART + SIMD Node16 + leaf-fingerprint miss-reject + full-leaf verify + AMAC 8-way**.

The read path, stated unambiguously:
- **ART** (adaptive radix tree, persistent/versioned) — the structure.
- **SIMD Node16** — SSE2/AVX2 child search (`_mm_cmpeq_epi8` + `movemask` + `tzcnt`), never a linear scan.
- **leaf-fingerprint miss-reject** — a 16-bit CRC32 of each leaf's full key carried in the leaf child pointer's spare bits (top 16 bits + a low leaf-marker bit), checked DURING the descent, before the leaf load. A fingerprint *mismatch* rejects an absent key WITHOUT the cold leaf load (the short-circuit the trie lacked and the hash had). On a *match*, still verify the full key. Sound: 0 false-negatives, 0 false-positives. Immutable node payload → carried by COW for free, no separate versioned filter.
- **full-leaf verify** — compare the full key at the leaf (absent-key-safe). **NOT** value-in-pointer.
- **AMAC 8-way** — software-pipelined batched probing for the columnar batch-probe path.

**Value-in-pointer is still rejected** for production: it skips the un-discriminated suffix verify, so it false-positives on prefix-colliding absent keys (200000/200000 measured). The fingerprint is its SOUND dual — same spare-pointer-bits mechanism, but it stores a fingerprint that still verifies, not a value that skips the verify. A front Bloom/quotient filter is also rejected: it re-hashes the whole key on every lookup (the cost ART exists to avoid) and needs its own versioned structure.

Backed by `docs/ART.md` and `tools/index-sim/report_art_miss.md` + `index_miss.cpp` (the fingerprint), on top of `report_art_opt.md` + `index_opt.cpp` (the rest) and `report_art.md` / `index_decompose.cpp` (decomposition).

**Why (fair, absent-safe, measured — high-cardinality 16-byte HEAD reads, cyc/op, median of 3, Intel Core Ultra 7 265):**
| variant (all verify; 0 false-positives) | HIT | MISS |
|---|---|---|
| MVCC hash, serial | 171 | 75 |
| MVCC hash + AMAC 8-way | 140 | 73 |
| ART + SIMD + leaf-verify, serial | 145 | 152 |
| ART + SIMD + leaf-verify + AMAC 8-way | 82 | 90 |
| ART + SIMD + fingerprint + leaf-verify, serial | 155 | 53 |
| **ART + SIMD + fingerprint + leaf-verify + AMAC 8-way** | **87** | **40** |

On HITS ART already beat the hash (the hash pays FNV ~94 cyc/probe; ART hashes nothing, AMAC hides its dependent-load latency). The **fingerprint now wins MISSES too** — serial 152→53 (beats hash 75), batched 90→40 (beats hash 73) — by rejecting the prefix-colliding absent key before the cold leaf load (the entire miss cost). So **ART dominates the hash on every point-read cell — hit and miss, serial and batched** — while keeping the trie's structural wins the hash lacks: flat-in-lag snapshot/time-travel reads, ordered/range/prefix scans, lock-free wait-free readers. Cost: a small fingerprint hit-tax (~+10 serial / ~+5 batched, the query CRC; ART still wins hits with it on). On *random* (non-colliding) absent keys ART already beat the hash — so the hash's miss edge was only the prefix-colliding tail, which the fingerprint removes.

**This overturns the earlier "the hash's one surviving edge is misses" note.** The hash now has NO point-read advantage; its only remaining edges are write-simplicity and lock-freedom (and ART already has lock-free reads). Route only **write-dominated** slices to the hash — point-read routing (including miss-heavy) is no longer needed.

**How to apply (build requirements):**
- SIMD (SSE2/AVX2) child search for Node16/Node4.
- Carry a per-leaf-edge fingerprint of the full key (16-bit CRC32 in the child pointer's spare bits); reject on fingerprint mismatch before the leaf load; on a match, still verify the full key. Do not maintain a separate front filter.
- Verify the full key at the leaf — do **not** inline the value in the child pointer.
- Expose an AMAC-style batched/pipelined lookup for batch-probe sites (join probes, `IN` filters, key-column scans); do not loop scalar lookups there.
- Keep path compression + lazy expansion (mandatory for long/structured keys: 44→6 dependent loads measured) and chain prefix nodes past the inline-prefix cap — the `index_bench` prototype mis-branches once a shared prefix exceeds `ART_MAX_PREFIX=16`; fixed in `index_opt.cpp`.
- Pair with transient/folded commits to contain persistent-ART write/memory cost (the Node256 copy-on-write clone cost flagged in `report_bench.md`).

Relevant to scale-out / versioned storage: see [[project_partitioning]].
