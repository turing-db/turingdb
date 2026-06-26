# Why ART is ~2× the multiversion hash for high-cardinality reads at low lag

`report_bench.md` left one row unexplained. For a high-cardinality string key read at HEAD (lag 0), the
three structures clock:

```
  lag 0 (HEAD) | high-cardinality key |  mvcc 30 ns  /  hamt 62 ns  /  art 57 ns
```

The multiversion hash reads in ~30 ns; ART takes ~57 ns — **almost 2× slower**, for short (16-byte)
keys with a cache-resident (warm) working set. This is the *one* regime where the hash's flat O(1) read
is not disqualified by time-travel or write contention, so understanding the gap matters: it is the
strongest case for keeping the hash at all.

This report reasons the gap out from first principles and then **measures it**, decomposed into named
causes, with a reproducible harness (`index_decompose.cpp`). It then surveys the known speed-up
principles for ART reads and ideates improvements beyond them.

> **How the numbers were taken.** `index_decompose.cpp` rebuilds the exact `MvccHash` and `Art` from
> `index_bench.cpp` over the same workload (100 k key space, 28 486 present, 16-byte keys, 2000 commits),
> then times the high-cardinality HEAD read under a set of **ablation variants** that each remove one
> cost, with **inline hardware counters** (cycles, instructions, LLC misses, branch misses) around each
> loop, pinned to one core (`taskset -c 0`), best of 5 passes. The box scales 0.8–5.2 GHz under the
> `powersave` governor, so **cycles/op is the primary, frequency-invariant metric**; ns is secondary
> (≈ cyc / 5.2). Build: `g++ -std=c++23 -O2 -o index_decompose index_decompose.cpp`.

## The one fact that explains most of it: dependent-load count

A read is a chain of loads where each address depends on the previous load's result, so the CPU cannot
overlap their latencies — it stalls on each in turn. The structures differ in **how many** such hops a
read costs. Measured exactly (averaged over 200 k reads):

| structure | dependent loads per HEAD read | the chain |
|-----------|------------------------------:|-----------|
| **multiversion hash** | **2.0** | slot (probe length 1.06) → version-chain head node |
| **ART** | **4.0** | N256 root (byte 0) → N256 (byte 1) → N16 (byte 2) → leaf |

ART's descent shape, measured (the index-stamped keys diverge in the first ~3 bytes, so the upper two
levels are dense N256 and the tree bottoms out at a leaf by depth 3–4):

```
ART descent: inner hops = 3.012   leaf compares = 1.000   total dependent loads = 4.012
  hop 0: N256 100%        (first byte — all 62 alphabet values present → grown to N256)
  hop 1: N256 100%        (second byte)
  hop 2: N16  95% / N4 5% (third byte — ~8 present keys share a 2-byte prefix)
  hop 3: Leaf 99%
MVCC: probe length 1.06   chain depth at HEAD 2.26 (HEAD read takes only the head — 1 hop past the slot)
```

This is exactly the original ART authors' own framing of why a hash table is hard to beat: a chained hash
table "can be considered a tree of only two levels" (Leis et al., ICDE 2013). ART here is a tree of four.
Alvarez et al. (ICDE 2015), re-evaluating ART against hash tables, measured the same thing as the decisive
term: ART pays ~2.5–3 dependent **L3-cache *hits*** per lookup (the warm upper-tree traversal) where the
hash table's L3-hit term "is essentially non-existent" (~0.1).

## The measured decomposition (cycles/op, best of 5, median of 3 runs)

```
  variant                                          | cyc/op | ins/op |  CPI  | LLC/op | br.miss
  -------------------------------------------------+--------+--------+-------+--------+--------
  MVCC M0 baseline (hash + 2 loads + key compare)  |   168  |  171   | 0.97  | 0.28   | 0.065
  MVCC M1 precomputed hash (no fnv1a)              |    74  |   66   | 1.05  | 0.20   | 0.065
  MVCC M2 hash, NO key compare                     |   152  |  122   | 1.24  | 0.28   | 0.061
  MVCC M3 precomputed hash, NO key compare         |    30  |   18   | 1.62  | 0.09   | 0.061
  ART  A0 baseline (descent + pooled key compare)  |   300  |  152   | 1.97  | 0.30   | 1.10
  ART  A1 descent only (NO leaf compare)           |   276  |  118   | 2.34  | 0.26   | 1.09
  ART  A2 inline-key leaf (no pooled-key chase)    |   288  |  129   | 2.23  | 0.33   | 1.09
```

Reading the ablations as differences gives a per-cause attribution:

**The multiversion hash (168 cyc total) splits into:**
- **fnv1a over the 16-byte key ≈ 94 cyc** (M0 − M1). The single largest component — ~56% of the read.
  It is a byte-serial chain of `xor; multiply` (the multiply has ~4-cycle latency), so it runs at
  **~4.5 cyc/byte** and grows linearly with key length (proven below). It is also high-ILP compute that
  *hides* the load latency underneath it — hence M0's CPI of 0.97.
- **2 dependent loads ≈ 30 cyc** (M3): the slot probe + the chain-head node. ~15 cyc/load (warm L2).
- **full key compare ≈ 16–42 cyc** — and this is interesting: it costs ~42 cyc in isolation (M1 − M3)
  but only ~16 cyc when the hash is present (M0 − M2), because the compare's loads overlap with the
  hash's compute tail (same key bytes, already in flight). Verification is *partly free* when you hashed.

**ART (300 cyc total) splits into:**
- **descent ≈ 276 cyc** (A1): the 4 dependent loads *plus* per-level work. This is the whole story, and
  it is far more than 4 × 15 cyc. The reasons it inflates:
  - **~118 instructions** vs the hash's 18 for its 2-load structure (M3) — ~6.5×. Each level runs a
    node-type `switch`, prefix handling, and an in-node child search.
  - **~1.10 branch mispredictions per lookup** vs the hash's 0.065 — **~17×**. These come from the
    node-type `switch` and the N16 **data-dependent linear scan** (`for i: if keys[i]==byte`). At ~15–18
    cyc each that is ~18 cyc of the descent, and it pushes CPI to 2.3 (memory- *and* branch-stalled).
- **leaf full-key compare ≈ 24 cyc** (A0 − A1). A radix descent only inspects *discriminating* bytes, so
  the leaf must verify the whole key against the stored copy. Of that, ~12 cyc is the **pooled-key
  pointer chase** (A0 − A2) — the stored key is a `string_view` into the dictionary, a separate cache
  line from the leaf node.

**The clean way to state the asymmetry:** strip both down to "pure structure, no hashing, no key
handling" and the hash is **30 cyc / 18 instr / 0.06 branch-misses** (M3) while ART is **276 cyc / 118
instr / 1.09 branch-misses** (A1) — ART's traversal alone is ~9× the cost. The hash then *gives most of
that back* by paying ~94 cyc to digest the key plus its own compare, netting the observed end-to-end
**300 vs 168 ≈ 1.8×**. So the gap is not "ART is slow"; it is "ART's structural traversal is ~9× heavier,
and hashing a short key is cheap enough that the hash can afford it."

The three factors, ranked by contribution to the warm-16B gap:

1. **2× the dependent loads** (4 vs 2), and the loads are not overlappable — the dominant *latency* cause.
2. **~6.5× the instruction count**, from per-level node dispatch + in-node search — the dominant cause
   that *survives* even when latency is hidden (see batching, below).
3. **~17× the branch mispredictions**, from the node-type `switch` and the N16 linear scan.
4. (small at 16 B) the leaf's mandatory full-key verification, ~24 cyc.

## Why specifically at 16 bytes — the crossover, measured

The hash's compensating weakness is that it must digest the *entire* key, and that cost is **linear in
key length** while ART's descent is **flat** (the key length past the discriminating prefix only lengthens
the one leaf `memcmp`, which is cheap and predictable). The sweep makes the crossover explicit:

```
  key len | MVCC M0 (hash+probe+cmp) | ART A0 (descent+cmp) | fnv1a-only cost (M0−M1)
  --------+-------------------------+----------------------+------------------------
   16 B   |   138 cyc   27 ns       |   300 cyc   58 ns    |    93 cyc
   24 B   |   160 cyc   31 ns       |   297 cyc   57 ns    |   117 cyc
   32 B   |   186 cyc   36 ns       |   293 cyc   57 ns    |   140 cyc
   48 B   |   281 cyc   54 ns       |   292 cyc   56 ns    |   238 cyc   ← tie
   64 B   |   351 cyc   68 ns       |   305 cyc   59 ns    |   302 cyc   ← ART wins
  100 B   |   508 cyc   98 ns       |   327 cyc   63 ns    |   443 cyc   ← ART wins
```

The hash's `fnv1a` cost climbs ~4.5 cyc/byte (93 → 443 cyc); ART stays ~290–330 cyc throughout. They
cross at **~48–56 bytes** — which is precisely where `report_bench.md` placed it ("ART overtakes the
multiversion hash at ~48-byte keys"). So the 16-byte result is not a verdict on ART; it is a verdict on
*short* keys, where hashing is too cheap for ART's structural overhead to beat.

## The gap is latency-bound — and that is recoverable

Because ART's 4 loads are serialized, the read is **latency-bound**, not throughput-bound. An 8-way
software-pipelined (AMAC-style) descent — keep 8 independent lookups in flight, prefetch each one's next
node, round-robin — overlaps their stalls:

```
  variant                                  | cyc/op | CPI
  -----------------------------------------+--------+-----
  ART  serial   A0                         |  300   | 1.97
  ART  8-way pipelined                     |  112   | 0.52   ← 2.7× faster
  MVCC serial   (precomputed hash)         |   74   | 1.05
  MVCC 8-way pipelined                     |   66   | 0.59   ← barely changes
```

ART recovers **2.7×** from pipelining; the hash barely moves (it has almost no dependent-load latency to
hide — exactly Leis et al.'s observation that pipelining sped ART 1.6–1.7× but a hash table only 1.2×, and
the Cuckoo Trie's whole thesis, SOSP 2021). Two consequences:

- In a **batched** read path (an analytical engine probing a column of keys — TuringDB's common case),
  ART's worst property largely evaporates: 112 cyc is within ~1.7× of the hash's 66, not 1.8× of a
  latency-bound 30.
- The **residual** 112 vs 66 is the instruction-count + branch-miss gap (factors 2 and 3) — the part MLP
  *cannot* hide. That is what node-structure improvements must attack.

## A negative result worth keeping: don't collapse the *top*

The obvious "reduce the depth" move — replace the two dense upper N256 hops with one array indexed by the
first two bytes (4 hops → 3) — **did not help**: 302 cyc, no better than baseline, even though it cut
instructions 152 → 110. A 65 536-entry array is 512 KB and sparsely populated; indexing it scatters across
the whole array and its LLC misses jumped (0.18 → ~0.8). Meanwhile the hops it removed were the *hottest*
ones: the single N256 root and the 62 second-level nodes are touched on every lookup and stay L1/L2-
resident. **ART's upper hops are cheap; the expensive load is the cold, scattered leaf.** Depth reduction
has to preserve cache density and should target the bottom, not the top.

## Known improvement principles for ART reads (from the literature)

| principle | mechanism | effect / cost | source |
|-----------|-----------|---------------|--------|
| **SIMD Node16 search** | one `_mm_cmpeq_epi8` + movemask + tzcnt over the 16 child bytes, instead of a linear/binary scan | removes the data-dependent N16 scan → kills a branch misprediction (our factor 3) | Leis et al., ICDE 2013 |
| **Path compression + lazy expansion** | drop single-child chains; create inner nodes only to distinguish ≥2 leaves; verify skipped bytes at the leaf | measured 40 → 8.1 average height for compound keys — each level removed is one fewer dependent load | Leis et al., ICDE 2013 |
| **Value-in-pointer (pointer tagging)** | when the value fits in a pointer, store it tagged in the parent's child slot instead of a separate leaf node | **removes the final (cold) leaf load** — directly our biggest LLC miss; DuckDB's ART inlines the rowid this way | Leis et al. 2013; DuckDB |
| **HOT — Height Optimized Trie** | combine binary-Patricia levels into compound nodes of bounded fanout k=32, discriminate on sparse partial keys with AVX2 | ≥25% higher lookup throughput vs ART/B-tree/Masstree, +200% on URLs; +26% memory on long strings vs ART's +51% | Binna et al., SIGMOD 2018 |
| **Masstree** | trie of B+-trees over 8-byte key slices; **prefetch the whole node** to overlap search with the miss | the prefetch lever ART lacks; >6 M queries/s | Mao et al., EuroSys 2012 |
| **MLP / software pipelining (AMAC, Cuckoo Trie)** | issue many independent lookups' dependent loads concurrently | what we measured: 2.7× here; Cuckoo Trie 1.2–4.6× over SOTA | Kocberber 2015; Zeitak & Morrison, SOSP 2021 |
| **Wormhole** | hash + trie + B+-tree to get O(log L) key-length scaling | 4.3× vs ART on lookups | Wu et al., EuroSys 2019 |
| **OLC / ROWEX synchronization** | readers take no locks (version-validate or read-optimized exclusion) — never dirty a shared line | keeps concurrent readers off the write path; ROWEX reads never block/restart | Leis et al., DaMoN 2016 |
| **Persistent ART (PART/VART)** | path-copy the O(log₂₅₆ n) ancestors per write; **mutate in place until a snapshot is taken** | makes versioned writes approach in-place cost; avoids cloning the wide upper nodes except at commit | Dave et al. (Berkeley TR); SurrealDB VART |

The literature's verdict on our exact question is blunt: for point lookups on short, fixed-length keys, a
bucketized SIMD hash table (Cuckoo / F14 / SwissTable) wins, and ART earns its place on **ordered/range/
prefix** access and **history** — none of which a hash table can do. (Alvarez et al. measured the fast
Cuckoo at ≥2× ART on lookups for 64-bit keys; swapping a cheap multiplicative hash for Murmur alone made
their hash table 1.93× faster — i.e. the hash function is the hash table's main lookup cost, exactly as our
fnv1a = 56% of the read shows.)

## Ideation — beyond the existing literature

Each idea names the measured factor it attacks. The first two are the ones I would prototype; the harness
makes them cheap to validate.

**1. Tuned hash/radix split — "hash the discriminating prefix, radix the tail" (compact, not array).**
The failed wide-root experiment failed only because a flat 512 KB array is cache-sparse. Replace it with a
**compact open-addressed hash table keyed on the first k bytes**, whose payload is a pointer to the
depth-k subtree. A lookup is then: hash k bytes (cheap — k is small, ~k·4.5 cyc) + 1 probe → land at the
subtree → radix-descend the remainder. This collapses the dense upper hops into **one** cache-dense probe
while leaving the radix tail (and its ordered/range/history properties) intact. The crossover sweep *is*
the tuning curve: pick k so that `hash(k bytes)` is still cheap but k disambiguates most of the dense
prefix. It is "Wormhole-shaped," but the novel framing is that **k is a measured knob** set from the
column's key-length × cardinality, not a fixed design — the index is a *spectrum* between pure hash (k =
keylen) and pure radix (k = 0), and the optimum is data-dependent. Attacks factor 1 (load count) without
the negative-result's cache penalty.

**2. Versioning-aware split: a shared, mutable, non-versioned hash front-end over persistent-radix tails.**
This is idea 1 specialized to TuringDB's MVCC and it pays off *twice*. The upper levels of the trie (the
dense first 2–3 bytes) are structurally **stable** — adding keys almost never reshapes them — so they do
not need to be path-copied per commit at all. Keep them as **one shared mutable prefix-hash**, versioned
only by the lower subtrees it points at. Reads get idea 1's collapsed top; **writes stop cloning the 2 KB
Node256 upper nodes** that `report_bench.md` measured as ART's worst axis (38 µs/commit, 107 MiB) — only
the small lower nodes get path-copied. One structural change improves the read depth *and* the write/
memory weakness simultaneously. (Risk: a structural change to the dense prefix set — rare — forces a
front-end rebuild; amortizable, and the prefix set converges quickly.)

**3. Swiss-table metadata dispatch inside radix nodes — branch-free child search.**
Factor 3 (1.1 branch-misses/lookup) is the node-type `switch` + the N16 scan. Borrow the *hash table's*
trick and graft it onto the radix node: store a fixed 16-byte SIMD group of child-bytes in every inner
node's header, searched with one `_mm_cmpeq_epi8` + movemask (branch-free), so the common case (≤16
children, which is hops 2+ here) resolves with **no data-dependent branch and no type switch** — the node
type only matters for the >16 overflow. This is SIMD-Node16 generalized to *all* node types and lifted
into a uniform header, turning the polymorphic `switch` into one predictable path. Attacks the residual
that MLP can't hide (the 112-vs-66 gap).

**4. Inline-verification terminal slots.** Combine value-in-pointer (known) with inlining a few
*discriminator* bytes (the un-consumed suffix) next to the value in the parent's slot, so a unique-key
lookup both skips the cold leaf load *and* stays correct against absent keys (no false positives). For a
versioned store the value is an 8-byte (commit, row) code; an 8-byte value + 8-byte discriminator fits a
slot pair. Attacks the one real LLC miss (the leaf).

**5. Make batched probing the default in the columnar pipeline.** Not a structural change — a *plumbing*
change. We measured ART recovering 2.7× from 8-way pipelining; analytical point lookups arrive in batches
(join probes, `IN` filters, key-column scans). Exposing a batch-lookup API that runs the AMAC descent,
instead of looping scalar `lookup()`, turns ART's defining weakness (serial dependent loads) into hidden
MLP for free, narrowing the gap and keeping ART's structural wins. The lowest-effort, highest-certainty
item — it requires no new index, only a different call site.

## Bottom line

The ~2× is real, and it is **structural, not incidental**: ART issues twice the dependent loads (4 vs 2),
runs ~6.5× the instructions, and mispredicts ~17× the branches, because a 4-level adaptive trie is simply
more machine than a 2-load probe. The multiversion hash claws ~94 cyc back the moment the key is longer
than a few bytes (hashing is ~4.5 cyc/byte and linear), which is the *entire* reason the hash still wins
at 16 B and loses by 48–64 B. For TuringDB this sharpens the routing rule from `report_bench.md`: the hash
is the right HEAD-read index only for **short, non-churning, point-read** string columns; the moment keys
get long, reads go historical, or range/prefix matters, the trie's flat-in-length, flat-in-lag read wins —
and most of the trie's remaining read deficit is recoverable by **batched (MLP) probing** plus a
**cache-dense hash/radix split** that also fixes its write cost.

## Files
- `index_decompose.cpp` — the ablation + hardware-counter harness behind every number here.
  `g++ -std=c++23 -O2 -o index_decompose index_decompose.cpp && taskset -c 0 ./index_decompose`.
- `index_bench.cpp` / `report_bench.md` — the wall-clock validation this drills into.
