// index_sim — cost-model simulator for versioned property-index designs.
//
// Compares four ways of maintaining a string-property index over TuringDB's
// immutable, git-like commit model, as the number of parts/commits scales 1..2000:
//
//   1. HAMT per commit with structural sharing
//   2. COW B-tree per commit, keyed by dictionary code (int) for strings
//   3. Multiversion hash table, global (one structure, version-tagged entries)
//   4. COW layers of hash tables, one layer per commit
//
// It is a COST MODEL, not a wall-clock benchmark: the workload is executed so that
// operation counts (tree depth, layers walked, version-chain length) reflect the real
// access distribution, but each operation is charged a FIXED ESTIMATED cost in
// nanoseconds (a cache-miss pointer chase, a node allocation, a CAS, a lock). The
// concurrency cost of the global multiversion table is DERIVED from those constants
// plus a writer-thread count, never measured with real threads. Every constant is a
// flag, so you can plug in your own machine's estimates.
//
// A follow-up study (runHamtVariants / runFanoutSweep / runStringFrontend, see report_hamt.md)
// asks how far the HAMT's read/submit/memory gap to the lock-free multiversion hash can be closed
// for STRING keys: it adds a string front-end (hashing/comparing a key is O(length)), dictionary
// coding, folded (transient) commit application, a width-aware path-copy cost, a fan-out sweep,
// CHAMP node compaction, and a persistent Adaptive Radix Tree (ART) that drops hashing entirely.
//
// Build:  g++ -std=c++23 -O2 -o index_sim index_sim.cpp
// Run:    ./index_sim                       # defaults, writes results_*.csv
//         ./index_sim --parts 2000 --keys 100000 --writes 32 --threads 8
//         ./index_sim --hot 0.05            # 5% of writes hit one hot key (stresses lock-free)
//         ./index_sim --bloom               # add a per-layer Bloom filter to approach 4

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <math.h>

#include <string>
#include <string_view>
#include <vector>
#include <algorithm>
#include <fstream>
#include <cstdio>

namespace {

// Fixed estimated per-operation costs (nanoseconds) for a modern server core, and the
// structural parameters of each index. All overridable from the command line.
struct CostModel {
    double hopNs {80.0};       // one pointer chase that misses cache and hits DRAM
    double compareNs {2.0};    // one integer key comparison / predicted branch
    double hashNs {5.0};       // compute a hash / map a dictionary code to a slot
    double allocNs {60.0};     // allocate and construct one node
    double casNs {20.0};       // one uncontended atomic compare-and-swap (with fence)
    double lockNs {25.0};      // one uncontended mutex lock + unlock (futex fast path)
    double bloomNs {30.0};     // one Bloom-filter probe
    double mergeNs {10.0};     // merge/reconcile one matching posting at read time

    // String-key front-end. Hashing and comparing a string are O(length), unlike the int
    // dictionary-code costs above. A streaming memcpy slot-copy is far cheaper than a random
    // cache-miss hop, so copySlotNs << hopNs.
    double strHashByteNs {0.5};   // hash one byte of a string key
    double strCmpByteNs {0.4};    // compare one byte of two string keys
    double strFixedNs {3.0};      // fixed per-string-op overhead (call, length load)
    double copySlotNs {1.0};      // copy one occupied child slot when path-copying a node

    size_t hamtBranch {32};    // HAMT fan-out (5 bits per level)
    size_t btreeOrder {32};    // B-tree node fan-out
    size_t stripeCount {256};  // lock stripes for the striped-locking policy

    // Estimated retained sizes (bytes) used for the memory comparison.
    double hamtNodeBytes {64.0};
    double champNodeBytes {40.0};  // CHAMP: compact data/node split — smaller than a HAMT node
    double artNodeBytes {32.0};    // ART: average over the adaptive Node4/16/48/256 mix
    double btreeNodeBytes {384.0};
    double versionNodeBytes {32.0};
    double layerEntryBytes {24.0};
    double layerHeaderBytes {64.0};
};

struct SimParams {
    size_t keys {100000};            // distinct dictionary codes (property cardinality)
    size_t writesPerCommit {32};     // write-set size of each commit
    size_t parts {2000};             // number of commits/parts to scale up to
    size_t readsPerCheckpoint {1000};// reads sampled to average read latency at each commit
    size_t threads {8};              // concurrent writers, for the wait-time derivation
    size_t compactParts {64};        // per-DataPart compaction target: keep <= this many live parts
    size_t keyLenBytes {16};         // average byte length of a string key (string front-end)
    double hotKeyShare {0.0};        // fraction of writes that target a single hot key
    bool bloom {false};              // give approach 4 a per-layer Bloom filter
    std::string outDir {"."};
    uint64_t seed {0x9E3779B97F4A7C15ull};
};

// Deterministic PRNG so runs are reproducible and the workload is identical across
// approaches (apples-to-apples).
struct SplitMix64 {
    uint64_t _state {0};

    explicit SplitMix64(uint64_t seed)
        : _state(seed)
    {
    }

    uint64_t next() {
        uint64_t z = (_state += 0x9E3779B97F4A7C15ull);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
        return z ^ (z >> 31);
    }

    size_t nextIndex(size_t bound) {
        return static_cast<size_t>(next() % bound);
    }
};

// Number of levels a tree with the given fan-out needs to hold keyCount keys.
size_t depthFor(size_t keyCount, size_t branch) {
    size_t depth = 1;
    size_t capacity = branch;
    while (capacity < keyCount) {
        capacity *= branch;
        ++depth;
    }
    return depth;
}

double log2Of(size_t value) {
    const size_t clamped = value < 2 ? 2 : value;
    return std::log2(static_cast<double>(clamped));
}

// --- Read latency models (point lookup at HEAD), in nanoseconds. ---

// HAMT: follow the hash slice down `depth` immutable nodes (one cache miss each).
double hamtReadNs(const CostModel& model, size_t depth) {
    return static_cast<double>(depth) * (model.hopNs + model.compareNs);
}

// COW B-tree: descend `depth` nodes; binary-search log2(order) int keys inside each.
double btreeReadNs(const CostModel& model, size_t depth) {
    const double inNodeCompare = log2Of(model.btreeOrder) * model.compareNs;
    return static_cast<double>(depth) * (model.hopNs + inNodeCompare);
}

// Multiversion hash: dictionary codes are dense, so the table is a direct-mapped array
// of version-chain heads. HEAD read = map the code to its slot, load the newest version.
// Independent of the number of parts.
double mvccReadNs(const CostModel& model) {
    return model.hashNs + 2.0 * model.hopNs;
}

// COW layers: walk layers newest -> oldest until the key is found. With a Bloom filter,
// the layers above the hit are rejected by a cheap probe instead of a full map lookup.
double layersReadNs(const CostModel& model, double layersWalked, bool bloom) {
    if (!bloom) {
        return layersWalked * (model.hashNs + model.hopNs);
    } else {
        const double negativeLayers = layersWalked > 1.0 ? layersWalked - 1.0 : 0.0;
        return negativeLayers * model.bloomNs + (model.hashNs + model.hopNs);
    }
}

// --- Submit latency models (apply one commit of `writes` updates), in nanoseconds. ---

double hamtSubmitNs(const CostModel& model, size_t depth, size_t writes) {
    const double perInsert = static_cast<double>(depth) * (model.allocNs + model.hopNs + model.compareNs);
    return static_cast<double>(writes) * perInsert + model.casNs;  // + publish new root
}

double btreeSubmitNs(const CostModel& model, size_t depth, size_t writes) {
    const double inNodeCompare = log2Of(model.btreeOrder) * model.compareNs;
    const double perInsert = static_cast<double>(depth) * (model.allocNs + model.hopNs + inNodeCompare);
    return static_cast<double>(writes) * perInsert + model.casNs;  // + publish new root
}

// Uncontended (single-writer) cost: allocate a version node and link it at the slot head.
double mvccSubmitBaseNs(const CostModel& model, size_t writes) {
    return static_cast<double>(writes) * (model.allocNs + model.hopNs);
}

double layersSubmitNs(const CostModel& model, size_t writes) {
    return static_cast<double>(writes) * (model.allocNs + model.hashNs) + model.allocNs + model.casNs;
}

// --- Approach 5: per-DataPart index, collected at read time. ---
// Each immutable part carries a local index over just its own write-set, built once and
// never mutated. A read must probe EVERY part and merge the matches, because any part may
// contain the key (this is the LSM/ClickHouse segment model, and what TuringDB already does
// per DataPart for edges and string properties). The write path and memory are identical to
// the COW-layers approach; only the read differs — collect-and-merge across all parts rather
// than stopping at the newest. That makes the read O(parts), unconditionally.

double perPartSubmitNs(const CostModel& model, size_t writes) {
    // Build one immutable local hash index over this part's writes (same as a layer).
    return static_cast<double>(writes) * (model.allocNs + model.hashNs) + model.allocNs;
}

// Naive collection: probe all `parts` local indexes, merge the `matchingParts` that hit.
double perPartReadCollectNs(const CostModel& model, double parts, double matchingParts) {
    return parts * (model.hashNs + model.hopNs) + matchingParts * model.mergeNs;
}

// Pruned collection: a per-part Bloom / zone-map filter rejects parts that cannot match with
// a cheap probe; only matching parts pay the full lookup. Still O(parts), smaller constant.
double perPartReadPrunedNs(const CostModel& model, double parts, double matchingParts) {
    return parts * model.bloomNs
         + matchingParts * (model.hashNs + model.hopNs)
         + matchingParts * model.mergeNs;
}

// Compacted: a background merge keeps the live part count at <= `target`, so a HEAD read
// fans out over only min(parts, target) live parts — the curve flattens once history
// passes the target instead of climbing forever.
double perPartReadCompactedNs(const CostModel& model, double parts, double target, double matchingParts) {
    const double liveParts = parts < target ? parts : target;
    const double liveMatches = matchingParts < liveParts ? matchingParts : liveParts;
    return liveParts * (model.hashNs + model.hopNs) + liveMatches * model.mergeNs;
}

// Compaction is not free: tiered merging rewrites each entry ~log2(parts/target) times, paid
// back on submit as write amplification — flat reads bought with rising writes.
double compactionWriteAmp(double parts, double target) {
    if (parts <= target) {
        return 1.0;
    }
    return std::log2(parts / target) + 1.0;
}

double perPartSubmitCompactedNs(const CostModel& model, size_t writes, double parts, double target) {
    return perPartSubmitNs(model, writes) * compactionWriteAmp(parts, target);
}

// One log-spaced sample of the per-DataPart approach, kept for the summary table.
struct PerPartCheckpoint {
    size_t parts {0};
    double readCollect {0.0};
    double readPruned {0.0};
    double readCompacted {0.0};
    double submit {0.0};
    double submitCompacted {0.0};
    double mvccRead {0.0};
};

// --- Derived concurrency wait time for the GLOBAL multiversion table. ---
// The other three are per-commit structures: writers serialize at a single root-publish
// CAS, so there is no per-key contention to model. The global table is shared, so every
// version prepend contends. Below, `threads` writers submit concurrently; the wait is
// derived purely from the fixed costs above — no real threads are spawned.

// One global mutex: a commit's whole critical section (linking `writes` slot heads) is
// serialized. A writer queues behind the other (threads-1) writers' critical sections.
double mvccSubmitGlobalLockNs(const CostModel& model, const SimParams& params) {
    const double writes = static_cast<double>(params.writesPerCommit);
    const double others = static_cast<double>(params.threads) - 1.0;
    const double criticalSection = writes * model.hopNs;       // link work under the lock
    const double wait = others * criticalSection;              // serialized behind others
    const double allocOutsideLock = writes * model.allocNs;
    return allocOutsideLock + model.lockNs + criticalSection + wait;
}

// Striped locks: a prepend locks stripe (code % stripeCount). Uniform writes collide with
// probability (threads-1)/stripeCount; a hot key forces all writers onto one stripe.
double mvccSubmitStripedNs(const CostModel& model, const SimParams& params) {
    const double writes = static_cast<double>(params.writesPerCommit);
    const double others = static_cast<double>(params.threads) - 1.0;
    const double hotWrites = params.hotKeyShare * writes;
    const double coldWrites = writes - hotWrites;

    const double coldCollision = others / static_cast<double>(model.stripeCount);
    const double coldWait = coldWrites * coldCollision * model.hopNs;
    const double hotWait = hotWrites * others * model.hopNs;    // all writers share one stripe

    const double lockWork = writes * (model.lockNs + model.hopNs);
    const double allocWork = writes * model.allocNs;
    return allocWork + lockWork + coldWait + hotWait;
}

// Lock-free: CAS-prepend at the slot head, no lock. Uniform writes almost never collide
// (slots = keys >> threads); a hot key makes every writer retry against the same slot.
double mvccSubmitLockFreeNs(const CostModel& model, const SimParams& params) {
    const double writes = static_cast<double>(params.writesPerCommit);
    const double others = static_cast<double>(params.threads) - 1.0;
    const double hotWrites = params.hotKeyShare * writes;
    const double coldWrites = writes - hotWrites;

    const double coldAttempts = 1.0 + others / static_cast<double>(params.keys);
    const double hotAttempts = 1.0 + others;                   // contend on the one slot
    const double casWork = coldWrites * coldAttempts * model.casNs
                         + hotWrites * hotAttempts * model.casNs;
    const double allocWork = writes * model.allocNs;
    return allocWork + casWork;
}

// --- Multiversion READ costs: version filtering, and reader contention under writes. ---
// A HEAD read resolves at the chain head (O(1)). A reader on an older snapshot must skip the
// versions newer than its snapshot — one hop + tag check each (this is version filtering).
// Lock-free readers are then wait-free: they acquire-load the slot head and walk immutable
// nodes, so concurrent writers never block them. Lock-based readers share the writers' lock,
// so a read queues behind in-flight commit critical sections.

double mvccReadFilteredNs(const CostModel& model, double versionsSkipped) {
    return model.hashNs + 2.0 * model.hopNs + versionsSkipped * (model.hopNs + model.compareNs);
}

double mvccReaderGlobalLockNs(const CostModel& model, const SimParams& params, double versionsSkipped) {
    const double writers = static_cast<double>(params.threads);
    const double writerHold = static_cast<double>(params.writesPerCommit) * model.hopNs; // a commit's lock hold
    const double wait = writers * writerHold;                                            // queue behind writers
    return mvccReadFilteredNs(model, versionsSkipped) + model.lockNs + wait;
}

double mvccReaderStripedNs(const CostModel& model, const SimParams& params, double versionsSkipped) {
    const double writers = static_cast<double>(params.threads);
    const double stripeHold = static_cast<double>(params.writesPerCommit) * model.hopNs
                            / static_cast<double>(model.stripeCount);  // only same-stripe writers block
    const double wait = writers * stripeHold;
    return mvccReadFilteredNs(model, versionsSkipped) + model.lockNs + wait;
}

double mvccReaderLockFreeNs(const CostModel& model, double versionsSkipped) {
    return mvccReadFilteredNs(model, versionsSkipped);  // wait-free: acquire-load + walk
}

// ============================================================================
// HAMT improvement study — closing the HAMT's read / submit / memory gap to the lock-free
// multiversion hash for STRING keys, without surrendering the structural wins (wait-free reads,
// flat time-travel). The models below add, for string keys: a string front-end (hashing and
// comparing a key is O(length), not a flat int cost); dictionary coding (intern once, then key on
// a dense int code); folded / transient commit application (share path-copies across a write-set);
// a width-aware path-copy cost (wide fan-out trades read hops for copy volume); CHAMP node
// compaction (memory); and an Adaptive Radix Tree (ART), a string-native trie that drops hashing.
// ============================================================================

// Hash a whole string key once (paid at the top of any lookup or insert).
double stringHashNs(const CostModel& model, size_t keyLen) {
    return model.strFixedNs + static_cast<double>(keyLen) * model.strHashByteNs;
}

// Compare two string keys (worst case: the full length). Paid to confirm a leaf match, or to
// resolve a hash collision.
double stringCompareNs(const CostModel& model, size_t keyLen) {
    return model.strFixedNs + static_cast<double>(keyLen) * model.strCmpByteNs;
}

// Intern a string into a dense integer dictionary code: hash it, probe the dictionary, confirm the
// bytes once. Thereafter the index keys on the int code, so its internal compares are cheap. This
// is a shared front-end — any code-keyed design (HAMT or multiversion hash) pays it on the read
// boundary, so coding alone does not separate the two.
double dictInternNs(const CostModel& model, size_t keyLen) {
    return stringHashNs(model, keyLen) + model.hopNs + stringCompareNs(model, keyLen);
}

// Expected distinct nodes touched at one level by `writes` random updates, given the level holds
// `bins` nodes (balls into bins). Near the root, bins is small and many writes share a node; deep
// down, bins >> writes and almost every write gets its own node.
double touchedNodesAtLevel(double bins, size_t writes) {
    return bins * (1.0 - std::pow(1.0 - 1.0 / bins, static_cast<double>(writes)));
}

// Total distinct nodes a folded (transient) commit allocates. Instead of `writes * depth`
// independent path-copies, a commit edits one edit-id-tagged private copy in place, so a node
// shared by several writes in the same commit is copied ONCE. Summed over the path's `depth`
// levels; bins at level d = min(branch^d, keys).
double foldedTouchedNodes(size_t keys, size_t branch, size_t depth, size_t writes) {
    double total = 0.0;
    double binsAtLevel = 1.0;  // branch^0 = the root
    for (size_t level = 0; level < depth; ++level) {
        const double keysD = static_cast<double>(keys);
        const double bins = binsAtLevel < keysD ? binsAtLevel : keysD;
        total += touchedNodesAtLevel(bins, writes);
        binsAtLevel *= static_cast<double>(branch);
    }
    return total;
}

// Cost to path-copy one node: read the old node (a cache-miss hop), allocate the replacement, and
// copy its occupied child slots (a streaming memcpy — cheap per slot). Upper nodes are densely
// populated, so we charge a `width`-wide copy: the conservative case that makes wide fan-out's
// write cost visible.
double nodeCopyNs(const CostModel& model, size_t width) {
    return model.hopNs + model.allocNs + static_cast<double>(width) * model.copySlotNs;
}

// --- Read at HEAD, string keys (nanoseconds). ---

// String-keyed HAMT, NOT dictionary-coded: hash the whole key once, chase `depth` immutable nodes,
// then one full byte-wise compare to confirm the leaf.
double hamtStringReadNs(const CostModel& model, size_t depth, size_t keyLen) {
    return stringHashNs(model, keyLen) + static_cast<double>(depth) * model.hopNs + stringCompareNs(model, keyLen);
}

// Dictionary-coded HAMT: intern once, then the trie is keyed by an int code, so the leaf
// confirmation is a single int compare. The trie is navigated by hash bits either way, so coding
// barely moves the HAMT read — its payoff is memory and cheap compares elsewhere, not point reads.
double hamtCodedReadNs(const CostModel& model, size_t depth, size_t keyLen) {
    return dictInternNs(model, keyLen) + static_cast<double>(depth) * model.hopNs + model.compareNs;
}

// String-keyed multiversion hash: hash the string to a slot, two hops to the newest version node,
// confirm with a full string compare against the slot's stored key.
double mvccStringReadNs(const CostModel& model, size_t keyLen) {
    return stringHashNs(model, keyLen) + 2.0 * model.hopNs + stringCompareNs(model, keyLen);
}

// Dictionary-coded multiversion hash: intern to a code, then the flat O(1) slot probe.
double mvccCodedReadNs(const CostModel& model, size_t keyLen) {
    return dictInternNs(model, keyLen) + model.hashNs + 2.0 * model.hopNs;
}

// ART (Adaptive Radix Tree): a string-native trie. No hashing — descend by key bytes (256-way,
// so depth ~ log_256(keys); path compression can shrink it further). A final full-key compare
// confirms the optimistically-skipped bytes. Ordered, so range scans come free.
double artReadNs(const CostModel& model, size_t artDepth, size_t keyLen) {
    return static_cast<double>(artDepth) * model.hopNs + stringCompareNs(model, keyLen);
}

// --- Submit one commit (apply `writes` updates), string keys (nanoseconds). ---

// Naive HAMT submit: `writes` independent path-copies from the same base root, each copying
// `depth` width-aware nodes, then one publish CAS. This is the prior model's assumption.
double hamtNaiveSubmitNs(const CostModel& model, size_t branch, size_t depth, size_t writes) {
    return static_cast<double>(writes) * static_cast<double>(depth) * nodeCopyNs(model, branch) + model.casNs;
}

// Folded (transient) HAMT submit: apply the whole write-set to one edit-tagged private copy, so a
// node shared by several writes is copied once; one publish CAS makes the batch atomically visible.
double hamtFoldedSubmitNs(const CostModel& model, size_t keys, size_t branch, size_t depth, size_t writes) {
    const double nodes = foldedTouchedNodes(keys, branch, depth, writes);
    return nodes * nodeCopyNs(model, branch) + model.casNs;
}

// ART submit: folded path-copy of adaptive nodes. ART's wide upper nodes (a dense subtree promotes
// to Node256) cost as much to copy as a wide HAMT node, so charge the effective fan-out of a
// balanced 256-ary trie — keys^(1/depth), capped at the 256-way max and floored at a small adaptive
// node. ART's advantage on writes is the shallower tree (fewer levels to copy), not cheaper nodes.
double artFoldedSubmitNs(const CostModel& model, size_t keys, size_t artDepth, size_t writes) {
    const double fanout = std::pow(static_cast<double>(keys), 1.0 / static_cast<double>(artDepth));
    const double clamped = std::min(256.0, std::max(4.0, std::ceil(fanout)));
    const size_t width = static_cast<size_t>(clamped);
    const double nodes = foldedTouchedNodes(keys, 256, artDepth, writes);
    return nodes * nodeCopyNs(model, width) + model.casNs;
}

// Runs the workload once, recording read and submit latency at every commit (part count),
// and writes the full curve to results_sweep.csv. Prints a log-spaced summary table and
// the retained-memory comparison. Returns the final distinct-key count for reporting.
size_t runSweep(const SimParams& params, const CostModel& model) {
    std::vector<uint8_t> present(params.keys, 0);
    std::vector<int64_t> lastWriteLayer(params.keys, -1);
    std::vector<int64_t> lastCommitCounted(params.keys, -1);
    std::vector<uint32_t> partsContaining(params.keys, 0);
    std::vector<uint32_t> writtenCodes;
    writtenCodes.reserve(params.keys);

    SplitMix64 writeRng(params.seed);
    SplitMix64 readRng(params.seed ^ 0xD1B54A32D192ED03ull);

    const std::string sweepPath = params.outDir + "/results_sweep.csv";
    std::ofstream csv(sweepPath);
    csv << "parts,distinct_keys,"
        << "read_hamt_ns,read_btree_ns,read_mvcc_ns,read_layers_ns,"
        << "read_perpart_ns,read_perpart_pruned_ns,read_perpart_compacted_ns,"
        << "submit_hamt_ns,submit_btree_ns,submit_mvcc_ns,submit_layers_ns,"
        << "submit_perpart_ns,submit_perpart_compacted_ns\n";

    // Retained-memory accumulators (bytes), summed across all commits.
    double hamtBytes = 0.0;
    double btreeBytes = 0.0;

    std::vector<size_t> checkpoints {1, 2, 5, 10, 25, 50, 100, 200, 400, 800, 1200, 1600, 2000};
    std::vector<PerPartCheckpoint> perPartRows;

    std::printf("\n  Read / submit latency vs number of parts (modeled nanoseconds)\n");
    std::printf("  %-6s | %-31s | %-31s\n", "", "read (point lookup at HEAD)", "submit (one commit)");
    std::printf("  %-6s | %7s %7s %7s %7s | %7s %7s %7s %7s\n",
                "parts", "hamt", "btree", "mvcc", "layers", "hamt", "btree", "mvcc", "layers");
    std::printf("  -------+---------------------------------+--------------------------------\n");

    for (size_t commit = 0; commit < params.parts; ++commit) {
        // Apply this commit's write set: mark keys present, stamp their newest layer.
        for (size_t w = 0; w < params.writesPerCommit; ++w) {
            const bool isHot = params.hotKeyShare > 0.0
                            && (static_cast<double>(writeRng.nextIndex(1000)) / 1000.0) < params.hotKeyShare;
            const size_t code = isHot ? 0 : writeRng.nextIndex(params.keys);
            if (present[code] == 0) {
                present[code] = 1;
                writtenCodes.push_back(static_cast<uint32_t>(code));
            }
            lastWriteLayer[code] = static_cast<int64_t>(commit);

            // Count the distinct parts (commits) that contain this code, for the
            // per-DataPart collect-at-read fan-out.
            if (lastCommitCounted[code] != static_cast<int64_t>(commit)) {
                lastCommitCounted[code] = static_cast<int64_t>(commit);
                partsContaining[code] += 1;
            }
        }

        const size_t distinct = writtenCodes.size();
        const size_t hamtDepth = depthFor(distinct, model.hamtBranch);
        const size_t btreeDepth = depthFor(distinct, model.btreeOrder);

        // Submit latencies for this commit (multiversion shown uncontended here; the
        // concurrency section adds the derived wait time).
        const double submitHamt = hamtSubmitNs(model, hamtDepth, params.writesPerCommit);
        const double submitBtree = btreeSubmitNs(model, btreeDepth, params.writesPerCommit);
        const double submitMvcc = mvccSubmitBaseNs(model, params.writesPerCommit);
        const double submitLayers = layersSubmitNs(model, params.writesPerCommit);

        hamtBytes += static_cast<double>(params.writesPerCommit * hamtDepth) * model.hamtNodeBytes;
        btreeBytes += static_cast<double>(params.writesPerCommit * btreeDepth) * model.btreeNodeBytes;

        // Read latencies: sample present keys to get the real layers-walked distribution.
        double layersWalkedSum = 0.0;
        double matchingPartsSum = 0.0;
        for (size_t r = 0; r < params.readsPerCheckpoint; ++r) {
            const size_t pick = readRng.nextIndex(writtenCodes.size());
            const size_t code = writtenCodes[pick];
            layersWalkedSum += static_cast<double>(static_cast<int64_t>(commit) - lastWriteLayer[code] + 1);
            matchingPartsSum += static_cast<double>(partsContaining[code]);
        }
        const double layersWalked = layersWalkedSum / static_cast<double>(params.readsPerCheckpoint);
        const double avgMatchingParts = matchingPartsSum / static_cast<double>(params.readsPerCheckpoint);

        const double readHamt = hamtReadNs(model, hamtDepth);
        const double readBtree = btreeReadNs(model, btreeDepth);
        const double readMvcc = mvccReadNs(model);
        const double readLayers = layersReadNs(model, layersWalked, params.bloom);

        const size_t parts = commit + 1;
        const double partsD = static_cast<double>(parts);
        const double compactTarget = static_cast<double>(params.compactParts);
        const double readPerPart = perPartReadCollectNs(model, partsD, avgMatchingParts);
        const double readPerPartPruned = perPartReadPrunedNs(model, partsD, avgMatchingParts);
        const double readPerPartCompacted = perPartReadCompactedNs(model, partsD, compactTarget, avgMatchingParts);
        const double submitPerPart = perPartSubmitNs(model, params.writesPerCommit);
        const double submitPerPartCompacted = perPartSubmitCompactedNs(model, params.writesPerCommit, partsD, compactTarget);

        csv << parts << ',' << distinct << ','
            << readHamt << ',' << readBtree << ',' << readMvcc << ',' << readLayers << ','
            << readPerPart << ',' << readPerPartPruned << ',' << readPerPartCompacted << ','
            << submitHamt << ',' << submitBtree << ',' << submitMvcc << ',' << submitLayers << ','
            << submitPerPart << ',' << submitPerPartCompacted << '\n';

        const bool isCheckpoint = std::find(checkpoints.begin(), checkpoints.end(), parts) != checkpoints.end();
        if (isCheckpoint) {
            std::printf("  %6zu | %7.0f %7.0f %7.0f %7.0f | %7.0f %7.0f %7.0f %7.0f\n",
                        parts,
                        readHamt, readBtree, readMvcc, readLayers,
                        submitHamt, submitBtree, submitMvcc, submitLayers);
            perPartRows.push_back(PerPartCheckpoint{parts, readPerPart, readPerPartPruned, readPerPartCompacted,
                                                    submitPerPart, submitPerPartCompacted, readMvcc});
        }
    }

    csv.close();
    std::printf("\n  Full per-part curve written to %s\n", sweepPath.c_str());

    std::printf("\n  Per-DataPart index — collect at read; compaction bounds live parts to K=%zu (LSM / ClickHouse)\n",
                params.compactParts);
    std::printf("    %-6s | %11s %11s %11s | %10s %11s | %8s\n",
                "parts", "collect", "+prune", "compact", "submit", "submit+cmp", "mvcc");
    std::printf("    -------+-------------------------------------+--------------------------+---------\n");
    for (const PerPartCheckpoint& row : perPartRows) {
        std::printf("    %6zu | %8.0f ns %8.0f ns %8.0f ns | %7.0f ns %8.0f ns | %5.0f ns\n",
                    row.parts, row.readCollect, row.readPruned, row.readCompacted,
                    row.submit, row.submitCompacted, row.mvccRead);
    }

    // Retained-memory comparison after the final commit.
    const size_t distinctFinal = writtenCodes.size();
    const double mvccBytes = static_cast<double>(params.parts * params.writesPerCommit) * model.versionNodeBytes
                           + static_cast<double>(params.keys) * 8.0;
    const double layersBytes = static_cast<double>(params.parts)
                             * (model.layerHeaderBytes + static_cast<double>(params.writesPerCommit) * model.layerEntryBytes);
    const double naiveFullCopyBytes = static_cast<double>(params.parts)
                                    * static_cast<double>(distinctFinal) * model.layerEntryBytes;

    const double toMiB = 1.0 / (1024.0 * 1024.0);
    std::printf("\n  Retained memory after %zu parts (estimated)\n", params.parts);
    std::printf("    HAMT (shared paths)        %8.1f MiB\n", hamtBytes * toMiB);
    std::printf("    COW B-tree (shared paths)  %8.1f MiB\n", btreeBytes * toMiB);
    std::printf("    Multiversion hash          %8.1f MiB\n", mvccBytes * toMiB);
    std::printf("    COW layers                 %8.1f MiB\n", layersBytes * toMiB);
    std::printf("    Per-DataPart indexes       %8.1f MiB  (same storage as COW layers; read differs)\n", layersBytes * toMiB);
    std::printf("    (naive full copy/commit)   %8.1f MiB  <- what structural sharing avoids\n",
                naiveFullCopyBytes * toMiB);

    return distinctFinal;
}

// Derives multiversion submit latency vs writer-thread count for the three concurrency
// policies, from the fixed cost constants. No real threads are spawned.
void runConcurrency(const SimParams& params, const CostModel& model) {
    const std::string path = params.outDir + "/results_concurrency.csv";
    std::ofstream csv(path);
    csv << "threads,submit_global_ns,submit_striped_ns,submit_lockfree_ns\n";

    std::printf("\n  Multiversion submit latency vs concurrent writers (derived wait time)\n");
    std::printf("    keys=%zu writes/commit=%zu stripes=%zu hotKeyShare=%.2f\n",
                params.keys, params.writesPerCommit, model.stripeCount, params.hotKeyShare);
    std::printf("    %-8s | %12s %12s %12s\n", "threads", "global", "striped", "lock-free");
    std::printf("    ---------+--------------------------------------\n");

    std::vector<size_t> threadCounts {1, 2, 4, 8, 16, 32, 64};
    for (const size_t threadCount : threadCounts) {
        SimParams scaled = params;
        scaled.threads = threadCount;

        const double global = mvccSubmitGlobalLockNs(model, scaled);
        const double striped = mvccSubmitStripedNs(model, scaled);
        const double lockFree = mvccSubmitLockFreeNs(model, scaled);

        csv << threadCount << ',' << global << ',' << striped << ',' << lockFree << '\n';
        std::printf("    %8zu | %10.0f ns %10.0f ns %10.0f ns\n", threadCount, global, striped, lockFree);
    }

    csv.close();
    std::printf("\n  Curve written to %s\n", path.c_str());
}

// Models the two read-side costs the submit section omits: reader contention against concurrent
// writers (by policy), and version filtering (walking past versions newer than the reader's snapshot).
void runMvccReads(const SimParams& params, const CostModel& model) {
    const std::string path = params.outDir + "/results_mvcc_reads.csv";
    std::ofstream csv(path);
    csv << "threads,reader_global_ns,reader_striped_ns,reader_lockfree_ns\n";

    std::printf("\n  Multiversion READS — reader latency vs concurrent writers (HEAD read, no version filtering)\n");
    std::printf("    %-8s | %12s %12s %12s\n", "threads", "global", "striped", "lock-free");
    std::printf("    ---------+--------------------------------------\n");
    std::vector<size_t> threadCounts {1, 2, 4, 8, 16, 32, 64};
    for (const size_t threadCount : threadCounts) {
        SimParams scaled = params;
        scaled.threads = threadCount;
        const double readerGlobal = mvccReaderGlobalLockNs(model, scaled, 0.0);
        const double readerStriped = mvccReaderStripedNs(model, scaled, 0.0);
        const double readerLockFree = mvccReaderLockFreeNs(model, 0.0);
        csv << threadCount << ',' << readerGlobal << ',' << readerStriped << ',' << readerLockFree << '\n';
        std::printf("    %8zu | %10.0f ns %10.0f ns %10.0f ns\n", threadCount, readerGlobal, readerStriped, readerLockFree);
    }
    csv.close();

    std::printf("\n  Multiversion READS — version filtering cost vs versions skipped (reader behind HEAD)\n");
    std::printf("    %-10s | %13s\n", "skipped", "read latency");
    std::printf("    -----------+--------------\n");
    std::vector<size_t> depths {0, 1, 2, 5, 10, 50, 100, 500, 1000, 2000};
    for (const size_t depth : depths) {
        std::printf("    %10zu | %10.0f ns\n", depth, mvccReadFilteredNs(model, static_cast<double>(depth)));
    }

    std::printf("\n  Across designs (read side): copy-on-write / persistent indexes publish each version with a\n");
    std::printf("    single atomic swap, so their reads are WAIT-FREE under concurrent writers (flat at the base\n");
    std::printf("    read cost). For an as-of read N commits back: persistent trees pay the HEAD cost (root lookup),\n");
    std::printf("    per-DataPart / COW layers pay their read cost at the part count that existed then (read it off\n");
    std::printf("    the sweep), and only the multiversion table rises (the filtering curve above).\n");
    std::printf("\n  Reader-contention curve written to %s\n", path.c_str());
}

// HAMT improvement study, part 1: read / submit / memory of the improved HAMT variants against
// the lock-free multiversion hash, as parts scale. Tracks only the distinct-key count (the only
// workload input a HEAD point-lookup needs), so it re-derives the same key progression as runSweep.
void runHamtVariants(const SimParams& params, const CostModel& model) {
    std::vector<uint8_t> present(params.keys, 0);
    size_t distinct = 0;
    SplitMix64 writeRng(params.seed);

    const size_t keyLen = params.keyLenBytes;
    const size_t branch = model.hamtBranch;
    const std::string path = params.outDir + "/results_hamt_variants.csv";
    std::ofstream csv(path);
    csv << "parts,distinct_keys,"
        << "read_hamt_string_ns,read_hamt_coded_ns,read_art_ns,read_mvcc_string_ns,read_mvcc_coded_ns,"
        << "submit_hamt_naive_ns,submit_hamt_folded_ns,submit_art_ns,submit_mvcc_ns,"
        << "mem_hamt_folded_mib,mem_champ_folded_mib,mem_art_mib\n";

    const double toMiB = 1.0 / (1024.0 * 1024.0);
    double hamtFoldedBytes = 0.0;
    double champFoldedBytes = 0.0;
    double artBytes = 0.0;

    const std::vector<size_t> checkpoints {1, 10, 100, 400, 800, 1200, 1600, 2000};
    std::printf("\n=== HAMT improvement study (string keys, len=%zu B) — variants vs lock-free multiversion hash ===\n",
                keyLen);
    std::printf("\n  Read at HEAD (point lookup, ns) — the lower the better\n");
    std::printf("    %-6s | %10s %10s %8s | %11s %10s\n",
                "parts", "hamt-str", "hamt-code", "art", "mvcc-str", "mvcc-code");
    std::printf("    -------+------------------------------+------------------------\n");

    std::vector<std::string> submitRows;
    std::vector<std::string> memRows;

    for (size_t commit = 0; commit < params.parts; ++commit) {
        for (size_t w = 0; w < params.writesPerCommit; ++w) {
            const size_t code = writeRng.nextIndex(params.keys);
            if (present[code] == 0) {
                present[code] = 1;
                ++distinct;
            }
        }

        const size_t hamtDepth = depthFor(distinct, branch);
        const size_t artDepth = depthFor(distinct, 256);

        const double foldedHamtNodes = foldedTouchedNodes(distinct, branch, hamtDepth, params.writesPerCommit);
        const double foldedArtNodes = foldedTouchedNodes(distinct, 256, artDepth, params.writesPerCommit);
        hamtFoldedBytes += foldedHamtNodes * model.hamtNodeBytes;
        champFoldedBytes += foldedHamtNodes * model.champNodeBytes;
        artBytes += foldedArtNodes * model.artNodeBytes;

        const double readHamtString = hamtStringReadNs(model, hamtDepth, keyLen);
        const double readHamtCoded = hamtCodedReadNs(model, hamtDepth, keyLen);
        const double readArt = artReadNs(model, artDepth, keyLen);
        const double readMvccString = mvccStringReadNs(model, keyLen);
        const double readMvccCoded = mvccCodedReadNs(model, keyLen);

        const double submitHamtNaive = hamtNaiveSubmitNs(model, branch, hamtDepth, params.writesPerCommit);
        const double submitHamtFolded = hamtFoldedSubmitNs(model, distinct, branch, hamtDepth, params.writesPerCommit);
        const double submitArt = artFoldedSubmitNs(model, distinct, artDepth, params.writesPerCommit);
        const double submitMvcc = mvccSubmitBaseNs(model, params.writesPerCommit);

        const size_t parts = commit + 1;
        csv << parts << ',' << distinct << ','
            << readHamtString << ',' << readHamtCoded << ',' << readArt << ','
            << readMvccString << ',' << readMvccCoded << ','
            << submitHamtNaive << ',' << submitHamtFolded << ',' << submitArt << ',' << submitMvcc << ','
            << hamtFoldedBytes * toMiB << ',' << champFoldedBytes * toMiB << ',' << artBytes * toMiB << '\n';

        const bool isCheckpoint = std::find(checkpoints.begin(), checkpoints.end(), parts) != checkpoints.end();
        if (isCheckpoint) {
            std::printf("    %6zu | %10.0f %10.0f %8.0f | %11.0f %10.0f\n",
                        parts, readHamtString, readHamtCoded, readArt, readMvccString, readMvccCoded);
            char buf[256];
            std::snprintf(buf, sizeof(buf), "    %6zu | %12.0f %12.0f %10.0f | %10.0f",
                          parts, submitHamtNaive, submitHamtFolded, submitArt, submitMvcc);
            submitRows.emplace_back(buf);
            std::snprintf(buf, sizeof(buf), "    %6zu | %12.1f %12.1f %10.1f",
                          parts, hamtFoldedBytes * toMiB, champFoldedBytes * toMiB, artBytes * toMiB);
            memRows.emplace_back(buf);
        }
    }

    std::printf("\n  Submit one commit (%zu writes, ns) — folded shares path-copies across the write-set\n",
                params.writesPerCommit);
    std::printf("    %-6s | %12s %12s %10s | %10s\n",
                "parts", "hamt-naive", "hamt-folded", "art", "mvcc");
    std::printf("    -------+--------------------------------------+-----------\n");
    for (const std::string& row : submitRows) {
        std::printf("%s\n", row.c_str());
    }

    std::printf("\n  Retained memory (MiB) — folding fewer node-copies and CHAMP's compact nodes both cut it\n");
    std::printf("    %-6s | %12s %12s %10s\n", "parts", "hamt-fold", "champ-fold", "art");
    std::printf("    -------+--------------------------------------\n");
    for (const std::string& row : memRows) {
        std::printf("%s\n", row.c_str());
    }

    csv.close();
    std::printf("\n  Curve written to %s\n", path.c_str());
}

// HAMT improvement study, part 2: fan-out sweep. Wider nodes shrink the tree (fewer cache-miss
// hops on read) but cost more to path-copy (more occupied slots per node) and more memory. Shows
// the branch factor at which the HAMT's HEAD read reaches lock-free multiversion-hash parity.
void runFanoutSweep(const SimParams& params, const CostModel& model) {
    const std::string path = params.outDir + "/results_fanout.csv";
    std::ofstream csv(path);
    csv << "branch,depth,read_coded_ns,submit_folded_ns,nodes_per_commit\n";

    const size_t keyLen = params.keyLenBytes;
    const double mvccCoded = mvccCodedReadNs(model, keyLen);

    std::printf("\n=== Fan-out sweep (keys=%zu, %zu writes/commit) — read hops vs path-copy volume ===\n",
                params.keys, params.writesPerCommit);
    std::printf("  lock-free multiversion-hash coded read = %.0f ns (the target to reach)\n", mvccCoded);
    std::printf("  %-7s | %5s | %12s | %14s | %s\n",
                "branch", "depth", "read (ns)", "submit (ns)", "nodes/commit");
    std::printf("  --------+-------+--------------+----------------+-------------\n");

    const std::vector<size_t> branches {16, 32, 64, 128, 256, 512, 1024};
    for (const size_t branch : branches) {
        const size_t depth = depthFor(params.keys, branch);
        const double readCoded = hamtCodedReadNs(model, depth, keyLen);
        const double folded = foldedTouchedNodes(params.keys, branch, depth, params.writesPerCommit);
        const double submitFolded = hamtFoldedSubmitNs(model, params.keys, branch, depth, params.writesPerCommit);

        csv << branch << ',' << depth << ',' << readCoded << ',' << submitFolded << ',' << folded << '\n';
        const char* marker = readCoded <= mvccCoded ? "  <- reaches mvcc read parity" : "";
        std::printf("  %7zu | %5zu | %9.0f ns | %11.0f ns | %8.0f%s\n",
                    branch, depth, readCoded, submitFolded, folded, marker);
    }
    csv.close();
    std::printf("\n  Curve written to %s\n", path.c_str());
}

// HAMT improvement study, part 3: key-length sweep. The string front-end (hashing and comparing a
// key) is O(length); a cache-miss hop is flat. Shows where string costs start to rival a hop, and
// why dictionary coding's read payoff is small (it trades a string compare for an extra intern hop).
void runStringFrontend(const SimParams& params, const CostModel& model) {
    const std::string path = params.outDir + "/results_string_frontend.csv";
    std::ofstream csv(path);
    csv << "key_len_bytes,read_hamt_string_ns,read_hamt_coded_ns,read_art_ns,string_hash_ns,string_cmp_ns\n";

    const size_t depth = depthFor(params.keys, model.hamtBranch);
    const size_t artDepth = depthFor(params.keys, 256);

    std::printf("\n=== String front-end vs key length (keys=%zu, hamt depth=%zu) — one hop = %.0f ns ===\n",
                params.keys, depth, model.hopNs);
    std::printf("  %-8s | %12s %12s %8s | %10s %10s\n",
                "key_len", "hamt-str", "hamt-code", "art", "hash", "compare");
    std::printf("  ---------+------------------------------+----------------------\n");

    const std::vector<size_t> lengths {4, 8, 16, 32, 64, 128, 256};
    for (const size_t keyLen : lengths) {
        const double readHamtString = hamtStringReadNs(model, depth, keyLen);
        const double readHamtCoded = hamtCodedReadNs(model, depth, keyLen);
        const double readArt = artReadNs(model, artDepth, keyLen);
        const double hashCost = stringHashNs(model, keyLen);
        const double cmpCost = stringCompareNs(model, keyLen);

        csv << keyLen << ',' << readHamtString << ',' << readHamtCoded << ',' << readArt << ','
            << hashCost << ',' << cmpCost << '\n';
        std::printf("  %8zu | %12.0f %12.0f %8.0f | %10.1f %10.1f\n",
                    keyLen, readHamtString, readHamtCoded, readArt, hashCost, cmpCost);
    }
    csv.close();
    std::printf("\n  Curve written to %s\n", path.c_str());
}

bool parseSizeArg(int argc, char** argv, int& i, std::string_view flag, size_t& out) {
    if (argv[i] == flag && i + 1 < argc) {
        out = static_cast<size_t>(std::strtoull(argv[++i], nullptr, 10));
        return true;
    }
    return false;
}

bool parseDoubleArg(int argc, char** argv, int& i, std::string_view flag, double& out) {
    if (argv[i] == flag && i + 1 < argc) {
        out = std::strtod(argv[++i], nullptr);
        return true;
    }
    return false;
}

void printHelp() {
    std::printf(
        "index_sim — cost-model simulator for versioned property-index designs\n\n"
        "  --parts N        commits/parts to scale to (default 2000)\n"
        "  --keys N         distinct dictionary codes / cardinality (default 100000)\n"
        "  --writes N       writes per commit (default 32)\n"
        "  --reads N        reads sampled per commit for read latency (default 1000)\n"
        "  --threads N      concurrent writers for the wait-time derivation (default 8)\n"
        "  --compact-parts N  per-DataPart compaction target: keep <= N live parts (default 64)\n"
        "  --key-len N      average string-key length in bytes, for the string front-end (default 16)\n"
        "  --hot F          fraction of writes hitting one hot key, 0..1 (default 0)\n"
        "  --bloom          give the COW-layers approach a per-layer Bloom filter\n"
        "  --out-dir DIR    where to write results_*.csv (default .)\n"
        "  --hop NS         cost of one cache-miss pointer chase (default 80)\n"
        "  --alloc NS       cost of one node allocation (default 60)\n"
        "  --cas NS         cost of one uncontended CAS (default 20)\n"
        "  --lock NS        cost of one uncontended mutex lock+unlock (default 25)\n"
        "  --help           this message\n");
}

} // namespace

int main(int argc, char** argv) {
    SimParams params;
    CostModel model;

    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        size_t sizeValue = 0;
        double doubleValue = 0.0;
        if (arg == "--help" || arg == "-h") {
            printHelp();
            return EXIT_SUCCESS;
        } else if (arg == "--bloom") {
            params.bloom = true;
        } else if (arg == "--out-dir" && i + 1 < argc) {
            params.outDir = argv[++i];
        } else if (parseSizeArg(argc, argv, i, "--parts", sizeValue)) {
            params.parts = sizeValue;
        } else if (parseSizeArg(argc, argv, i, "--keys", sizeValue)) {
            params.keys = sizeValue;
        } else if (parseSizeArg(argc, argv, i, "--writes", sizeValue)) {
            params.writesPerCommit = sizeValue;
        } else if (parseSizeArg(argc, argv, i, "--reads", sizeValue)) {
            params.readsPerCheckpoint = sizeValue;
        } else if (parseSizeArg(argc, argv, i, "--threads", sizeValue)) {
            params.threads = sizeValue;
        } else if (parseSizeArg(argc, argv, i, "--compact-parts", sizeValue)) {
            params.compactParts = sizeValue;
        } else if (parseSizeArg(argc, argv, i, "--key-len", sizeValue)) {
            params.keyLenBytes = sizeValue;
        } else if (parseDoubleArg(argc, argv, i, "--hot", doubleValue)) {
            params.hotKeyShare = doubleValue;
        } else if (parseDoubleArg(argc, argv, i, "--hop", doubleValue)) {
            model.hopNs = doubleValue;
        } else if (parseDoubleArg(argc, argv, i, "--alloc", doubleValue)) {
            model.allocNs = doubleValue;
        } else if (parseDoubleArg(argc, argv, i, "--cas", doubleValue)) {
            model.casNs = doubleValue;
        } else if (parseDoubleArg(argc, argv, i, "--lock", doubleValue)) {
            model.lockNs = doubleValue;
        } else {
            std::printf("unknown argument: %s (try --help)\n", argv[i]);
            return EXIT_FAILURE;
        }
    }

    if (params.parts == 0 || params.keys == 0 || params.writesPerCommit == 0 || params.compactParts == 0) {
        std::printf("parts, keys, writes and compact-parts must all be > 0\n");
        return EXIT_FAILURE;
    }

    std::printf("index_sim — fixed-cost model (ns): hop=%.0f compare=%.0f hash=%.0f alloc=%.0f cas=%.0f lock=%.0f\n",
                model.hopNs, model.compareNs, model.hashNs, model.allocNs, model.casNs, model.lockNs);
    std::printf("workload: parts=%zu keys=%zu writes/commit=%zu reads/checkpoint=%zu\n",
                params.parts, params.keys, params.writesPerCommit, params.readsPerCheckpoint);

    runSweep(params, model);
    runConcurrency(params, model);
    runMvccReads(params, model);

    runHamtVariants(params, model);
    runFanoutSweep(params, model);
    runStringFrontend(params, model);

    return EXIT_SUCCESS;
}
