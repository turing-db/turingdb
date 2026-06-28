// Versioned "read as of a commit" microbenchmark: a single ART (per-commit retained roots) vs a
// multiversion hash whose per-key version chain is tagged with the real TuringDB CommitHash, over a
// committed timeline of a dozen commits with moderate per-commit churn.
//
// Fidelity to TuringDB's model (see storage/versioning/): committed history is *linear* -- a branch is a
// transient Change created off some base commit and *rebased* onto the timeline at submit, so committed
// branch history is this linear sequence. Visibility is therefore the VersionController rule: a version
// written at commit V is visible from commit C iff index(V) <= index(C), where the position in the commit
// vector (the `_offsets` map: CommitHash -> index) is the implicit generation number -- an O(1) check.
// "Handling branches" at the committed-index level is exactly this: versions originating from any branch
// are ordered by their rebased commit index and filtered by that one inequality; no per-branch logic.
//
//   - MVCC hash: one open-addressed table; each key's slot heads a version chain (newest first). An
//     as-of read walks the chain and returns the newest version with index(V) <= index(C). Cost grows
//     with lag (how many times the key was rewritten after C).
//   - ART ("a single ART"): one retained root per commit; an as-of read is a single ordinary traversal
//     of commit C's tree -- flat in lag. (Here the per-commit trees are independent; a real COW build
//     would share structure, lowering memory but not changing read latency.)
//
// Run with: ./test/storage/bench_storage_adaptiveradixtree_versioned

#include "indexes/AdaptiveRadixTree.h"
#include "versioning/CommitHash.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <benchmark/benchmark.h>

using db::AdaptiveRadixTree;
using db::CommitHash;

namespace {

constexpr size_t KeyLength = 16;
constexpr size_t CommitCount = 12;
constexpr double UpdateFraction = 0.2;   // each commit after the base rewrites ~20% of the keys

uint64_t fnv1a(std::string_view key) {
    uint64_t hash = 1469598103934665603ull;
    for (const char byte : key) {
        hash ^= static_cast<uint8_t>(byte);
        hash *= 1099511628211ull;
    }
    return hash;
}

// The committed timeline: a linear sequence of real CommitHashes plus the CommitHash -> index map, the
// exact structure VersionController keeps. index(V) <= index(C) is the visibility test.
struct CommitTimeline {
    std::vector<CommitHash> commits;
    std::unordered_map<CommitHash, size_t> offsets;

    void build() {
        commits.reserve(CommitCount);
        for (size_t i = 0; i < CommitCount; ++i) {
            const CommitHash hash = CommitHash::create();   // real random, opaque hash
            commits.push_back(hash);
            offsets.emplace(hash, i);
        }
    }

    size_t indexOf(CommitHash commit) const { return offsets.find(commit)->second; }
};

// ---- multiversion hash with CommitHash-tagged version chains, read as-of a commit ----------------
struct HashVersion {
    CommitHash commit;           // durable tag: which commit wrote this value
    size_t commitIndex {0};      // its position in the timeline (the resolved generation)
    uint64_t value {0};
    HashVersion* older {nullptr};
};

struct HashSlot {
    std::string_view key;
    uint64_t hash {0};
    HashVersion* head {nullptr};
    bool used {false};
};

class VersionedHash {
public:
    explicit VersionedHash(size_t expectedKeys);
    ~VersionedHash();

    VersionedHash(const VersionedHash&) = delete;
    VersionedHash& operator=(const VersionedHash&) = delete;

    void insert(std::string_view key, uint64_t value, CommitHash commit, size_t commitIndex);

    // Newest version visible from a commit at `commitIndex`. `versionsExamined` reports the chain walk.
    bool findAsOf(std::string_view key, size_t commitIndex, uint64_t& value, size_t& versionsExamined) const;

private:
    size_t locate(std::string_view key, uint64_t hash) const;
    void grow();

    std::vector<HashSlot> _slots;
    size_t _mask {0};
    size_t _count {0};
};

VersionedHash::VersionedHash(size_t expectedKeys) {
    size_t capacity = 16;
    while (capacity < expectedKeys * 2) {
        capacity <<= 1;
    }
    _slots.assign(capacity, HashSlot{});
    _mask = capacity - 1;
}

VersionedHash::~VersionedHash() {
    for (const HashSlot& slot : _slots) {
        HashVersion* node = slot.head;
        while (node != nullptr) {
            HashVersion* older = node->older;
            delete node;
            node = older;
        }
    }
}

size_t VersionedHash::locate(std::string_view key, uint64_t hash) const {
    size_t i = hash & _mask;
    while (_slots[i].used && !(_slots[i].hash == hash && _slots[i].key == key)) {
        i = (i + 1) & _mask;
    }
    return i;
}

void VersionedHash::grow() {
    std::vector<HashSlot> old;
    old.swap(_slots);
    _slots.assign(old.size() * 2, HashSlot{});
    _mask = _slots.size() - 1;

    for (const HashSlot& slot : old) {
        if (slot.used) {
            size_t i = slot.hash & _mask;
            while (_slots[i].used) {
                i = (i + 1) & _mask;
            }
            _slots[i] = slot;
        }
    }
}

void VersionedHash::insert(std::string_view key, uint64_t value, CommitHash commit, size_t commitIndex) {
    const bool tooFull = (_count + 1) * 10 >= _slots.size() * 7;
    if (tooFull) {
        grow();
    }

    const uint64_t hash = fnv1a(key);
    HashSlot& slot = _slots[locate(key, hash)];
    if (!slot.used) {
        slot.used = true;
        slot.key = key;
        slot.hash = hash;
        ++_count;
    }
    slot.head = new HashVersion{commit, commitIndex, value, slot.head};
}

bool VersionedHash::findAsOf(std::string_view key,
                             size_t commitIndex,
                             uint64_t& value,
                             size_t& versionsExamined) const {
    const HashSlot& slot = _slots[locate(key, fnv1a(key))];
    versionsExamined = 0;
    if (!slot.used) {
        return false;
    }
    for (const HashVersion* node = slot.head; node != nullptr; node = node->older) {
        ++versionsExamined;
        if (node->commitIndex <= commitIndex) {   // visible from the query commit
            value = node->value;
            return true;
        }
    }
    return false;
}

// ---- the world: keys, timeline, and both structures, built once and shared --------------------------
struct World {
    std::vector<std::string> keys;
    std::vector<size_t> probeOrder;
    CommitTimeline timeline;
    std::unique_ptr<VersionedHash> hash;
    std::vector<std::unique_ptr<AdaptiveRadixTree<uint64_t>>> arts;   // one retained root per commit
};

void verifyWorld(const World& world) {
    // The hash's as-of read must agree with the corresponding commit's ART for every commit.
    std::mt19937_64 rng(7);
    for (size_t trial = 0; trial < 2000; ++trial) {
        const size_t commitIndex = rng() % CommitCount;
        const size_t keyIndex = rng() % world.keys.size();
        uint64_t hashValue = 0;
        uint64_t artValue = 0;
        size_t examined = 0;
        const bool hashFound = world.hash->findAsOf(world.keys[keyIndex], commitIndex, hashValue, examined);
        const bool artFound = world.arts[commitIndex]->find(world.keys[keyIndex], artValue);
        if (hashFound != artFound || (hashFound && hashValue != artValue)) {
            fprintf(stderr, "verifyWorld mismatch at commit %zu key %zu\n", commitIndex, keyIndex);
            abort();
        }
    }
}

World buildWorld(size_t keyCount) {
    std::mt19937_64 rng(0x9E3779B97F4A7C15ull ^ keyCount);

    World world;
    std::unordered_set<std::string> seen;
    seen.reserve(keyCount * 2);
    world.keys.reserve(keyCount);
    while (world.keys.size() < keyCount) {
        std::string key(KeyLength, '\0');
        for (char& c : key) {
            c = static_cast<char>(1 + rng() % 255);
        }
        if (seen.insert(key).second) {
            world.keys.push_back(std::move(key));
        }
    }

    world.timeline.build();
    world.hash = std::make_unique<VersionedHash>(keyCount * 2);

    // Commit 0 writes every key; commits 1..N rewrite a random ~UpdateFraction subset. The hash stores
    // only those deltas; each ART materialises the full state at its commit.
    std::vector<uint64_t> current(keyCount, 0);
    const size_t updatesPerCommit = static_cast<size_t>(keyCount * UpdateFraction);
    for (size_t i = 0; i < CommitCount; ++i) {
        const CommitHash commit = world.timeline.commits[i];
        if (i == 0) {
            for (size_t k = 0; k < keyCount; ++k) {
                current[k] = rng();
                world.hash->insert(world.keys[k], current[k], commit, 0);
            }
        } else {
            for (size_t u = 0; u < updatesPerCommit; ++u) {
                const size_t k = rng() % keyCount;
                current[k] = rng();
                world.hash->insert(world.keys[k], current[k], commit, i);
            }
        }

        auto tree = std::make_unique<AdaptiveRadixTree<uint64_t>>();
        for (size_t k = 0; k < keyCount; ++k) {
            tree->insert(world.keys[k], current[k]);
        }
        world.arts.push_back(std::move(tree));
    }

    world.probeOrder.resize(keyCount);
    for (size_t k = 0; k < keyCount; ++k) {
        world.probeOrder[k] = k;
    }
    std::shuffle(world.probeOrder.begin(), world.probeOrder.end(), rng);

    verifyWorld(world);
    return world;
}

const World& world(size_t keyCount) {
    static std::unordered_map<size_t, World> cache;
    const auto found = cache.find(keyCount);
    if (found != cache.end()) {
        return found->second;
    }
    return cache.emplace(keyCount, buildWorld(keyCount)).first->second;
}

void BM_Hash_AsOf(benchmark::State& state) {
    const World& w = world(static_cast<size_t>(state.range(0)));
    const size_t commitIndex = static_cast<size_t>(state.range(1));

    size_t probe = 0;
    uint64_t walked = 0;
    uint64_t lookups = 0;
    for (auto _ : state) {
        uint64_t value = 0;
        size_t examined = 0;
        benchmark::DoNotOptimize(w.hash->findAsOf(w.keys[w.probeOrder[probe]], commitIndex, value, examined));
        benchmark::DoNotOptimize(value);
        walked += examined;
        ++lookups;
        if (++probe == w.probeOrder.size()) {
            probe = 0;
        }
    }
    state.counters["versions_walked"] = static_cast<double>(walked) / static_cast<double>(lookups);
    state.SetItemsProcessed(state.iterations());
}

void BM_ART_AsOf(benchmark::State& state) {
    const World& w = world(static_cast<size_t>(state.range(0)));
    const size_t commitIndex = static_cast<size_t>(state.range(1));
    const AdaptiveRadixTree<uint64_t>& tree = *w.arts[commitIndex];

    size_t probe = 0;
    for (auto _ : state) {
        uint64_t value = 0;
        benchmark::DoNotOptimize(tree.find(w.keys[w.probeOrder[probe]], value));
        benchmark::DoNotOptimize(value);
        if (++probe == w.probeOrder.size()) {
            probe = 0;
        }
    }
    state.SetItemsProcessed(state.iterations());
}

}

// commit 11 = HEAD (shallow walk), 6 = mid, 0 = oldest (deepest walk); 12 commits.
// 200k keys is the "moderate" cache-resident target; 1M adds the DRAM-bound regime for contrast.
BENCHMARK(BM_Hash_AsOf)
    ->Args({200000, 11})->Args({200000, 6})->Args({200000, 0})
    ->Args({1000000, 11})->Args({1000000, 6})->Args({1000000, 0})
    ->ArgNames({"keys", "commit"});
BENCHMARK(BM_ART_AsOf)
    ->Args({200000, 11})->Args({200000, 6})->Args({200000, 0})
    ->Args({1000000, 11})->Args({1000000, 6})->Args({1000000, 0})
    ->ArgNames({"keys", "commit"});

BENCHMARK_MAIN();
