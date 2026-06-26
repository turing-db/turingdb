// Microbenchmark comparing db::AdaptiveRadixTree<uint64_t> against two hash-table baselines for the
// string-property index read path described in docs/ART.md:
//   - std::unordered_map (with a transparent hash, probed by string_view without allocating).
//   - the multiversion ("MVCC") hash reproduced from the tools/index-sim prototype -- the structure
//     docs/ART.md weighs the ART against. It hashes the whole key (FNV-1a) on every probe, the
//     per-lookup cost the ART avoids.
// Keys are byte strings mapping to a 64-bit value (an entity/row reference).
//
// Benchmarks (each over 1K / 128K / 1M high-cardinality 16-byte keys):
//   Build      - construct the structure from scratch (insert + teardown).
//   FindHit    - serial point lookups of present keys.
//   FindMiss   - serial point lookups of absent, prefix-colliding keys (the case the ART's leaf
//                fingerprint targets; a hash short-circuits at an empty/mismatched slot).
//   FindBatch  - batched-probe throughput: the ART uses its AMAC-pipelined findBatch, the MVCC hash
//                an equivalent 8-way AMAC probe, and std::unordered_map a plain find() loop.
//
// Run with: ./test/storage/bench_storage_adaptiveradixtree

#include "indexes/AdaptiveRadixTree.h"

#include <stdint.h>
#include <algorithm>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <benchmark/benchmark.h>

using db::AdaptiveRadixTree;

namespace {

constexpr size_t KeyLength = 16;
constexpr size_t MaxBatchSize = 4096;

// Transparent hash/equality so std::unordered_map can be looked up by std::string_view with no
// temporary std::string, matching how the ART is probed.
struct StringViewHash {
    using is_transparent = void;
    size_t operator()(std::string_view key) const { return std::hash<std::string_view>{}(key); }
};

struct StringViewEqual {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const { return a == b; }
};

using StringMap = std::unordered_map<std::string, uint64_t, StringViewHash, StringViewEqual>;

// ---------------------------------------------------------------------------------------------
// Multiversion ("MVCC") hash baseline, reproduced from the tools/index-sim prototype -- the structure
// docs/ART.md weighs the ART against. Open-addressed with linear probing; each slot heads a version
// chain, so a write is a push-front of a VersionNode and a HEAD read is the front of the chain. Every
// lookup hashes the whole query key with FNV-1a -- the per-probe cost the ART exists to avoid. One
// version per key here, matching the single value the ART stores.
uint64_t fnv1a(std::string_view key) {
    uint64_t hash = 1469598103934665603ull;
    for (const char byte : key) {
        hash ^= static_cast<uint8_t>(byte);
        hash *= 1099511628211ull;
    }
    return hash;
}

struct MvccVersion {
    uint64_t value {0};
    uint32_t version {0};
    MvccVersion* older {nullptr};
};

struct MvccSlot {
    std::string_view key;
    uint64_t hash {0};
    MvccVersion* head {nullptr};
    bool used {false};
};

class MvccHash {
public:
    explicit MvccHash(size_t expectedKeys);
    ~MvccHash();

    MvccHash(const MvccHash&) = delete;
    MvccHash& operator=(const MvccHash&) = delete;

    void insert(std::string_view key, uint64_t value);
    bool lookupHead(std::string_view key, uint64_t& value) const;

    // 8-way AMAC-pipelined batch probe (hash computed inside, to match a real probe site).
    void lookupBatch(const std::vector<std::string_view>& keys,
                     std::vector<uint8_t>& found,
                     std::vector<uint64_t>& values) const;

private:
    size_t locate(std::string_view key, uint64_t hash) const;
    void grow();

    std::vector<MvccSlot> _slots;
    size_t _mask {0};
    size_t _count {0};
};

MvccHash::MvccHash(size_t expectedKeys) {
    size_t capacity = 16;
    while (capacity < expectedKeys * 2) {
        capacity <<= 1;
    }
    _slots.assign(capacity, MvccSlot{});
    _mask = capacity - 1;
}

MvccHash::~MvccHash() {
    for (const MvccSlot& slot : _slots) {
        MvccVersion* node = slot.head;
        while (node != nullptr) {
            MvccVersion* older = node->older;
            delete node;
            node = older;
        }
    }
}

size_t MvccHash::locate(std::string_view key, uint64_t hash) const {
    size_t i = hash & _mask;
    while (_slots[i].used && !(_slots[i].hash == hash && _slots[i].key == key)) {
        i = (i + 1) & _mask;
    }
    return i;
}

void MvccHash::grow() {
    std::vector<MvccSlot> old;
    old.swap(_slots);
    _slots.assign(old.size() * 2, MvccSlot{});
    _mask = _slots.size() - 1;

    for (const MvccSlot& slot : old) {
        if (slot.used) {
            size_t i = slot.hash & _mask;
            while (_slots[i].used) {
                i = (i + 1) & _mask;
            }
            _slots[i] = slot;
        }
    }
}

void MvccHash::insert(std::string_view key, uint64_t value) {
    const bool tooFull = (_count + 1) * 10 >= _slots.size() * 7;   // grow past ~70% load factor
    if (tooFull) {
        grow();
    }

    const uint64_t hash = fnv1a(key);
    MvccSlot& slot = _slots[locate(key, hash)];
    if (!slot.used) {
        slot.used = true;
        slot.key = key;
        slot.hash = hash;
        ++_count;
    }
    slot.head = new MvccVersion{value, 0, slot.head};
}

bool MvccHash::lookupHead(std::string_view key, uint64_t& value) const {
    const MvccSlot& slot = _slots[locate(key, fnv1a(key))];
    if (!slot.used || slot.head == nullptr) {
        return false;
    }
    value = slot.head->value;
    return true;
}

void MvccHash::lookupBatch(const std::vector<std::string_view>& keys,
                           std::vector<uint8_t>& found,
                           std::vector<uint64_t>& values) const {
    constexpr int Width = 8;
    const size_t count = keys.size();
    found.resize(count);
    values.resize(count);

    const MvccSlot* slots = _slots.data();
    size_t index[Width];
    uint64_t hash[Width];
    std::string_view key[Width];
    bool matched[Width];   // false: still probing for the slot; true: slot found, read its head
    bool done[Width];

    for (size_t base = 0; base < count; base += Width) {
        const int lanes = static_cast<int>(std::min<size_t>(Width, count - base));
        int remaining = lanes;

        for (int j = 0; j < lanes; ++j) {
            key[j] = keys[base + j];
            hash[j] = fnv1a(key[j]);
            index[j] = hash[j] & _mask;
            matched[j] = false;
            done[j] = false;
            found[base + j] = 0;
            __builtin_prefetch(&slots[index[j]]);
        }

        while (remaining > 0) {
            for (int j = 0; j < lanes; ++j) {
                if (done[j]) {
                    continue;
                }

                const MvccSlot& slot = slots[index[j]];
                if (!matched[j]) {
                    if (!slot.used) {
                        done[j] = true;
                        --remaining;
                        continue;
                    }
                    if (slot.hash == hash[j] && slot.key == key[j]) {
                        __builtin_prefetch(slot.head);
                        matched[j] = true;
                        continue;
                    }
                    index[j] = (index[j] + 1) & _mask;
                    __builtin_prefetch(&slots[index[j]]);
                    continue;
                }

                if (slot.head != nullptr) {
                    values[base + j] = slot.head->value;
                    found[base + j] = 1;
                }
                done[j] = true;
                --remaining;
            }
        }
    }
}

struct Dataset {
    std::vector<std::string> keys;
    std::vector<std::string> missKeys;
    std::vector<size_t> hitOrder;    // shuffled indices into keys
    std::vector<size_t> missOrder;   // shuffled indices into missKeys
};

Dataset buildDataset(size_t count) {
    std::mt19937_64 rng(0x9E3779B97F4A7C15ull ^ count);
    std::unordered_set<std::string> seen;
    seen.reserve(count * 2);

    Dataset dataset;
    dataset.keys.reserve(count);
    while (dataset.keys.size() < count) {
        std::string key(KeyLength, '\0');
        for (size_t i = 0; i < KeyLength; ++i) {
            key[i] = static_cast<char>(1 + rng() % 255);   // avoid 0x00 (reserved end-of-key marker)
        }
        if (seen.insert(key).second) {
            dataset.keys.push_back(std::move(key));
        }
    }

    // Prefix-colliding miss keys: change only the last byte of each present key, keeping it absent.
    dataset.missKeys.reserve(count);
    for (const std::string& key : dataset.keys) {
        std::string miss = key;
        const uint8_t last = static_cast<uint8_t>(miss.back());
        miss.back() = static_cast<char>(last == 1 ? 2 : 1);
        if (seen.find(miss) == seen.end()) {
            dataset.missKeys.push_back(std::move(miss));
        }
    }

    dataset.hitOrder.resize(dataset.keys.size());
    for (size_t i = 0; i < dataset.hitOrder.size(); ++i) {
        dataset.hitOrder[i] = i;
    }
    std::shuffle(dataset.hitOrder.begin(), dataset.hitOrder.end(), rng);

    dataset.missOrder.resize(dataset.missKeys.size());
    for (size_t i = 0; i < dataset.missOrder.size(); ++i) {
        dataset.missOrder[i] = i;
    }
    std::shuffle(dataset.missOrder.begin(), dataset.missOrder.end(), rng);

    return dataset;
}

// Datasets are generated once per key count and shared across the benchmarks that use them.
const Dataset& dataset(size_t count) {
    static std::unordered_map<size_t, Dataset> cache;
    auto it = cache.find(count);
    if (it == cache.end()) {
        it = cache.emplace(count, buildDataset(count)).first;
    }
    return it->second;
}

void fillTree(AdaptiveRadixTree<uint64_t>& tree, const Dataset& data) {
    for (size_t i = 0; i < data.keys.size(); ++i) {
        tree.insert(data.keys[i], i);
    }
}

void fillMap(StringMap& map, const Dataset& data) {
    map.reserve(data.keys.size());
    for (size_t i = 0; i < data.keys.size(); ++i) {
        map.emplace(data.keys[i], i);
    }
}

void fillMvcc(MvccHash& hash, const Dataset& data) {
    for (size_t i = 0; i < data.keys.size(); ++i) {
        hash.insert(data.keys[i], i);
    }
}

void BM_ART_Build(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    for (auto _ : state) {
        AdaptiveRadixTree<uint64_t> tree;
        fillTree(tree, data);
        benchmark::DoNotOptimize(tree.getSize());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(data.keys.size()));
}

void BM_Map_Build(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    for (auto _ : state) {
        StringMap map;
        fillMap(map, data);
        benchmark::DoNotOptimize(map.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(data.keys.size()));
}

void BM_Mvcc_Build(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    for (auto _ : state) {
        MvccHash hash(data.keys.size());
        fillMvcc(hash, data);
        benchmark::DoNotOptimize(&hash);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(data.keys.size()));
}

void BM_ART_FindHit(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    AdaptiveRadixTree<uint64_t> tree;
    fillTree(tree, data);

    size_t probe = 0;
    for (auto _ : state) {
        uint64_t value = 0;
        const bool found = tree.find(data.keys[data.hitOrder[probe]], value);
        benchmark::DoNotOptimize(found);
        benchmark::DoNotOptimize(value);
        if (++probe == data.hitOrder.size()) {
            probe = 0;
        }
    }
    state.SetItemsProcessed(state.iterations());
}

void BM_Map_FindHit(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    StringMap map;
    fillMap(map, data);

    size_t probe = 0;
    for (auto _ : state) {
        const auto it = map.find(std::string_view(data.keys[data.hitOrder[probe]]));
        const bool found = it != map.end();
        benchmark::DoNotOptimize(found);
        if (found) {
            benchmark::DoNotOptimize(it->second);
        }
        if (++probe == data.hitOrder.size()) {
            probe = 0;
        }
    }
    state.SetItemsProcessed(state.iterations());
}

void BM_Mvcc_FindHit(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    MvccHash hash(data.keys.size());
    fillMvcc(hash, data);

    size_t probe = 0;
    for (auto _ : state) {
        uint64_t value = 0;
        const bool found = hash.lookupHead(data.keys[data.hitOrder[probe]], value);
        benchmark::DoNotOptimize(found);
        benchmark::DoNotOptimize(value);
        if (++probe == data.hitOrder.size()) {
            probe = 0;
        }
    }
    state.SetItemsProcessed(state.iterations());
}

void BM_ART_FindMiss(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    AdaptiveRadixTree<uint64_t> tree;
    fillTree(tree, data);

    size_t probe = 0;
    for (auto _ : state) {
        uint64_t value = 0;
        const bool found = tree.find(data.missKeys[data.missOrder[probe]], value);
        benchmark::DoNotOptimize(found);
        if (++probe == data.missOrder.size()) {
            probe = 0;
        }
    }
    state.SetItemsProcessed(state.iterations());
}

void BM_Map_FindMiss(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    StringMap map;
    fillMap(map, data);

    size_t probe = 0;
    for (auto _ : state) {
        const auto it = map.find(std::string_view(data.missKeys[data.missOrder[probe]]));
        const bool found = it != map.end();
        benchmark::DoNotOptimize(found);
        if (++probe == data.missOrder.size()) {
            probe = 0;
        }
    }
    state.SetItemsProcessed(state.iterations());
}

void BM_Mvcc_FindMiss(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    MvccHash hash(data.keys.size());
    fillMvcc(hash, data);

    size_t probe = 0;
    for (auto _ : state) {
        uint64_t value = 0;
        const bool found = hash.lookupHead(data.missKeys[data.missOrder[probe]], value);
        benchmark::DoNotOptimize(found);
        if (++probe == data.missOrder.size()) {
            probe = 0;
        }
    }
    state.SetItemsProcessed(state.iterations());
}

void BM_ART_FindBatch(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    AdaptiveRadixTree<uint64_t> tree;
    fillTree(tree, data);

    const size_t batchSize = std::min(MaxBatchSize, data.keys.size());
    std::vector<std::string_view> probe;
    probe.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
        probe.push_back(data.keys[data.hitOrder[i]]);
    }

    std::vector<uint8_t> found;
    std::vector<uint64_t> values;
    for (auto _ : state) {
        tree.findBatch(probe, found, values);
        benchmark::DoNotOptimize(found.data());
        benchmark::DoNotOptimize(values.data());
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batchSize));
}

void BM_Map_FindBatch(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    StringMap map;
    fillMap(map, data);

    const size_t batchSize = std::min(MaxBatchSize, data.keys.size());
    std::vector<std::string_view> probe;
    probe.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
        probe.push_back(data.keys[data.hitOrder[i]]);
    }

    for (auto _ : state) {
        for (const std::string_view key : probe) {
            const auto it = map.find(key);
            benchmark::DoNotOptimize(it);
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batchSize));
}

void BM_Mvcc_FindBatch(benchmark::State& state) {
    const Dataset& data = dataset(static_cast<size_t>(state.range(0)));
    MvccHash hash(data.keys.size());
    fillMvcc(hash, data);

    const size_t batchSize = std::min(MaxBatchSize, data.keys.size());
    std::vector<std::string_view> probe;
    probe.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
        probe.push_back(data.keys[data.hitOrder[i]]);
    }

    std::vector<uint8_t> found;
    std::vector<uint64_t> values;
    for (auto _ : state) {
        hash.lookupBatch(probe, found, values);
        benchmark::DoNotOptimize(found.data());
        benchmark::DoNotOptimize(values.data());
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batchSize));
}

}

BENCHMARK(BM_ART_Build)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Map_Build)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Mvcc_Build)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_ART_FindHit)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Map_FindHit)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Mvcc_FindHit)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_ART_FindMiss)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Map_FindMiss)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Mvcc_FindMiss)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_ART_FindBatch)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Map_FindBatch)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Mvcc_FindBatch)->Arg(1000)->Arg(131072)->Arg(1000000);

BENCHMARK_MAIN();
