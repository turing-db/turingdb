// Microbenchmark comparing db::AdaptiveRadixTree<uint64_t> against std::unordered_map for the
// string-property index read path described in docs/ART.md. Both structures are keyed by byte strings
// and map to a 64-bit value (an entity/row reference). The unordered_map uses a transparent hash so it
// can be probed by std::string_view without allocating, making the comparison fair.
//
// Benchmarks (each over 1K / 128K / 1M high-cardinality 16-byte keys):
//   Build      - construct the structure from scratch (insert + teardown).
//   FindHit    - serial point lookups of present keys.
//   FindMiss   - serial point lookups of absent, prefix-colliding keys (the case the leaf fingerprint
//                targets: they descend to a present leaf and are rejected at the leaf edge).
//   FindBatch  - throughput of a batch of present-key probes; the ART uses its AMAC-pipelined
//                findBatch, the map loops find() (it has no batched API).
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

}

BENCHMARK(BM_ART_Build)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Map_Build)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_ART_FindHit)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Map_FindHit)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_ART_FindMiss)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Map_FindMiss)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_ART_FindBatch)->Arg(1000)->Arg(131072)->Arg(1000000);
BENCHMARK(BM_Map_FindBatch)->Arg(1000)->Arg(131072)->Arg(1000000);

BENCHMARK_MAIN();
