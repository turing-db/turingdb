#include <chrono>
#include <iostream>
#include <random>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <spdlog/spdlog.h>

#include "RWSpinLock.h"
#include "indexes/HAMTIndex.h"
#include "indexes/HAMTIndexManager.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace {
    void lookup(const auto& map, auto key)  {
        [[maybe_unused]] volatile auto x = map.find(key);
    }
}

static const auto now = []() -> auto {
    return std::chrono::high_resolution_clock::now();
};

static const auto separator = []() {
    spdlog::info("");
    spdlog::info("----------------------------------------");
    spdlog::info("");
};

static constexpr auto printTime = [](std::string_view label, const auto& time) -> void {
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();
    if (ns < 1'000) {
        spdlog::info("{}: {}ns", label, ns);
    } else if (ns < 1'000'000) {
        spdlog::info("{}: {}us", label, ns / 1'000.0);
    } else if (ns < 1'000'000'000) {
        spdlog::info("{}: {}ms", label, ns / 1'000'000.0);
    } else {
        spdlog::info("{}: {}s", label, ns / 1'000'000'000.0);
    }
};

void basictest() {
    spdlog::info("basictest: start");

    GraphView view;
    HAMTManager man;
    PropertyTypeID pid {1};

    HAMTIndex<std::string_view, NodeID> index("my index", &man, pid);
    index.init(view);

    std::unordered_map<std::string_view, NodeID> groundTruth;

    const auto insert = [&](std::string_view key, NodeID val) {
        index.exhaustiveMutInsert(key, val);
        groundTruth.emplace(key, val);
    };

    insert("my string", NodeID{101});
    insert("my other string", NodeID{333});
    insert("my third string", NodeID{8333333333333333333});

    for (const auto& [key, expected] : groundTruth) {
        const NodeID* result = index.find(key);
        bioassert(result, "lookup failed for key {}", key);
        bioassert(result->getValue() == expected.getValue(),
                  "value mismatch for key {}: got {}, expected {}",
                  key, result->getValue(), expected.getValue());
    }

    spdlog::info("basictest: ok");
    separator();
}

void loadtest(size_t numPairs, size_t numLookups, bool enableMap = false, double hitRate = 0.8) {
    spdlog::info("loadtest: start ({} pairs, {} lookups, {:.0f}% hit rate)", numPairs, numLookups, hitRate * 100);

    GraphView view;
    HAMTManager man;
    PropertyTypeID pid {1};

    HAMTIndex<types::UInt64::Primitive, NodeID> index("my index", &man, pid);
    index.init(view);

    std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<types::UInt64::Primitive> dist;

    std::vector<types::UInt64::Primitive> keys(numPairs);
    for (auto& k : keys) {
        k = dist(rng);
    }

    RWSpinLock lock;
    std::unordered_map<types::UInt64::Primitive, NodeID> groundTruth;

    {
        const auto start = now();
        for (size_t i = 0; i < numPairs; i++) {
            const NodeID value(keys[i]);
            index.exhaustiveMutInsert(keys[i], value);
            if (enableMap) {
                groundTruth.emplace(keys[i], value);
            }
        }
        const auto taken = now() - start;
        printTime("build", taken);
    }

    // {
    //     std::string input;
    //     spdlog::info("press y to continue with lookups...");
    //     std::getline(std::cin, input);
    //     if (input != "y") {
    //         spdlog::info("aborted");
    //         return;
    //     }
    // }

    std::uniform_int_distribution<size_t> indexDist(0, numPairs - 1);
    std::bernoulli_distribution hitDist(hitRate);

    std::vector<types::UInt64::Primitive> lookupKeys(numLookups);
    for (auto& k : lookupKeys) {
        k = hitDist(rng) ? keys[indexDist(rng)] : dist(rng);
    }

    if (enableMap) {
        size_t correctnessHits = 0;
        size_t correctnessMisses = 0;

        for (size_t i = 0; i < numLookups; i++) {
            const types::UInt64::Primitive key = lookupKeys[i];
            const NodeID* result = index.find(key);

            bool expectedHit = false;
            {
                const auto it = groundTruth.find(key);
                expectedHit = it != groundTruth.end();
            }

            if (expectedHit) {
                bioassert(result, "lookup failed for key {}", key);
                // bioassert(result->getValue() == it->second.getValue(),
                          // "value mismatch for key {}: got {}, expected {}", key,
                          // result->getValue(), it->second.getValue());
                correctnessHits++;
            } else {
                bioassert(!result, "expected miss but got hit for key {}", key);
                correctnessMisses++;
            }
        }

        spdlog::info("correctness: {} hits, {} misses ({:.1f}% hit rate)",
                     correctnessHits, correctnessMisses,
                     100.0 * correctnessHits / numLookups);

        {
            const auto start = now();
            for (size_t i = 0; i < numLookups; i++) {
                index.find(lookupKeys[i]);
            }
            const auto taken = now() - start;
            printTime("hamt lookup total", taken);
            printTime("hamt lookup per query", taken / numLookups);
            spdlog::info("hamt: {} hits, {} misses", correctnessHits, correctnessMisses);
        }

        spdlog::info("");

        {
            const auto start = now();
            for (size_t i = 0; i < numLookups; i++) {
                // add a lock to simulate contention, required for "immutable" tid hashmap
                std::shared_lock<RWSpinLock> mut(lock);
                lookup(groundTruth, lookupKeys[i]);
            }
            const auto taken = now() - start;
            printTime("map lookup total", taken);
            printTime("map lookup per query", taken / numLookups);
            spdlog::info("map: {} hits, {} misses", correctnessHits, correctnessMisses);
        }
    } else {
        const auto start = now();
        for (size_t i = 0; i < numLookups; i++) {
            index.find(lookupKeys[i]);
        }
        const auto taken = now() - start;
        printTime("hamt lookup total", taken);
        printTime("hamt lookup per query", taken / numLookups);
    }

    // {
    //     std::string input;
    //     spdlog::info("press y to continue with teardown...");
    //     std::getline(std::cin, input);
    //     if (input != "y") {
    //         spdlog::info("aborted");
    //         return;
    //     }
    // }

    spdlog::info("loadtest: ok");
    separator();
}

int main() {
    basictest();
    // loadtest(1'000'000, 1'000'000, true);
    // loadtest(1'000, 1'000, true);
    // loadtest(1'000'000, 1'000, true);
    // loadtest(10'000'000, 100'000, true);
    loadtest(1'000'000, 500, true);
}
