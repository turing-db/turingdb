#include <algorithm>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <random>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <spdlog/spdlog.h>

#include "RWSpinLock.h"
#include "indexes/HAMTIndex.h"
#include "indexes/HAMTIndexManager.h"
#include "indexes/HAMTIndexNode.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace {

constexpr bool inputbrk = false;
constexpr std::ptrdiff_t numThreads = 8;

void lookup(const auto& map, const auto& key) {
    [[maybe_unused]] volatile const auto&& x = map.find(key);
}

struct TxValue {
    NodeID value;
    uint64_t txStart {0};
    uint64_t txEnd {0};
};

struct CorrectnessResult {
    size_t hits {0};
    size_t misses {0};
};

CorrectnessResult checkCorrectness(
    const HAMTIndex<types::UInt64::Primitive, NodeID>& index,
    const std::unordered_map<types::UInt64::Primitive, TxValue>& groundTruth,
    const std::vector<types::UInt64::Primitive>& lookupKeys)
{
    CorrectnessResult result;

    for (const types::UInt64::Primitive key : lookupKeys) {
        const NodeID* found = index.find(key);
        const auto it = groundTruth.find(key);
        const bool expectedHit = it != groundTruth.end();

        if (expectedHit) {
            bioassert(found, "lookup failed for key {}", key);
            bioassert(found->getValue() == it->second.value.getValue(),
                      "value mismatch for key {}: got {}, expected {}",
                      key, found->getValue(), it->second.value.getValue());
            result.hits++;
        } else {
            bioassert(!found, "expected miss but got hit for key {}", key);
            result.misses++;
        }
    }

    const size_t total = result.hits + result.misses;
    spdlog::info("correctness: {} hits, {} misses ({:.1f}% hit rate)",
                 result.hits, result.misses, 100.0 * result.hits / total);

    return result;
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

static void printStats(std::string_view label,
                       const std::vector<std::chrono::nanoseconds>& times,
                       size_t numLookups)
{
    std::chrono::nanoseconds sum {0};
    for (const auto t : times) { sum += t; }
    const auto avg = sum / static_cast<long long>(times.size());
    const auto min = *std::min_element(times.begin(), times.end());
    const auto max = *std::max_element(times.begin(), times.end());
    const long long n = static_cast<long long>(numLookups);

    const std::string base(label);
    printTime(base + " avg",           avg);
    printTime(base + " min",           min);
    printTime(base + " max",           max);
    printTime(base + " per query avg", avg / n);
    printTime(base + " per query min", min / n);
    printTime(base + " per query max", max / n);
};

static void basictest() {
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

static void loadtest(size_t numPairs, size_t numLookups, bool enableMap = false,
              double hitRate = 0.8, double validFraction = 0.8, size_t numRuns = 10) {
    spdlog::info(
        "loadtest: start ({} pairs, {} lookups, {:.0f}% hit rate, {:.0f}% tx valid)",
        numPairs, numLookups, hitRate * 100, validFraction * 100);

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

    static constexpr uint64_t queryTid = 1000;
    static constexpr uint64_t txRange = 1000;

    RWSpinLock lock;
    std::unordered_map<types::UInt64::Primitive, TxValue> groundTruth;

    std::bernoulli_distribution validDist(validFraction);
    std::uniform_int_distribution<uint64_t> validStartDist(0, queryTid);
    std::uniform_int_distribution<uint64_t> validEndDist(queryTid, queryTid + txRange);
    std::uniform_int_distribution<uint64_t> invalidEndDist(0, queryTid - 1);

    size_t validTxCount = 0;

    {
        const auto start = now();
        for (size_t i = 0; i < numPairs; i++) {
            index.exhaustiveMutInsert(keys[i], NodeID(keys[i]));
        }
        const auto taken = now() - start;
        printTime("hamt build", taken);
    }

    if (enableMap) {
        groundTruth.reserve(numPairs);
        const auto start = now();
        for (size_t i = 0; i < numPairs; i++) {
            const bool valid = validDist(rng);
            const uint64_t txStart = valid ? validStartDist(rng) : 0;
            const uint64_t txEnd = valid ? validEndDist(rng) : invalidEndDist(rng);
            groundTruth.emplace(
                keys[i],
                TxValue {.value = NodeID {keys[i]}, .txStart = txStart, .txEnd = txEnd});
            if (valid) { validTxCount++; }
        }
        const auto taken = now() - start;
        printTime("map build", taken);
        spdlog::info("build: {}/{} entries visible at queryTid={} ({:.1f}%)",
                     validTxCount, numPairs, queryTid, 100.0 * validTxCount / numPairs);
    }

    if (inputbrk) {
        std::string input;
        spdlog::info("press y to continue with lookups...");
        std::getline(std::cin, input);
        if (input != "y") {
            spdlog::info("aborted");
            return;
        }
    }

    std::uniform_int_distribution<size_t> indexDist(0, numPairs - 1);
    std::bernoulli_distribution hitDist(hitRate);

    std::vector<types::UInt64::Primitive> lookupKeys(numLookups);
    for (auto& k : lookupKeys) {
        k = hitDist(rng) ? keys[indexDist(rng)] : dist(rng);
    }

    CorrectnessResult correctness;
    if (enableMap) {
        correctness = checkCorrectness(index, groundTruth, lookupKeys);
    }

    // run with tx timestamping
    if (enableMap) {

        {
            spdlog::info("map+tx lookup: {} thread(s), {} run(s)", numThreads, numRuns);

            std::vector<std::chrono::nanoseconds> times;
            times.reserve(numRuns);

            for (size_t run = 0; run < numRuns; run++) {
                std::vector<std::thread> threads;
                threads.reserve(numThreads);

                const size_t chunkSize = numLookups / numThreads;
                const auto start = now();
                for (size_t t = 0; t < numThreads; t++) {
                    const size_t begin = t * chunkSize;
                    const size_t end = (t + 1 == numThreads) ? numLookups : begin + chunkSize;
                    threads.emplace_back([&, begin, end]() {
                        for (size_t i = begin; i < end; i++) {
                            std::shared_lock<RWSpinLock> mut(lock);
                            const auto it = groundTruth.find(lookupKeys[i]);
                            [[maybe_unused]] volatile bool visible =
                                it != groundTruth.end() && it->second.txStart <= queryTid
                                && queryTid <= it->second.txEnd;
                        }
                    });
                }
                for (auto& th : threads) { th.join(); }
                times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(now() - start));
            }

            printStats("map+tx lookup", times, numLookups);
            spdlog::info("map+tx: {} hits, {} misses", correctness.hits, correctness.misses);
        }
        spdlog::info("");
    }

    // run HAMT
    {
        std::vector<std::chrono::nanoseconds> times;
        times.reserve(numRuns);

        for (size_t run = 0; run < numRuns; run++) {
            const auto start = now();
            for (size_t i = 0; i < numLookups; i++) {
                [[maybe_unused]] volatile const NodeID* x = index.find(lookupKeys[i]);
            }
            times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(now() - start));
        }

        if (inputbrk) {
            std::string input;
            spdlog::info("press y to continue with teardown...");
            std::getline(std::cin, input);
            if (input != "y") {
                spdlog::info("aborted");
                return;
            }
        }

        printStats("hamt lookup", times, numLookups);
        if (enableMap) {
            spdlog::info("hamt: {} hits, {} misses", correctness.hits, correctness.misses);
        }
    }

    spdlog::info("");

    // run without tx timestamping
    if (enableMap) {
        spdlog::info("map lookup: {} thread(s), {} run(s)", numThreads, numRuns);

        std::vector<std::chrono::nanoseconds> times;
        times.reserve(numRuns);

        for (size_t run = 0; run < numRuns; run++) {
            std::vector<std::thread> threads;
            threads.reserve(numThreads);

            const size_t chunkSize = numLookups / numThreads;
            const auto start = now();
            for (size_t t = 0; t < numThreads; t++) {
                const size_t begin = t * chunkSize;
                const size_t end = (t + 1 == numThreads) ? numLookups : begin + chunkSize;
                threads.emplace_back([&, begin, end]() {
                    for (size_t i = begin; i < end; i++) {
                        std::shared_lock<RWSpinLock> mut(lock);
                        lookup(groundTruth, lookupKeys[i]);
                    }
                });
            }
            for (auto& th : threads) { th.join(); }
            times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(now() - start));
        }

        printStats("map lookup", times, numLookups);
        spdlog::info("map: {} hits, {} misses", correctness.hits, correctness.misses);
    }

    spdlog::info("loadtest: ok");
    separator();
}

int main() {
    basictest();
    constexpr bool map = true;
    // loadtest(1'000'000, 1'000'000, map);
    // loadtest(1'000, 1'000, map);
    // loadtest(1'000'000, 1'000, map);
    // loadtest(10'000'000, 100'000, map);
    // loadtest(1'000'000, 500, map);
    loadtest(1'000'000, 500, map);
}
