#include <chrono>
#include <iostream>
#include <random>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <spdlog/spdlog.h>

#include "indexes/HAMTIndex.h"
#include "indexes/HAMTIndexManager.h"
#include "metadata/PropertyType.h"

using namespace db;

static const auto now = []() -> auto {
    return std::chrono::high_resolution_clock::now();
};

static constexpr auto printTime = [](std::string_view label, const auto& time) -> void {
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();
    std::cout << label << ": ";
    if (ns < 1'000) {
        std::cout << ns << "ns";
    } else if (ns < 1'000'000) {
        std::cout << ns / 1'000.0 << "us";
    } else if (ns < 1'000'000'000) {
        std::cout << ns / 1'000'000.0 << "ms";
    } else {
        std::cout << ns / 1'000'000'000.0 << "s";
    }
    std::cout << '\n';
};

void basictest() {
    GraphView view;
    HAMTManager man;
    PropertyTypeID pid {1};

    HAMTIndex<std::string_view, NodeID> index("my index", &man, pid);
    index.init(view);

    {
        std::string_view key {"my string"};
        NodeID val {101};

        index.exhaustiveMutInsert(key, val);

        const NodeID* n = index.find(key);
        bioassert(n, "find {} failed", key);

        spdlog::info("{}", n->getValue());
    }

    {
        std::string_view key {"my other string"};
        NodeID val {333};

        index.exhaustiveMutInsert(key, val);

        const NodeID* n = index.find(key);
        bioassert(n, "find {} failed", key);

        spdlog::info("{}", n->getValue());
    }

    {
        std::string_view key {"my third string"};
        NodeID val {8333333333333333333};

        index.exhaustiveMutInsert(key, val);

        const NodeID* n = index.find(key);
        bioassert(n, "find {} failed", key);

        spdlog::info("{}", n->getValue());
    }
}

void loadtest(size_t numPairs, size_t numLookups) {
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

    std::unordered_map<types::UInt64::Primitive, NodeID> groundTruth;
    groundTruth.reserve(numPairs);

    {
        const auto start = now();
        for (size_t i = 0; i < numPairs; i++) {
            const NodeID value(keys[i]);
            index.exhaustiveMutInsert(keys[i], value);
            groundTruth.emplace(keys[i], value);
        }
        const auto taken = now() - start;
        printTime("build", taken);
    }

    std::uniform_int_distribution<size_t> indexDist(0, numPairs - 1);
    std::vector<types::UInt64::Primitive> lookupKeys(numLookups);
    for (auto& k : lookupKeys) {
        k = keys[indexDist(rng)];
    }

    {
        const auto start = now();
        for (size_t i = 0; i < numLookups; i++) {
            const types::UInt64::Primitive key = lookupKeys[i];
            const NodeID* result = index.find(key);
            bioassert(result, "lookup failed for key {}", key);

            const NodeID& expected = groundTruth.at(key);
            bioassert(result->getValue() == expected.getValue(),
                      "value mismatch for key {}: got {}, expected {}",
                      key, result->getValue(), expected.getValue());
        }
        const auto taken = now() - start;
        printTime("lookup total", taken);

        const auto perLookup = taken / numLookups;
        printTime("lookup per query", perLookup);
    }
}

int main() {
    basictest();
    loadtest(1'000'000, 1'000'000);
}
