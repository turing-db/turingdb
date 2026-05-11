#include <chrono>
#include <iostream>
#include <string_view>

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

void loadtest(size_t numPairs) {
    GraphView view;
    HAMTManager man;
    PropertyTypeID pid {1};

    HAMTIndex<types::UInt64::Primitive, NodeID> index("my index", &man, pid);
    index.init(view);

    {
        const auto start = now();
        for (size_t i = 0; i < numPairs; i++) {
            const types::UInt64::Primitive key = i;
            const NodeID value = i;

            index.exhaustiveMutInsert(key, value);
        }
        const auto taken = now() - start;
        printTime("build", taken);
    }
}

int main() {
    basictest();
    loadtest(1'000'000);
}
