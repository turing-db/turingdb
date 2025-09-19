#pragma once

#include "prefix-tree.h"

#include <string>
#include <unordered_map>

using HashingStringIndexer = std::unordered_map<std::string, std::vector<NodeID>>;

struct QueryResults {
    uint64_t finds{0};
    uint64_t partial_finds{0};
    uint64_t misses{0};
};

void benchmarkPrefix(StringApproximatorIndex* trie,
                     const std::string& inputsPath,
                     const std::string& queryPath);

void benchmarkHash(HashingStringIndexer& index,
                   const std::string& inputsPath,
                   const std::string& queryPath);

void runBenchmarks(const std::string& inputPath, const std::string& queryPath);
