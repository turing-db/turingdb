#pragma once

#include "prefix-tree.h"

#include <chrono>
#include <string>

struct QueryResults {
    uint64_t finds{0};
    uint64_t partial_finds{0};
    uint64_t misses{0};
};

void dumpTree(StringApproximatorIndex* trie, const std::string& inPath);
QueryResults* queryTree(StringApproximatorIndex* trie, const std::string& queryPath);
void benchmarkPrefix(const std::string& inputsPath, const std::string& queryPath);
