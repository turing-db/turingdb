#pragma once

#include "prefix-tree.h"

#include <chrono>
#include <string>

void dumpTree(StringApproximatorIndex* trie, const std::string& inPath);
void queryTree(StringApproximatorIndex* trie, const std::string& queryPath);
void benchmarkPrefix(const std::string& inputsPath);
