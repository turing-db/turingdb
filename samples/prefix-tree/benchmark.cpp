#include <chrono>
#include <fstream>
#include <iostream>

#include "prefix-tree.h"
#include "benchmark.h"
#include "utils.h"

void dumpTree(StringApproximatorIndex* trie, const std::string& inPath) {
    std::fstream infile(inPath);
    std::string input;

    while (infile >> input) {
        auto words = preprocess(input);
        for (const auto& word : words) trie->insert(word);
    }
}

void queryTree(StringApproximatorIndex* trie, const std::string& queryPath) {
    std::fstream infile(queryPath);
    std::string input;

    while (infile >> input) {
        trie->find(input);
    }
}

void benchmarkPrefix(const std::string& inputsPath) {
    using std::chrono::high_resolution_clock;

    auto tsIndexPrior = high_resolution_clock::now();
    auto trie = new StringApproximatorIndex();
    dumpTree(trie, inputsPath);
    auto tsIndexPost = high_resolution_clock::now();

    std::chrono::duration<double,std::milli> timeIndexCreation =
        tsIndexPost - tsIndexPrior;

    std::cout << "Prefix Tree Index Creation: " << timeIndexCreation.count() << "ms" << std::endl;
}
