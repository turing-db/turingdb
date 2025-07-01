#include <chrono>
#include <fstream>
#include <iostream>

#include "prefix-tree.h"
#include "benchmark.h"
#include "utils.h"

void dumpTree(StringApproximatorIndex* trie, const std::string& inPath) {
    std::fstream infile(inPath);
    std::string input;

    while (std::getline(infile, input)) {
        if (input == "Inputs" || input.empty()) continue;
        auto words = preprocess(input);
        for (const auto& word : words) trie->insert(word);
    }
}

QueryResults* queryTree(StringApproximatorIndex* trie, const std::string& queryPath) {

    QueryResults* res = new QueryResults;
    
    std::fstream infile(queryPath);
    std::string line;
    while (std::getline(infile, line)) {
        if (line == "Query" || line.empty()) continue;
    
        if (line.front() == '"' && line.back() == '"') {
            line = line.substr(1, line.length() - 2);
        }
        std::vector<std::string> words = preprocess(line);
        for (const auto& word : words){
            using Trie = StringApproximatorIndex;
            auto it = trie->find(word);
            if (it._result == Trie::FOUND) res->finds++;
            else if (it._result == Trie::FOUND_PREFIX) res->partial_finds++;
            else res->misses++;
        }
    }
    return res;
}

void benchmarkPrefix(const std::string& inputsPath, const std::string& queryPath) {
    using std::chrono::high_resolution_clock;

    auto tsIndexPrior = high_resolution_clock::now();
    auto trie = new StringApproximatorIndex();
    dumpTree(trie, inputsPath);
    auto tsIndexPost = high_resolution_clock::now();

    std::chrono::duration<double,std::milli> timeIndexCreation =
        tsIndexPost - tsIndexPrior;
    std::cout << "Prefix Tree Index Creation: " << timeIndexCreation.count() << "ms" << std::endl;

    auto tsQueryPrior = high_resolution_clock::now();
    QueryResults* results = queryTree(trie, queryPath);
    auto tsQueryPost = high_resolution_clock::now();

    std::chrono::duration<double,std::milli> timeQuery =
        tsQueryPost - tsQueryPrior;
    std::cout << "Prefix Tree Query: " << timeQuery.count() << "ms" << std::endl;
    std::cout << "with: " << std::endl;
    std::cout << results->finds << " complete matches" << std::endl;
    std::cout << results->partial_finds << " partial matches" << std::endl;
    std::cout << results->misses << " misses" << std::endl;
}
