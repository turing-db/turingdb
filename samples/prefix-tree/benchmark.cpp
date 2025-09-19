#include <chrono>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include "prefix-tree.h"
#include "benchmark.h"
#include "utils.h"

using HashingStringIndexer = std::unordered_map<std::string, std::vector<NodeID>>;

// Populate hash and tree with data
inline void dump(StringApproximatorIndex* trie,
                 HashingStringIndexer& hash,
                 const std::string& path) {
    std::fstream infile(path);
    std::string input;

    while (std::getline(infile, input)) {
        if (input == "Inputs" || input.empty()) continue;
        std::vector<std::string> words{};
        preprocess(words, input);
        NodeID id{0};
        for (const auto& word : words){
            trie->insert(word);
            hash[word].push_back(id);
        }
    }
    infile.close();
    
}

inline void dumpTree(StringApproximatorIndex* trie, const std::string& inPath) {
    std::fstream infile(inPath);
    std::string input;

    while (std::getline(infile, input)) {
        if (input == "Inputs" || input.empty()) continue;
        std::vector<std::string> words{};
        preprocess(words, input);
        for (const auto& word : words) trie->insert(word);
    }
    infile.close();
}

inline void dumpHash(HashingStringIndexer& hash, const std::string& inPath) {
    std::fstream infile(inPath);
    std::string input;

    NodeID id{0};
    while (std::getline(infile, input)) {
        if (input == "Inputs" || input.empty()) continue;
        std::vector<std::string> words{};
        preprocess(words, input);
        for (const auto& word : words) {
            hash[word].push_back(id);
        }
        id++;
    }
    infile.close();
}

inline QueryResults* queryHash(HashingStringIndexer& hash, const std::string& queryPath) {
    QueryResults* res = new QueryResults;
    
    std::fstream infile(queryPath);
    std::string line;
    while (std::getline(infile, line)) {
        if (line == "Query" || line.empty()) continue;
    
        if (line.front() == '"' && line.back() == '"') {
            line = line.substr(1, line.length() - 2);
        }
        std::vector<std::string> words;
        preprocess(words, line);
        for (const auto& word : words) {
            if (word.empty()) continue;
            auto it = hash.find(word);
            if (it != std::end(hash)) res->finds += hash[word].size();
            else res->misses++;
        }
    }
    return res;
}

inline QueryResults* queryTree(StringApproximatorIndex* trie, const std::string& queryPath) {
    QueryResults* res = new QueryResults;
    
    std::fstream infile(queryPath);
    std::string line;
    while (std::getline(infile, line)) {
        if (line == "Query" || line.empty()) continue;
    
        if (line.front() == '"' && line.back() == '"') {
            line = line.substr(1, line.length() - 2);
        }
        std::vector<std::string> words;
        preprocess(words, line);
        for (const auto& word : words){
            if (word.empty()) continue;
            using Trie = StringApproximatorIndex;
            auto it = trie->find(word);
            if (it._result == Trie::FOUND) {
                size_t numOwners = it._nodePtr->_owners->size();
                res->finds += numOwners;
            }
            else if (it._result == Trie::FOUND_PREFIX) res->partial_finds++;
            else res->misses++;
        }
    }
    infile.close();
    return res;
}

void benchmarkPrefix(StringApproximatorIndex* trie, const std::string& inputsPath, const std::string& queryPath) {
    using std::chrono::high_resolution_clock;

    /*
    auto tsIndexPrior = high_resolution_clock::now();
    dumpTree(trie, inputsPath);
    auto tsIndexPost = high_resolution_clock::now();

    std::chrono::duration<double,std::milli> timeIndexCreation =
        tsIndexPost - tsIndexPrior;
    std::cout << "Prefix Tree Index Creation: " << timeIndexCreation.count() << "ms" << std::endl;
    */

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

void benchmarkHash(HashingStringIndexer& index, const std::string& inputsPath, const std::string& queryPath) {
    using std::chrono::high_resolution_clock;

    /*
    auto tsIndexPrior = high_resolution_clock::now();
    dumpHash(index, inputsPath);
    auto tsIndexPost = high_resolution_clock::now();

    std::chrono::duration<double,std::milli> timeIndexCreation =
        tsIndexPost - tsIndexPrior;
    std::cout << "Hashing Index Creation: " << timeIndexCreation.count() << "ms" << std::endl;

    */
    auto tsQueryPrior = high_resolution_clock::now();
    QueryResults* results = queryHash(index, queryPath);
    auto tsQueryPost = high_resolution_clock::now();

    std::chrono::duration<double,std::milli> timeQuery =
        tsQueryPost - tsQueryPrior;
    std::cout << "Hashing index Query: " << timeQuery.count() << "ms" << std::endl;
    std::cout << "with: " << std::endl;
    std::cout << results->finds << " complete matches" << std::endl;
    std::cout << results->partial_finds << " partial matches" << std::endl;
    std::cout << results->misses << " misses" << std::endl;
}

void runBenchmarks(const std::string& inputPath, const std::string& queryPath) {
    auto trie = new StringApproximatorIndex();
    HashingStringIndexer hash{};
    dump(trie, hash, inputPath);
    benchmarkHash(hash, inputPath, queryPath);
    std::cout << std::endl;
    benchmarkPrefix(trie, inputPath, queryPath);
}
