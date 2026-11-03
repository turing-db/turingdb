#include "CypherCorpus.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>

void turing::test::readCypherCorpus(std::vector<std::string>& queries) {
    std::vector<std::filesystem::path> paths;
    for (const std::filesystem::directory_entry& entry : std::filesystem::directory_iterator(CYPHER_CORPUS_DIR)) {
        paths.push_back(entry.path());
    }

    std::ranges::sort(paths);

    for (const std::filesystem::path& path : paths) {
        std::ifstream file(path, std::ios::binary);
        queries.emplace_back(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
    }
}
