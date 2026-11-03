#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "ParseOutcome.h"
#include "QueryMutants.h"

using namespace turing::test;

namespace {

void readCorpus(std::vector<std::string>& queries) {
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

}

// No query of the fuzzing corpus, and no query one edit away from one, makes the parser fail
// in any other way than by reporting a syntax error
class ParseMutantTest : public ::testing::Test {
protected:
    static void expectWellFormedParse(const std::string& query) {
        std::string problem;
        EXPECT_TRUE(isWellFormedParse(query, problem)) << query << "\n" << problem;
    }
};

TEST_F(ParseMutantTest, parsesEveryCorpusQueryOrReportsItsError) {
    std::vector<std::string> queries;
    readCorpus(queries);
    ASSERT_GT(queries.size(), 5000u);

    for (const std::string& query : queries) {
        expectWellFormedParse(query);
    }
}

TEST_F(ParseMutantTest, parsesEveryMutantOfTheCorpusOrReportsItsError) {
    std::vector<std::string> queries;
    readCorpus(queries);
    ASSERT_GT(queries.size(), 5000u);

    for (const std::string& query : queries) {
        std::vector<std::string> mutants;
        buildQueryMutants(query, mutants);

        for (const std::string& mutant : mutants) {
            expectWellFormedParse(mutant);
        }
    }
}
