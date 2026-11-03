#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CypherCorpus.h"
#include "ParseOutcome.h"
#include "QueryMutants.h"

using namespace turing::test;

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
    readCypherCorpus(queries);
    ASSERT_GT(queries.size(), 5000u);

    for (const std::string& query : queries) {
        expectWellFormedParse(query);
    }
}

TEST_F(ParseMutantTest, parsesEveryMutantOfTheCorpusOrReportsItsError) {
    std::vector<std::string> queries;
    readCypherCorpus(queries);
    ASSERT_GT(queries.size(), 5000u);

    for (const std::string& query : queries) {
        std::vector<std::string> mutants;
        buildQueryMutants(query, mutants);

        for (const std::string& mutant : mutants) {
            expectWellFormedParse(mutant);
        }
    }
}
