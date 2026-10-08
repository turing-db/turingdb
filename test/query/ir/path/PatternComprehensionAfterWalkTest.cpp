#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// simpledb's KNOWS_WELL edges are Remy -> Adam, Adam -> Remy and Ghosts -> Remy. Adam is
// INTERESTED_IN Bio and Cooking, Remy in Ghosts, Computers and Eighties.
class PatternComprehensionAfterWalkTest : public CallV3Test {
protected:
    void expectSortedRows(const std::string& query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(PatternComprehensionAfterWalkTest, comprehendsAPatternAfterAOneHopBody) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) "
                     "RETURN [v IN y | v.name], size([(c)-[:INTERESTED_IN]->(i) | i.name])",
                     {{"Adam", "2"},
                      {"Adam, Remy", "3"}});
}

TEST_F(PatternComprehensionAfterWalkTest, readsAGroupInsideTheComprehension) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) "
                     "RETURN c.name, [(c)-[:INTERESTED_IN]->(i) | size(y)]",
                     {{"Adam", "1, 1"},
                      {"Remy", "2, 2, 2"}});
}

TEST_F(PatternComprehensionAfterWalkTest, comprehendsAPatternAfterASeveralHopBody) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)-[:KNOWS_WELL]->(z)){1,1} (c) "
                     "RETURN [v IN y | v.name], size([(c)-[:INTERESTED_IN]->(i) | i.name])",
                     {{"Adam", "3"}});
}
