#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A clause reading the rows in flight through a region of its own - a CALL subquery, an
// optional pattern - over a column the query matched against an unwound value
class UnwindMatchedScopeTest : public CallV3Test {
};

TEST_F(UnwindMatchedScopeTest, aSubqueryImportsAColumnMatchedAgainstAnUnwoundValue) {
    StringRowSink sink;
    runQuery("UNWIND ['Remy', 'Adam'] AS nm "
             "MATCH (p:Person {name: nm}) "
             "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest } "
             "RETURN nm, interest",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Remy", "Ghosts"},
                                                    {"Remy", "Computers"},
                                                    {"Remy", "Eighties"},
                                                    {"Adam", "Bio"},
                                                    {"Adam", "Cooking"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(UnwindMatchedScopeTest, anOptionalPatternExtendsAColumnMatchedAgainstAnUnwoundValue) {
    StringRowSink sink;
    runQuery("UNWIND ['Remy', 'Adam'] AS nm "
             "MATCH (p:Person {name: nm}) "
             "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(k) "
             "RETURN nm, k.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Remy", "Adam"}, {"Adam", "Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}
