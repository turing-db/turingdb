#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallReturnAliasTest : public CallV3Test {
};

TEST_F(CallReturnAliasTest, aliasesAYieldedColumn) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label RETURN label AS l", sink);

    const std::vector<std::string> expectedNames {"l"};
    EXPECT_EQ(sink.getNames(), expectedNames);
    EXPECT_EQ(sink.getRows().size(), 9u);
}

// The alias the YIELD gives a return value is the name the WHERE and the RETURN read it by.
TEST_F(CallReturnAliasTest, yieldAliasFlowsIntoWhereAndReturn) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label AS l WHERE l = 'Person' RETURN l", sink);

    const std::vector<std::string> expectedNames {"l"};
    EXPECT_EQ(sink.getNames(), expectedNames);

    const std::vector<StringRowSink::Row> expected {{"Person"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallReturnAliasTest, ordersByAnAliasOfAYieldedColumn) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label RETURN label AS l ORDER BY l LIMIT 1", sink);

    const std::vector<StringRowSink::Row> expected {{"Bioinformatics"}};
    EXPECT_EQ(sink.getRows(), expected);
}
