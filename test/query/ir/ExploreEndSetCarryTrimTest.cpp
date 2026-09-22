#include <gtest/gtest.h>

#include <set>
#include <string>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// An exploration whose end is pinned to a set takes that set as an operand after its carry
// set. The trim pass reads one result per carried operand, so the trailing operand is not
// one of them: counting it walks off the results and puts the set itself in the carry set.
class ExploreEndSetCarryTrimTest : public CallV3Test {
protected:
    size_t rowCount(const std::string& query) {
        StringRowSink sink;
        runQuery(query, sink);

        return sink.getRows().size();
    }
};

TEST_F(ExploreEndSetCarryTrimTest, aCarriedColumnSurvivesBesideAnEndSet) {
    const size_t walks = rowCount("MATCH (a {name:'Remy'})-[e*1..3]->(b {name:'Adam'}) RETURN e");
    ASSERT_GT(walks, 0u);

    std::set<std::string> interests;
    StringRowSink interestSink;
    runQuery("MATCH (c:Interest) RETURN c.name", interestSink);
    for (const StringRowSink::Row& row : interestSink.getRows()) {
        interests.insert(row.front());
    }
    ASSERT_FALSE(interests.empty());

    StringRowSink sink;
    runQuery("MATCH (a {name:'Remy'}), (b {name:'Adam'}), (c:Interest) WITH a, b, c "
             "MATCH (a)-[e*1..3]->(b) RETURN a.name, b.name, c.name",
             sink);

    EXPECT_EQ(sink.getRows().size(), walks * interests.size());

    for (const StringRowSink::Row& row : sink.getRows()) {
        ASSERT_EQ(row.size(), 3u);
        EXPECT_EQ(row[0], "Remy");
        EXPECT_EQ(row[1], "Adam");
        EXPECT_TRUE(interests.contains(row[2])) << row[2];
    }
}
