#include <gtest/gtest.h>

#include <string>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A variable-length relationship binds the list of edges each match walked. collect() over
// it gathers one list per row into a list of those lists, the same way it gathers any other
// column, so the aggregate has to read the edges the walk bound rather than its handle.
class CollectVariableLengthEdgesTest : public CallV3Test {
protected:
    size_t walkCount() {
        StringRowSink sink;
        runQuery("MATCH (a)-[e:KNOWS_WELL*1..3]->(b) RETURN e", sink);

        return sink.getRows().size();
    }
};

TEST_F(CollectVariableLengthEdgesTest, collectGathersOneElementPerWalk) {
    const size_t walks = walkCount();
    ASSERT_GT(walks, 1u);

    StringRowSink sink;
    runQuery("MATCH (a)-[e:KNOWS_WELL*1..3]->(b) RETURN size(collect(e))", sink);

    ASSERT_EQ(sink.getRows().size(), 1u);
    EXPECT_EQ(sink.getRows().front().front(), std::to_string(walks));
}

TEST_F(CollectVariableLengthEdgesTest, collectAgreesWithCountOverTheSameWalks) {
    const size_t walks = walkCount();

    StringRowSink sink;
    runQuery("MATCH (a)-[e:KNOWS_WELL*1..3]->(b) RETURN count(e), size(collect(e))", sink);

    ASSERT_EQ(sink.getRows().size(), 1u);

    const StringRowSink::Row& row = sink.getRows().front();
    ASSERT_EQ(row.size(), 2u);
    EXPECT_EQ(row[0], std::to_string(walks));
    EXPECT_EQ(row[1], std::to_string(walks));
}
