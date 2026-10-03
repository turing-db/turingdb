#include <gtest/gtest.h>

#include <string>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// SKIP and LIMIT cut the rows the query returns, whatever those rows hold. A named path is
// built from the seed and the walk the pattern bound rather than from the projected column,
// so the cut has to reach it too.
class NamedPathCutTest : public CallV3Test {
protected:
    size_t rowCount(const std::string& query) {
        StringRowSink sink;
        runQuery(query, sink);

        return sink.getRows().size();
    }
};

TEST_F(NamedPathCutTest, skipDropsTheFirstRowsOfAPathProjection) {
    const std::string pattern = "MATCH p = (n:Person)-[e:KNOWS_WELL*1..2]->(m:Person) RETURN p";
    const size_t all = rowCount(pattern);
    ASSERT_GT(all, 2u);

    EXPECT_EQ(rowCount(pattern + " SKIP 2"), all - 2);
}

TEST_F(NamedPathCutTest, limitBoundsAPathProjection) {
    const std::string pattern = "MATCH p = (n:Person)-[e:KNOWS_WELL*1..2]->(m:Person) RETURN p";
    ASSERT_GT(rowCount(pattern), 1u);

    EXPECT_EQ(rowCount(pattern + " LIMIT 1"), 1u);
}

TEST_F(NamedPathCutTest, skipKeepsThePathAlignedWithTheColumnsBesideIt) {
    const std::string pattern = "MATCH p = (n:Person)-[e:KNOWS_WELL*1..2]->(m:Person) RETURN n.name, p";

    StringRowSink all;
    runQuery(pattern, all);
    ASSERT_GT(all.getRows().size(), 2u);

    StringRowSink cut;
    runQuery(pattern + " SKIP 2", cut);

    const std::vector<StringRowSink::Row> expected(all.getRows().begin() + 2, all.getRows().end());
    EXPECT_EQ(cut.getRows(), expected);
}
