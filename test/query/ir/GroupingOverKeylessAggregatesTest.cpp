#include <gtest/gtest.h>

#include <algorithm>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// A projection grouping on what a keyless one reduced: a count is read where the match
// ends, a collect in the loop draining its list, and the group takes both
class GroupingOverKeylessAggregatesTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(GroupingOverKeylessAggregatesTest, countsTheRowsOfACountAndACollect) {
    expectRows("MATCH (n:Person) WITH count(n) AS c, collect(n.name) AS names RETURN c, size(names), count(*)",
               {{"8", "8", "1"}});
}

TEST_F(GroupingOverKeylessAggregatesTest, groupsACountBesideACollectItReduces) {
    expectRows("MATCH (n:Person) WITH count(n) AS c, collect(n.name) AS names RETURN c, max(size(names))",
               {{"8", "8"}});
}

TEST_F(GroupingOverKeylessAggregatesTest, collectsACollectGroupedOnACount) {
    expectRows("MATCH (n:Person) WITH count(n) AS c, collect(n.name) AS names RETURN c, size(collect(names))",
               {{"8", "1"}});
}

TEST_F(GroupingOverKeylessAggregatesTest, groupsACollectBesideACount) {
    expectRows("MATCH (n:Person) WITH collect(n.name) AS names, count(n) AS c RETURN size(names), c, count(*)",
               {{"8", "8", "1"}});
}

TEST_F(GroupingOverKeylessAggregatesTest, limitsACountAndACollect) {
    expectRows("MATCH (n:Person) WITH count(n) AS c, collect(n.name) AS names RETURN c, size(names) LIMIT 1",
               {{"8", "8"}});
}

TEST_F(GroupingOverKeylessAggregatesTest, skipsACountAndACollect) {
    expectRows("MATCH (n:Person) WITH count(n) AS c, collect(n.name) AS names RETURN c, size(names) SKIP 1", {});
}
