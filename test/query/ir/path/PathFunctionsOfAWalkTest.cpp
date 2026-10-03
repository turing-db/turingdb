#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

Rows sorted(Rows rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

}

// The walk (n:Person)-[e]->+(m:Person) on simpledb runs 10 paths: two of length 1, three
// of length 2, two of length 3 and three of length 4
class PathFunctionsOfAWalkTest : public CallV3Test {
};

TEST_F(PathFunctionsOfAWalkTest, groupsRowsByTheLength) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN length(p) AS l, count(*)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"1", "2"},
        {"2", "3"},
        {"3", "2"},
        {"4", "3"},
    }));
}

TEST_F(PathFunctionsOfAWalkTest, keepsTheDistinctLengths) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN DISTINCT length(p)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {{"1"}, {"2"}, {"3"}, {"4"}}));
}

TEST_F(PathFunctionsOfAWalkTest, ordersRowsByTheLength) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN length(p) ORDER BY length(p)", sink);

    EXPECT_EQ(sink.getRows(), (Rows {
        {"1"}, {"1"}, {"2"}, {"2"}, {"2"}, {"3"}, {"3"}, {"4"}, {"4"}, {"4"},
    }));
}

TEST_F(PathFunctionsOfAWalkTest, sumsTheLengths) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN sum(length(p))", sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"26"}}));
}

TEST_F(PathFunctionsOfAWalkTest, collectsTheLengths) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN size(collect(length(p)))", sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"10"}}));
}

TEST_F(PathFunctionsOfAWalkTest, collectsTheNodes) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN size(collect(nodes(p)))", sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"10"}}));
}

TEST_F(PathFunctionsOfAWalkTest, collectsTheRelationships) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN size(collect(relationships(p)))", sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"10"}}));
}
