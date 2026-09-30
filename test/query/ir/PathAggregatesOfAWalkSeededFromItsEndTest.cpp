#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

constexpr std::string_view walksIntoRemy = "MATCH (b:Person {name: 'Remy'}) MATCH p = (a)-[*1..2]->(b) ";

}

// The walks into Remy, seeded from Remy since the first MATCH binds it: the path each
// handle stands for reads from its far end back, and groups, orders and collects as the
// path the pattern spells
class PathAggregatesOfAWalkSeededFromItsEndTest : public CallV3Test {
protected:
    void expectRows(std::string_view tail, Rows expected) {
        const std::string query = std::string(walksIntoRemy) + std::string(tail);

        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << query;
    }

    void expectOrderedRows(std::string_view tail, const Rows& expected) {
        const std::string query = std::string(walksIntoRemy) + std::string(tail);

        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(PathAggregatesOfAWalkSeededFromItsEndTest, seedsTheWalkFromItsEnd) {
    const std::string query = "EXPLAIN (db) " + std::string(walksIntoRemy) + "RETURN DISTINCT p";

    StringRowSink sink;
    runQuery(query, sink);

    std::string dump;
    for (const StringRowSink::Row& row : sink.getRows()) {
        if (row.front() == "db") {
            dump = row.back();
        }
    }

    EXPECT_NE(dump.find("reversed_paths"), std::string::npos) << dump;
}

TEST_F(PathAggregatesOfAWalkSeededFromItsEndTest, dedupsTheWalks) {
    expectOrderedRows("RETURN DISTINCT p ORDER BY p",
                      {{"(0), [0], (1), [4], (0)"}, {"(0), [1], (6), [7], (0)"}, {"(1), [4], (0)"}, {"(6), [7], (0)"}});
}

TEST_F(PathAggregatesOfAWalkSeededFromItsEndTest, groupsOnTheWalks) {
    expectRows("RETURN p, count(*)",
               {{"(0), [0], (1), [4], (0)", "1"}, {"(0), [1], (6), [7], (0)", "1"}, {"(1), [4], (0)", "1"}, {"(6), [7], (0)", "1"}});
}

TEST_F(PathAggregatesOfAWalkSeededFromItsEndTest, reducesTheWalks) {
    expectRows("RETURN min(p), max(p), count(DISTINCT p)", {{"(0), [0], (1), [4], (0)", "(6), [7], (0)", "4"}});
}

TEST_F(PathAggregatesOfAWalkSeededFromItsEndTest, unwindsTheCollectedWalks) {
    expectOrderedRows("WITH collect(p) AS paths UNWIND paths AS q RETURN nodes(q) ORDER BY q",
                      {{"0, 1, 0"}, {"0, 6, 0"}, {"1, 0"}, {"6, 0"}});
}
