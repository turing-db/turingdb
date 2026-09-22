#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A pattern that returns to a variable it already bound closes a cycle, and the dependency
// graph breaks the cycle by electing one of its variables as the merge target. The election
// must pick a node: an edge variable behind a merge edge never completes a
// (source, edge, target) triple, so it never holds a column for the merge to read. It read
// GET_OUT_EDGES and GET_IN_EDGES only, and missed the GET_EDGES an undirected hop carries.
class UndirectedCycleTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

// The reported query. The undirected hop binds every edge between a and b either way, so
// each of the 18 edges matches itself, and the 4 with a reciprocal edge match that one too.
TEST_F(UndirectedCycleTest, closesTheCycleOnAnUndirectedHop) {
    expectRows("MATCH (a)-->(b)--(a) RETURN count(*)", {{"22"}});
}

// The two directed readings of that hop, which both already worked: 18 rows where the
// second edge runs a to b, 4 where it runs b to a.
TEST_F(UndirectedCycleTest, splitsIntoTheTwoDirectedReadings) {
    expectRows("MATCH (a)-->(b)<--(a) RETURN count(*)", {{"18"}});
    expectRows("MATCH (a)-->(b)-->(a) RETURN count(*)", {{"4"}});
}

// Both hops undirected: a pair joined by d edges matches d * d rows from either end. Two
// pairs of simpledb carry 2 edges and fourteen carry 1, so 2 * (2 * 4 + 14).
TEST_F(UndirectedCycleTest, closesTheCycleOnTwoUndirectedHops) {
    expectRows("MATCH (a)--(b)--(a) RETURN count(*)", {{"44"}});
}

// The rows themselves, over the three KNOWS_WELL edges. Remy and Adam know each other both
// ways and Remy is interested in Ghosts, so each of the three has one reciprocal edge for
// the undirected hop to bind beside itself.
TEST_F(UndirectedCycleTest, bindsBothEdgesOfThePairToTheUndirectedHop) {
    expectRows("MATCH (a)-[:KNOWS_WELL]->(b)--(a) RETURN a.name, b.name",
               {
                   {"Adam", "Remy"},
                   {"Adam", "Remy"},
                   {"Ghosts", "Remy"},
                   {"Ghosts", "Remy"},
                   {"Remy", "Adam"},
                   {"Remy", "Adam"},
               });
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
