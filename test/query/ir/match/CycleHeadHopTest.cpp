#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A cycle is broken by splitting its head into two merge copies. When a hop from outside
// the cycle reaches the head before either copy holds a column, the head takes the hop's
// column and the cycle closes with a filter on it.
class CycleHeadHopTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

// Bo has a KNOWS_WELL edge to himself, and Ana has one to Bo.
TEST_F(CycleHeadHopTest, reachesASelfLoopOverAHop) {
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}), (b)-[:KNOWS_WELL]->(b)");

    expectRows("MATCH (a)-->(b)-->(b) RETURN a.name, b.name",
               {
                   {"Ana", "Bo"},
                   {"Bo", "Bo"},
               });
}

// simpledb has no self-loop, so no row.
TEST_F(CycleHeadHopTest, matchesNoSelfLoopOnSimpledb) {
    expectRows("MATCH (a)-->(b)-->(b) RETURN id(a)", {});
}

// The same 6 rows as MATCH (x)-->(a)-->(b)-->(c) WHERE id(c) = id(a).
TEST_F(CycleHeadHopTest, reachesATwoHopCycleOverAHop) {
    expectRows("MATCH (x)-->(a)-->(b)-->(a) RETURN id(x), id(a), id(b)",
               {
                   {"0", "1", "0"},
                   {"0", "6", "0"},
                   {"1", "0", "1"},
                   {"1", "0", "6"},
                   {"6", "0", "1"},
                   {"6", "0", "6"},
               });
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
