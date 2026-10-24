#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class AnyMapValueCollectedTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyMapValueCollectedTest, collectsAMapValueInAPatternComprehension) {
    expectRows("MATCH (n {name: 'Remy'}) UNWIND [{a: 1}, {a: 'x'}, {b: 2}] AS m "
               "RETURN [(n)-[:KNOWS_WELL]->(k) | m.a]",
               {{"[1]"}, {"[null]"}, {"[x]"}});
}

TEST_F(AnyMapValueCollectedTest, collectsAMapValueInACollectSubquery) {
    expectRows("MATCH (n {name: 'Remy'}) UNWIND [{a: 1}, {a: 'x'}, {b: 2}] AS m "
               "RETURN COLLECT { MATCH (n)-[:KNOWS_WELL]->(k) RETURN m.a }",
               {{"[1]"}, {"[null]"}, {"[x]"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
