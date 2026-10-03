#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

class SeedIDsBelowAFilterTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(SeedIDsBelowAFilterTest, computesTheIDsOnlyOverTheRowsTheFilterKeeps) {
    expectRows("MATCH (a)-[e]->(b) UNWIND [0, 2] AS y WITH a, b, y WHERE y > 0 "
               "WITH a, b, y WHERE b = 4 / y RETURN a.name, b.name",
               {{"Luc", "Computers"}, {"Remy", "Computers"}});
}

TEST_F(SeedIDsBelowAFilterTest, comparesOnlyTheIDsTheFilterKeeps) {
    expectRows("MATCH (n) UNWIND [-1, 2] AS x WITH n, x WHERE x > 0 WITH n, x WHERE n = x RETURN n.name, x",
               {{"Computers", "2"}});
}
