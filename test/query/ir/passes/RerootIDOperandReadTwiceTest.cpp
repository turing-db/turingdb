#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

}

class RerootIDOperandReadTwiceTest : public CallV3Test {
};

TEST_F(RerootIDOperandReadTwiceTest, fetchesTheNodeASquaredColumnNames) {
    const std::string_view query = "MATCH (s:Person) WITH s MATCH (n)-->(m) WITH n, m, s.age - 31 AS v "
                                   "WHERE n = v * v RETURN n.name, m.name";

    StringRowSink explained;
    runQuery(std::string("EXPLAIN (db) ") + std::string(query), explained);
    ASSERT_EQ(explained.getRows().size(), 1u);

    const std::string& program = explained.getRows().front().back();
    EXPECT_TRUE(contains(program, "db.fetch_nodes")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;

    StringRowSink sink;
    runQuery(query, sink);

    Rows rows;
    sink.sortedRows(rows);

    const Rows expected {
        {"Adam", "Bio"},
        {"Adam", "Bio"},
        {"Adam", "Cooking"},
        {"Adam", "Cooking"},
        {"Adam", "Remy"},
        {"Adam", "Remy"},
    };
    EXPECT_EQ(rows, expected);
}
