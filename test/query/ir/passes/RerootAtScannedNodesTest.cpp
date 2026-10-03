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

class RerootAtScannedNodesTest : public CallV3Test {
protected:
    void expectRerootedWithoutFetch(std::string_view query, const Rows& expected) {
        StringRowSink explained;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), explained);
        ASSERT_EQ(explained.getRows().size(), 1u);

        const std::string& program = explained.getRows().front().back();
        EXPECT_FALSE(contains(program, "db.fetch_nodes")) << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << program;

        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(RerootAtScannedNodesTest, walksFromTheScannedNodes) {
    expectRerootedWithoutFetch("MATCH (s:Founder) WITH s MATCH (n)-[e]->(m) WHERE n = s RETURN s.name, m.name",
                               {
                                   {"Adam", "Bio"},
                                   {"Adam", "Cooking"},
                                   {"Adam", "Remy"},
                                   {"Remy", "Adam"},
                                   {"Remy", "Computers"},
                                   {"Remy", "Eighties"},
                                   {"Remy", "Ghosts"},
                               });
}

TEST_F(RerootAtScannedNodesTest, walksBackFromTheScannedNodes) {
    expectRerootedWithoutFetch("MATCH (s:Founder) WITH s MATCH (m)-[e]->(n:Person) WHERE n = s RETURN m.name, s.name",
                               {{"Adam", "Remy"}, {"Ghosts", "Remy"}, {"Remy", "Adam"}});
}

TEST_F(RerootAtScannedNodesTest, keepsTheLabelOfTheRoot) {
    expectRerootedWithoutFetch("MATCH (s:Founder) WITH s MATCH (n:Interest)-[e]->(m) WHERE n = s RETURN n.name", {});
}
