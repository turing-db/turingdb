#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

bool buildsOpTypedAs(std::string_view program, std::string_view op, std::string_view resultType) {
    size_t lineStart = 0;
    while (lineStart < program.size()) {
        const size_t lineEnd = std::min(program.find('\n', lineStart), program.size());
        const std::string_view line = program.substr(lineStart, lineEnd - lineStart);

        if (contains(line, op) && line.ends_with(resultType)) {
            return true;
        }

        lineStart = lineEnd + 1;
    }

    return false;
}

}

// An UNWIND fused into an equality hands every reader but an output a stand-in typed as the
// elements: the node's ID where they were compared to it, the element itself where there is one.
class FuseUnwindEqualityStandInTest : public WriteQueryTest {
protected:
    void programOf(std::string_view query, std::string& program) {
        RowSink sink;
        const QueryStatus status = runQuery(std::string("EXPLAIN (db) ") + std::string(query), &sink);
        ASSERT_TRUE(status.isOk()) << query << "\n" << status.getError();

        const Rows& rows = sink.rows();
        ASSERT_EQ(rows.size(), 1u);

        program = rows.front().back();
    }

    void expectReadsTheNodeID(std::string_view query, const Rows& expected) {
        expectRows(query, expected);

        std::string program;
        programOf(query, program);

        EXPECT_FALSE(contains(program, "db.cross_product")) << program;
        EXPECT_FALSE(contains(program, "db.to_integer")) << program;
        EXPECT_TRUE(buildsOpTypedAs(program, "db.element_id(", "-> !db.column<i64>")) << program;
    }
};

TEST_F(FuseUnwindEqualityStandInTest, readsTheNodeIDUnderASort) {
    expectReadsTheNodeID("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN i ORDER BY i", {{"0"}, {"1"}});
}

TEST_F(FuseUnwindEqualityStandInTest, readsTheNodeIDAsAGroupingKey) {
    expectReadsTheNodeID("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN i, count(*)", {{"0", "1"}, {"1", "1"}});
}

TEST_F(FuseUnwindEqualityStandInTest, readsTheNodeIDInARegion) {
    expectReadsTheNodeID("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN [x IN [1, 2] | x + i]",
                         {{"[1, 2]"}, {"[2, 3]"}});
}

TEST_F(FuseUnwindEqualityStandInTest, readsTheOneElementAsAGroupingKey) {
    const std::string_view query = "UNWIND ['Remy'] AS x MATCH (n:Person) WHERE n.name = x RETURN x, count(*)";
    expectRows(query, {{"Remy", "1"}});

    std::string program;
    programOf(query, program);

    EXPECT_FALSE(contains(program, "db.cross_product")) << program;
    EXPECT_TRUE(buildsOpTypedAs(program, "db.broadcast_constant(", "-> !db.column<!storage.string>")) << program;
}
