#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "HashJoinQueryTest.h"
#include "StringRowSink.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

// A join whose key property the query also returns. The projection and the key read one
// property of one variable, so the reuse pass hands both the same column - and the cut
// still has to fuse, since the join yields that column among its results.
class HashJoinProjectedKeyTest : public HashJoinQueryTest {
protected:
    // The rows the join produces against the rows the cross product and its filter
    // produce, which is what the cut stands as when the join is not forced. Neither form
    // is ordered, so the two are held against each other sorted.
    void expectJoinMatchesTheProduct(std::string_view query) {
        StringRowSink joined;
        runQuery(query, joined);

        StringRowSink product;
        runQuery(query, product, /*forcesJoin=*/false);

        Rows joinedRows = joined.getRows();
        Rows productRows = product.getRows();
        std::sort(joinedRows.begin(), joinedRows.end());
        std::sort(productRows.begin(), productRows.end());

        EXPECT_FALSE(joinedRows.empty()) << "query: " << query;
        EXPECT_EQ(joinedRows, productRows) << "query: " << query;
    }
};

TEST_F(HashJoinProjectedKeyTest, fusesWhenTheReturnProjectsTheKeyProperty) {
    std::string program;
    dbProgram("MATCH (n), (m) WHERE n.name = m.name RETURN n.name", program);

    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;
}

TEST_F(HashJoinProjectedKeyTest, fusesWhenTheReturnProjectsTheKeyOfBothSides) {
    std::string program;
    dbProgram("MATCH (n), (m) WHERE n.name = m.name RETURN n.name, m.name", program);

    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;
}

// Every node carries its own name, so the join is the diagonal: one row per node, whose
// projected key is that node's name.
TEST_F(HashJoinProjectedKeyTest, projectsTheKeyOfEveryJoinedRow) {
    StringRowSink sink;
    runQuery("MATCH (n), (m) WHERE n.name = m.name RETURN n.name", sink);

    const Rows& rows = sink.getRows();
    EXPECT_EQ(rows.size(), 18u);

    for (const StringRowSink::Row& row : rows) {
        ASSERT_EQ(row.size(), 1u);
        EXPECT_FALSE(row.front().empty());
    }
}

TEST_F(HashJoinProjectedKeyTest, projectsTheKeyOfBothSidesOfEveryJoinedRow) {
    StringRowSink sink;
    runQuery("MATCH (n), (m) WHERE n.name = m.name RETURN n.name, m.name", sink);

    const Rows& rows = sink.getRows();
    EXPECT_EQ(rows.size(), 18u);

    for (const StringRowSink::Row& row : rows) {
        ASSERT_EQ(row.size(), 2u);
        EXPECT_EQ(row.front(), row.back());
    }
}

// Only Remy and Adam carry an age, and both carry 32, so the join on the age is the four
// pairs of those two - and a null key matches nothing, so no other node reaches a row.
TEST_F(HashJoinProjectedKeyTest, projectsANullableKeyOffTheJoinedRows) {
    std::string program;
    dbProgram("MATCH (n), (m) WHERE n.age = m.age RETURN n.name, n.age", program);
    EXPECT_TRUE(contains(program, "db.hash_join")) << program;

    expectRows("MATCH (n), (m) WHERE n.age = m.age RETURN n.name, n.age",
               {{"Remy", "32"}, {"Remy", "32"}, {"Adam", "32"}, {"Adam", "32"}});
}

TEST_F(HashJoinProjectedKeyTest, joinsTheRowsTheProductAndItsFilterProduce) {
    expectJoinMatchesTheProduct("MATCH (n), (m) WHERE n.name = m.name RETURN n.name");
    expectJoinMatchesTheProduct("MATCH (n), (m) WHERE n.name = m.name RETURN n.name, m.name");
    expectJoinMatchesTheProduct("MATCH (n), (m) WHERE n.age = m.age RETURN n.name, n.age");
    expectJoinMatchesTheProduct("MATCH (n), (m) WHERE n.name = m.name RETURN n, n.name, m");
}
