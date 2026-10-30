#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// Two expressions of one projection that each run a subquery one row at a time inside the
// elements of a list. The LIMIT makes the COUNT run per row: Remy counts 2 and Doruk 1.
class SiblingPerRowSubqueryTest : public CallV3Test {
protected:
    static constexpr std::string_view COUNT_INTERESTS =
        "COUNT { MATCH (q:Person)-[:INTERESTED_IN]->(i) WHERE q.name = name RETURN i LIMIT 2 }";

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.rows(), expected) << query;
    }
};

TEST_F(SiblingPerRowSubqueryTest, countsInTwoComprehensionsOverARowsList) {
    std::string query = "UNWIND [['Remy', 'Doruk']] AS names RETURN [name IN names | ";
    query += COUNT_INTERESTS;
    query += "] AS a, [name IN names | ";
    query += COUNT_INTERESTS;
    query += "] AS b";

    expectRows(query, {{"[2, 1]", "[2, 1]"}});
}

// Disabled: lowering puts the second comprehension over a constant list at the top of the
// function, above the property and edge type handles the first one created and both read.
TEST_F(SiblingPerRowSubqueryTest, DISABLED_countsInTwoComprehensionsOverAConstantList) {
    std::string query = "RETURN [name IN ['Remy', 'Doruk'] | ";
    query += COUNT_INTERESTS;
    query += "] AS a, [name IN ['Remy', 'Doruk'] | ";
    query += COUNT_INTERESTS;
    query += "] AS b";

    expectRows(query, {{"[2, 1]", "[2, 1]"}});
}

TEST_F(SiblingPerRowSubqueryTest, DISABLED_countsInTwoReducesOverAConstantList) {
    std::string query = "RETURN reduce(total = 0, name IN ['Remy', 'Doruk'] | total + ";
    query += COUNT_INTERESTS;
    query += ") AS a, reduce(total = 0, name IN ['Remy', 'Doruk'] | total + ";
    query += COUNT_INTERESTS;
    query += ") AS b";

    expectRows(query, {{"3", "3"}});
}

TEST_F(SiblingPerRowSubqueryTest, DISABLED_countsInAReduceBesideAComprehensionOverAConstantList) {
    std::string query = "RETURN reduce(total = 0, name IN ['Remy', 'Doruk'] | total + ";
    query += COUNT_INTERESTS;
    query += ") AS a, [name IN ['Remy', 'Doruk'] | ";
    query += COUNT_INTERESTS;
    query += "] AS b";

    expectRows(query, {{"3", "[2, 1]"}});
}
