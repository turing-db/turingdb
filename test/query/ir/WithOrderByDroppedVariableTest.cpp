#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The ORDER BY of a WITH that neither aggregates nor dedups reads the variables before the
// WITH as well as the ones it publishes, as in Neo4j
class WithOrderByDroppedVariableTest : public WriteQueryTest {
protected:
    void expectRejected(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR)
            << "query: " << query << "\nerror: " << error;
        EXPECT_EQ(error.find("Internal Error"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
    }
};

TEST_F(WithOrderByDroppedVariableTest, ordersByADroppedConstant) {
    expectRowsInOrder("UNWIND [3, 1, 2] AS x WITH x % 2 AS y ORDER BY x RETURN y",
                      {{"1"}, {"0"}, {"1"}});
}

// Remy, Adam, Maxime and Luc are the four people with a dob, in dob order
TEST_F(WithOrderByDroppedVariableTest, ordersByAPropertyOfADroppedNode) {
    expectRowsInOrder("MATCH (p:Person) WHERE p.dob IS NOT NULL "
                      "WITH p.name AS name ORDER BY p.dob RETURN name",
                      {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}});
    expectRowsInOrder("MATCH (p:Person) WHERE p.dob IS NOT NULL "
                      "WITH p.name AS name ORDER BY name DESC, p.dob RETURN name",
                      {{"Remy"}, {"Maxime"}, {"Luc"}, {"Adam"}});
}

TEST_F(WithOrderByDroppedVariableTest, cutsTheRowsTheDroppedKeyOrdered) {
    expectRowsInOrder("MATCH (p:Person) WHERE p.dob IS NOT NULL "
                      "WITH p.name AS name ORDER BY p.dob DESC LIMIT 2 RETURN name",
                      {{"Luc"}, {"Maxime"}});
    expectRowsInOrder("UNWIND [3, 1, 2] AS x WITH x % 2 AS y ORDER BY x DESC SKIP 1 RETURN y",
                      {{"0"}, {"1"}});
}

TEST_F(WithOrderByDroppedVariableTest, filtersTheRowsTheDroppedKeyOrdered) {
    expectRowsInOrder("UNWIND [3, 1, 2] AS x WITH x AS y ORDER BY x WHERE x > 1 RETURN y",
                      {{"2"}, {"3"}});
}

TEST_F(WithOrderByDroppedVariableTest, dropsTheVariableAfterTheSort) {
    expectRejected("UNWIND [3, 1, 2] AS x WITH x % 2 AS y ORDER BY x RETURN x");
}

TEST_F(WithOrderByDroppedVariableTest, rejectsADroppedKeyUnderAnAggregate) {
    expectRejected("UNWIND [3, 1, 2] AS x WITH x % 2 AS y, count(*) AS c ORDER BY x RETURN y");
}

TEST_F(WithOrderByDroppedVariableTest, rejectsADroppedKeyUnderDistinct) {
    expectRejected("UNWIND [3, 1, 2] AS x WITH DISTINCT x % 2 AS y ORDER BY x RETURN y");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
