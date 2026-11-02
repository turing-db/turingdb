#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

constexpr std::string_view AFTER_RETURN_REASON =
    "Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding RETURN";

constexpr std::string_view AFTER_WITH_REASON =
    "Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding WITH";

}

// An ORDER BY sorts the rows its projection emits, and a projection with no aggregate
// emits one row per incoming row: there is no group for an aggregate in a key to reduce,
// and the query is rejected, as Neo4j rejects it. The out-degrees of the simpledb Persons
// are Remy 4, Adam 3, 2 for Cyrus, Luc, Maxime and Suhas, and 1 for Doruk and Martina.
class OrderByAggregateWithoutAggregateItemTest : public CallV3Test {
};

TEST_F(OrderByAggregateWithoutAggregateItemTest, rejectsACountOfAProjectionOfAProperty) {
    runQueryExpectingError("MATCH (n:Person) RETURN n.name ORDER BY count(*)", AFTER_RETURN_REASON);
}

TEST_F(OrderByAggregateWithoutAggregateItemTest, rejectsAnAggregateAfterAnOrdinaryKey) {
    runQueryExpectingError("MATCH (n:Person)-->(m) RETURN n.name AS name ORDER BY name, count(m) DESC",
                           AFTER_RETURN_REASON);
}

TEST_F(OrderByAggregateWithoutAggregateItemTest, rejectsAKeyCarryingAnAggregate) {
    runQueryExpectingError("MATCH (n:Person)-->(m) RETURN n.name AS name ORDER BY count(m) + n.age",
                           AFTER_RETURN_REASON);
}

TEST_F(OrderByAggregateWithoutAggregateItemTest, rejectsAMapProjectionOverAnAggregate) {
    runQueryExpectingError("MATCH (n:Person) RETURN n.name ORDER BY n {c: count(*)}.c", AFTER_RETURN_REASON);
}

TEST_F(OrderByAggregateWithoutAggregateItemTest, rejectsAnAggregateAfterDistinct) {
    runQueryExpectingError("MATCH (a)-->(b) RETURN DISTINCT b.name ORDER BY count(b) DESC LIMIT 1",
                           AFTER_RETURN_REASON);
}

TEST_F(OrderByAggregateWithoutAggregateItemTest, rejectsAnAggregateAfterAWildcard) {
    runQueryExpectingError("MATCH (n:Person) RETURN * ORDER BY count(*)", AFTER_RETURN_REASON);
}

TEST_F(OrderByAggregateWithoutAggregateItemTest, rejectsAnAggregateOfAConstantProjection) {
    runQueryExpectingError("RETURN 1 AS x ORDER BY count(*)", AFTER_RETURN_REASON);
}

TEST_F(OrderByAggregateWithoutAggregateItemTest, rejectsAnAggregateAfterTheReturnOfASubquery) {
    runQueryExpectingError("MATCH (n:Person) CALL (n) { MATCH (n)-->(m) RETURN m.name AS x ORDER BY count(*) } RETURN x",
                           AFTER_RETURN_REASON);
}

TEST_F(OrderByAggregateWithoutAggregateItemTest, rejectsAnAggregateAfterAWith) {
    runQueryExpectingError("MATCH (n:Person) WITH n.name AS name ORDER BY count(*) RETURN name", AFTER_WITH_REASON);
}

TEST_F(OrderByAggregateWithoutAggregateItemTest, rejectsAMapProjectionOverAnAggregateAfterAWith) {
    runQueryExpectingError("MATCH (n:Person) WITH n.name AS name ORDER BY n {c: count(*)}.c RETURN name",
                           AFTER_WITH_REASON);
}

// An item that carries an aggregate without being one still makes the projection
// aggregating, so the key has groups to reduce.
TEST_F(OrderByAggregateWithoutAggregateItemTest, ordersByAnAggregateBesideAnItemCarryingOne) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-->(m) RETURN n.name AS name, size(collect(m)) AS k "
             "ORDER BY count(*) DESC, name LIMIT 3",
             sink);

    const Rows expected {{"Remy", "4"}, {"Adam", "3"}, {"Cyrus", "2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(OrderByAggregateWithoutAggregateItemTest, ordersByAnAggregateTheWithDoesNotProject) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-->(m) WITH n.name AS name, collect(m) AS ms "
             "ORDER BY count(*) DESC, name LIMIT 3 RETURN name",
             sink);

    const Rows expected {{"Remy"}, {"Adam"}, {"Cyrus"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// k is a count the WITH reduced, and a plain value of each row the RETURN reads.
TEST_F(OrderByAggregateWithoutAggregateItemTest, ordersByTheAggregateOfAnEarlierWith) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-->(m) WITH n.name AS name, count(m) AS k "
             "RETURN name ORDER BY k DESC, name LIMIT 3",
             sink);

    const Rows expected {{"Remy"}, {"Adam"}, {"Cyrus"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A COUNT subquery counts the rows of its own body once per outer row: it is not an
// aggregate of the projection, so it orders the rows of one that does not aggregate.
TEST_F(OrderByAggregateWithoutAggregateItemTest, ordersByACountSubquery) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) RETURN n.name AS name ORDER BY COUNT { (n)-->() } DESC, name LIMIT 3", sink);

    const Rows expected {{"Remy"}, {"Adam"}, {"Cyrus"}};
    EXPECT_EQ(sink.getRows(), expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
