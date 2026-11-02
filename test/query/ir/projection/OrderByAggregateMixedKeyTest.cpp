#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

constexpr std::string_view DROPPED_KEY_REASON =
    "ORDER BY with an aggregate may only order by expressions over the returned columns";

}

// An ORDER BY key over an aggregating projection holds one value per group only if every
// part of it does. An aggregate is one such part, and so is a grouping key, but a variable
// the grouping dropped holds one value per matched row: a key mixing it with an aggregate
// has no value per group, and is rejected like the dropped variable alone.
class OrderByAggregateMixedKeyTest : public CallV3Test {
};

TEST_F(OrderByAggregateMixedKeyTest, rejectsAnAggregatePlusADroppedVariable) {
    runQueryExpectingError("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                           "ORDER BY count(m) + n.age DESC LIMIT 3",
                           DROPPED_KEY_REASON);
}

TEST_F(OrderByAggregateMixedKeyTest, rejectsAListOfAnAggregateAndADroppedVariable) {
    runQueryExpectingError("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                           "ORDER BY [count(m), n.age][0]",
                           DROPPED_KEY_REASON);
}

TEST_F(OrderByAggregateMixedKeyTest, rejectsAMapProjectionOfADroppedVariableOverAnAggregate) {
    runQueryExpectingError("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                           "ORDER BY n {c: count(m)}.c",
                           DROPPED_KEY_REASON);
}

TEST_F(OrderByAggregateMixedKeyTest, rejectsTheAliasOfAnAggregatePlusADroppedVariable) {
    runQueryExpectingError("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                           "ORDER BY k + n.age",
                           DROPPED_KEY_REASON);
}

TEST_F(OrderByAggregateMixedKeyTest, rejectsACaseOverAnAggregateSelectingADroppedVariable) {
    runQueryExpectingError("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                           "ORDER BY CASE WHEN count(m) > 1 THEN n.age ELSE 0 END",
                           DROPPED_KEY_REASON);
}

TEST_F(OrderByAggregateMixedKeyTest, rejectsAComprehensionOverAnAggregateReadingADroppedVariable) {
    runQueryExpectingError("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                           "ORDER BY [x IN collect(m.name) | size(x) + n.age]",
                           DROPPED_KEY_REASON);
}

TEST_F(OrderByAggregateMixedKeyTest, rejectsAnAggregatePlusTheAggregatedVariable) {
    runQueryExpectingError("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                           "ORDER BY count(m) + m.age",
                           DROPPED_KEY_REASON);
}

// The aggregate in the key is what makes the projection aggregating, with n.name its one
// grouping key: n.age is dropped all the same.
TEST_F(OrderByAggregateMixedKeyTest, rejectsAKeyThatIsTheOnlyAggregate) {
    runQueryExpectingError("MATCH (n:Person)-->(m) RETURN n.name AS name ORDER BY count(m) + n.age",
                           DROPPED_KEY_REASON);
}

TEST_F(OrderByAggregateMixedKeyTest, rejectsAnAggregatePlusADroppedVariableUnderAWith) {
    runQueryExpectingError("MATCH (n:Person)-->(m) WITH n.name AS name, count(m) AS k "
                           "ORDER BY count(m) + n.age RETURN name",
                           DROPPED_KEY_REASON);
}

// The out-degrees of the simpledb Persons are Remy 4, Adam 3, then 2 for Maxime, Cyrus,
// Suhas and Luc. Ten per edge plus the length of the name puts Maxime (26) ahead of the
// other three, an order the count alone leaves tied.
TEST_F(OrderByAggregateMixedKeyTest, ordersByAnAggregatePlusAGroupingKey) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
             "ORDER BY count(m) * 10 + size(n.name) DESC LIMIT 3",
             sink);

    const Rows expected {{"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(OrderByAggregateMixedKeyTest, ordersByTheAliasesOfAnAggregateAndAGroupingKey) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
             "ORDER BY k * 10 + size(name) DESC LIMIT 3",
             sink);

    const Rows expected {{"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Remy and Adam are 32 and have 7 out-edges between them, the six other Persons no age and
// 10. The ageless group's key is 10 + null, a null, which sorts after every value.
TEST_F(OrderByAggregateMixedKeyTest, ordersByAnAggregatePlusAGroupedAge) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-->(m) RETURN n.age AS age, count(m) AS k ORDER BY count(m) + n.age", sink);

    const Rows expected {{"32", "7"}, {"null", "10"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Returning n groups by it, so the map projection of n holds one value per group. Remy is
// node 0 and Adam node 1.
TEST_F(OrderByAggregateMixedKeyTest, ordersByAMapProjectionOfAGroupingKeyOverAnAggregate) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-->(m) RETURN n, count(m) AS k ORDER BY n {c: count(m)}.c DESC LIMIT 2", sink);

    const Rows expected {{"0", "4"}, {"1", "3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
