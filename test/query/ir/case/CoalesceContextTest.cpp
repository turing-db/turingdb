#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// coalesce is an expression like any other: it computes its arguments lazily wherever it
// stands, so a division guarded by an earlier argument divides by no zero.
class CoalesceContextTest : public WriteQueryTest {
};

TEST_F(CoalesceContextTest, coalescesInAListComprehension) {
    expectRows("RETURN [t IN [0, 1, 2] | "
               "coalesce(CASE WHEN t = 0 THEN -1 END, CASE WHEN t = 1 THEN -2 END, 10 / (t * (t - 1)))]",
               {{"[-1, -2, 5]"}});
}

TEST_F(CoalesceContextTest, coalescesInAReduce) {
    expectRows("RETURN reduce(acc = 0, t IN [0, 1, 2] | acc + coalesce(CASE WHEN t = 0 THEN 100 END, 10 / t))",
               {{"115"}});
}

TEST_F(CoalesceContextTest, coalescesInACaseBranch) {
    expectRowsInOrder("UNWIND [0, 1, 2] AS t "
                      "RETURN CASE WHEN t > 0 THEN coalesce(CASE WHEN t = 1 THEN -1 END, 10 / (t - 1)) ELSE 0 END",
                      {{"0"}, {"-1"}, {"10"}});
}

TEST_F(CoalesceContextTest, coalescesAnAggregateArgument) {
    expectRows("UNWIND [0, 1, 2, 4] AS t RETURN sum(coalesce(CASE WHEN t = 0 THEN 7 END, 8 / t))", {{"21"}});
}

TEST_F(CoalesceContextTest, coalescesInAWhere) {
    expectRows("UNWIND [0, 1, 2, 4] AS t WITH t WHERE coalesce(CASE WHEN t = 0 THEN true END, 8 / t > 2) RETURN t",
               {{"0"}, {"1"}, {"2"}});
}

TEST_F(CoalesceContextTest, coalescesAnOrderByKey) {
    expectRowsInOrder("UNWIND [0, 1, 2, 4] AS t RETURN t ORDER BY coalesce(CASE WHEN t = 0 THEN 3 END, 8 / t)",
                      {{"4"}, {"0"}, {"2"}, {"1"}});
}

TEST_F(CoalesceContextTest, coalescesAGroupingKey) {
    expectRows("UNWIND [0, 1, 2, 4, 8] AS t RETURN coalesce(CASE WHEN t = 0 THEN 1 END, 8 / t) AS k, count(*)",
               {{"1", "2"}, {"2", "1"}, {"4", "1"}, {"8", "1"}});
}

TEST_F(CoalesceContextTest, coalescesInsideACoalesce) {
    expectRowsInOrder("UNWIND [0, 1, 2] AS t "
                      "RETURN coalesce(coalesce(CASE WHEN t = 0 THEN -1 END, CASE WHEN t = 1 THEN -2 END), 10 / t)",
                      {{"-1"}, {"-2"}, {"5"}});
}
