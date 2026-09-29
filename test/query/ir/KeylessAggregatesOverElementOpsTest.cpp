#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A keyless projection whose second aggregate reads a list predicate or a comprehension
// over the rows every aggregate reduces: the 8 people of simpledb, or 10 unwound integers
class KeylessAggregatesOverElementOpsTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(KeylessAggregatesOverElementOpsTest, countsAListPredicateBesideACount) {
    expectRows("MATCH (n:Person) RETURN count(*), count(any(x IN [n.name] WHERE x = 'Remy'))", {{"8", "8"}});
}

TEST_F(KeylessAggregatesOverElementOpsTest, countsTwoListPredicates) {
    expectRows("MATCH (n:Person) RETURN count(any(x IN [n.name] WHERE x = 'Remy')), count(all(x IN [n.name] WHERE x = 'Remy'))",
               {{"8", "8"}});
}

TEST_F(KeylessAggregatesOverElementOpsTest, countsAListPredicateOverUnwoundRows) {
    expectRows("UNWIND range(1, 10) AS i RETURN count(*), count(any(x IN [i] WHERE x = 1))", {{"10", "10"}});
}

TEST_F(KeylessAggregatesOverElementOpsTest, reducesAComprehensionBesideACount) {
    expectRows("MATCH (n:Person) RETURN count(*), max(size([x IN [n.name, 'Remy'] WHERE x = 'Remy']))", {{"8", "2"}});
}
