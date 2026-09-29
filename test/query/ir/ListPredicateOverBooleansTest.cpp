#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class ListPredicateOverBooleansTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ListPredicateOverBooleansTest, decidesOverBooleanElements) {
    expectRows("RETURN all(x IN [true, false] WHERE x), any(x IN [true, false] WHERE x), none(x IN [false] WHERE x), single(x IN [true, false] WHERE x)",
               {{"false", "true", "true", "true"}});
}

TEST_F(ListPredicateOverBooleansTest, decidesOverABooleanProperty) {
    expectRows("MATCH (n:Person) RETURN n.name, any(x IN [n] WHERE x.isFrench)", {
        {"Adam", "true"}, {"Cyrus", "false"}, {"Doruk", "false"}, {"Luc", "true"},
        {"Martina", "false"}, {"Maxime", "true"}, {"Remy", "true"}, {"Suhas", "false"},
    });
}

TEST_F(ListPredicateOverBooleansTest, decidesOverAComparisonOfBooleans) {
    expectRows("RETURN all(x IN [true, true] WHERE x = true), any(x IN [1, 2] WHERE true)", {{"true", "true"}});
}
