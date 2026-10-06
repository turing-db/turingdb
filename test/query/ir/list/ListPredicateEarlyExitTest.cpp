#include <gtest/gtest.h>

#include <algorithm>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A list predicate stops reading a row's elements once they decide it. The last element of
// range(1, 100000) divides by zero, and lies past the first chunk of 65536 elements, so a
// predicate that kept reading after the decision fails the query.
class ListPredicateEarlyExitTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, Rows expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ListPredicateEarlyExitTest, stopsAnyAtAnElementThatHolds) {
    expectRows("RETURN any(x IN range(1, 100000) WHERE 100000 / (100000 - x) = 1)", {{"true"}});
}

TEST_F(ListPredicateEarlyExitTest, stopsAllAtAnElementThatFails) {
    expectRows("RETURN all(x IN range(1, 100000) WHERE 100000 / (100000 - x) > 1)", {{"false"}});
}

TEST_F(ListPredicateEarlyExitTest, stopsNoneAtAnElementThatHolds) {
    expectRows("RETURN none(x IN range(1, 100000) WHERE 100000 / (100000 - x) = 1)", {{"false"}});
}

TEST_F(ListPredicateEarlyExitTest, stopsSingleAtTheSecondElementThatHolds) {
    expectRows("RETURN single(x IN range(1, 100000) WHERE 100000 / (100000 - x) >= 1)", {{"false"}});
}

TEST_F(ListPredicateEarlyExitTest, stopsEachRowOnItsOwn) {
    expectRows("UNWIND [1, 2] AS k RETURN k, any(x IN range(1, 100000) WHERE 100000 / (100000 - x) = k)",
               {{"1", "true"}, {"2", "true"}});
}

TEST_F(ListPredicateEarlyExitTest, readsTheNextRowAfterAStoppedOne) {
    expectRows("UNWIND [1, 100000, 0] AS target RETURN target, any(x IN range(1, 100000) WHERE x = target)",
               {{"1", "true"}, {"100000", "true"}, {"0", "false"}});
}

TEST_F(ListPredicateEarlyExitTest, readsEveryElementOfAnUndecidedRow) {
    expectRows("RETURN all(x IN range(1, 100000) WHERE x > 0), single(x IN range(1, 100000) WHERE x = 100000)",
               {{"true", "true"}});
}
