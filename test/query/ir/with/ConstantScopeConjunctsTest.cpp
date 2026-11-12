#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

// A WHERE of several conjuncts over a scope of constants alone keeps its single row only when
// every conjunct holds, whichever conjunct fails
class ConstantScopeConjunctsTest : public WriteQueryTest {
};

TEST_F(ConstantScopeConjunctsTest, dropsTheRowALaterConjunctFails) {
    expectRows("WITH 1 AS x, 200 AS y WHERE x < 5 AND y < 100 RETURN x, y", {});
    expectRows("WITH 1 AS x, 200 AS y WHERE y < 100 AND x < 5 RETURN x, y", {});
    expectRows("WITH 1 AS x, 200 AS y WHERE x < 5 AND y < 100 RETURN count(*)", {{"0"}});
    expectRows("WITH 'abc' AS s, 200 AS y WHERE s STARTS WITH 'a' AND y < 100 RETURN s, y", {});
    expectRows("WITH [1, 2, 3] AS l, 200 AS y WHERE size(l) = 3 AND y < 100 RETURN l, y", {});
    expectRows("WITH 1 AS x, 200 AS y WHERE x < 5 AND y = 100 RETURN x, y", {});
    expectRows("WITH 1 AS x, 200 AS y WHERE x < 5 AND y = 100 RETURN count(*)", {{"0"}});
}

TEST_F(ConstantScopeConjunctsTest, keepsTheRowEveryConjunctHolds) {
    expectRows("WITH 1 AS x, 50 AS y WHERE x < 5 AND y < 100 RETURN x, y", {{"1", "50"}});
    expectRows("WITH 1 AS x, 50 AS y WHERE x < 5 AND y < 100 RETURN count(*)", {{"1"}});
}
