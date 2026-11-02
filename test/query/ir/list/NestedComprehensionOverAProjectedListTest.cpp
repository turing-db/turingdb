#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class NestedComprehensionOverAProjectedListTest : public WriteQueryTest {
};

TEST_F(NestedComprehensionOverAProjectedListTest, buildsAComprehensionPerElement) {
    expectRows("WITH [1, 2, 3] AS l RETURN [x IN l | [y IN l | x * y]]", {{"[[1, 2, 3], [2, 4, 6], [3, 6, 9]]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, foldsAReducePerElement) {
    expectRows("WITH [1, 2, 3] AS l RETURN [x IN l | reduce(s = 0, y IN l | s + x * y)]", {{"[6, 12, 18]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, foldsAReduceThatReadsNoOuterElement) {
    expectRows("WITH [1, 2, 3] AS l RETURN [x IN l | reduce(s = 0, y IN l | s + y)]", {{"[6, 6, 6]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, decidesAnAllReducePerElement) {
    expectRows("WITH [1, 2, 3] AS l RETURN [x IN l | allReduce(s = 0, y IN l | s + x * y, s < 10)]",
               {{"[true, false, false]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, sizesAFilteredComprehensionPerElement) {
    expectRows("WITH [1, 2, 3] AS l RETURN [x IN l | size([y IN l WHERE y < x])]", {{"[0, 1, 2]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, foldsAReduceInsideAListPredicate) {
    expectRows("WITH [1, 2, 3] AS l RETURN all(x IN l WHERE reduce(s = 0, y IN l | s + x * y) > 6)", {{"false"}});
    expectRows("WITH [1, 2, 3] AS l RETURN any(x IN l WHERE reduce(s = 0, y IN l | s + x * y) > 6)", {{"true"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, slicesTheListPerElement) {
    expectRows("WITH [1, 2, 3] AS l RETURN [x IN l | [y IN l[0..x] | x * y]]", {{"[[1], [2, 4], [3, 6, 9]]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, readsAnotherProjectedConstant) {
    expectRows("WITH [1, 2] AS l, 10 AS k RETURN [x IN l | [y IN l | x * y + k]]", {{"[[11, 12], [12, 14]]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, nestsThreeComprehensionsOverOneList) {
    expectRows("WITH [1, 2] AS l RETURN [x IN l | [y IN l | [z IN l | x * y * z]]]",
               {{"[[[1, 2], [2, 4]], [[2, 4], [4, 8]]]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, nestsTheListUnderASecondList) {
    expectRows("WITH [1, 2] AS l, [1, 2] AS m RETURN [x IN l | [y IN m | [z IN l | x * y * z]]]",
               {{"[[[1, 2], [2, 4]], [[2, 4], [4, 8]]]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, buildsTheNestedListInACaseBranch) {
    expectRows("WITH [1, 2, 3] AS l RETURN CASE WHEN size(l) > 2 THEN [x IN l | [y IN l | x * y]] ELSE [] END",
               {{"[[1, 2, 3], [2, 4, 6], [3, 6, 9]]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, buildsTheNestedListAsACoalesceArgument) {
    expectRows("WITH [1, 2, 3] AS l RETURN coalesce(null, [x IN l | [y IN l | x * y]])",
               {{"[[1, 2, 3], [2, 4, 6], [3, 6, 9]]"}});
}

TEST_F(NestedComprehensionOverAProjectedListTest, raisesInTheInnerBody) {
    expectError("WITH [1, 0] AS l RETURN [x IN l | [y IN l | x / y]]", "Attempted to divide by zero");
}

TEST_F(NestedComprehensionOverAProjectedListTest, raisesNothingWhenNoRowReachesIt) {
    expectRows("MATCH (n:Nothing) WITH [1, 0] AS l RETURN [x IN l | [y IN l | x / y]]", {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
