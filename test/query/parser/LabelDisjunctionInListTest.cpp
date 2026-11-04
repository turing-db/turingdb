#include <gtest/gtest.h>

#include "ExpectedASTTest.h"

using namespace db;
using namespace turing::test;

class LabelDisjunctionInListTest : public ExpectedASTTest {
};

TEST_F(LabelDisjunctionInListTest, readsTheFirstItemOfAListAsALabelDisjunction) {
    expectParse("MATCH (n) RETURN [n:A|B]",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                       returning({list({entityType("n", labelOr({labels({"A"}), labels({"B"})}))})}))});

    expectParse("MATCH (n) RETURN [n:A | B]",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                       returning({list({entityType("n", labelOr({labels({"A"}), labels({"B"})}))})}))});

    expectParse("MATCH (n) RETURN [n:A|B, 1]",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                       returning({list({entityType("n", labelOr({labels({"A"}), labels({"B"})})), integer(1)})}))});
}

TEST_F(LabelDisjunctionInListTest, readsALabelDisjunctionInAListThatStartsLikeAComprehension) {
    expectParse("MATCH (n) RETURN [x IN [1] AND n:A|B, 1]",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                       returning({list({logical(LogicalOperator::And,
                                                {binary(BinaryOperator::In, symbol("x"), list({integer(1)})),
                                                 entityType("n", labelOr({labels({"A"}), labels({"B"})}))}),
                                        integer(1)})}))});
}
