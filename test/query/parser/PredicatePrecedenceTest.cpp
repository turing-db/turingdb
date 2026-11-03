#include <gtest/gtest.h>

#include "ExpectedASTTest.h"

using namespace db;
using namespace turing::test;

// STARTS WITH, ENDS WITH, CONTAINS, IN and the IS tests share one level, below + and
// above the comparisons, as in openCypher's oC_StringListNullPredicateExpression
class PredicatePrecedenceTest : public ExpectedASTTest {
};

TEST_F(PredicatePrecedenceTest, bindsStringPredicatesLooserThanArithmetic) {
    expectParse("RETURN 'ab' + 'b' STARTS WITH 'a'",
                {query({},
                       returning({stringExpr(StringOperator::StartsWith,
                                             binary(BinaryOperator::Add, string("ab"), string("b")),
                                             string("a"))}))});

    expectParse("RETURN 'ab' + 'b' CONTAINS 'b'",
                {query({},
                       returning({stringExpr(StringOperator::Contains,
                                             binary(BinaryOperator::Add, string("ab"), string("b")),
                                             string("b"))}))});

    expectParse("RETURN 'a' CONTAINS 'a' + 'b'",
                {query({},
                       returning({stringExpr(StringOperator::Contains,
                                             string("a"),
                                             binary(BinaryOperator::Add, string("a"), string("b")))}))});

    expectParse("RETURN 'a' || 'b' ENDS WITH 'b' || 'c'",
                {query({},
                       returning({stringExpr(StringOperator::EndsWith,
                                             binary(BinaryOperator::Concat, string("a"), string("b")),
                                             binary(BinaryOperator::Concat, string("b"), string("c")))}))});

    expectParse("RETURN 1 - 2 STARTS WITH 3 * 4",
                {query({},
                       returning({stringExpr(StringOperator::StartsWith,
                                             binary(BinaryOperator::Sub, integer(1), integer(2)),
                                             binary(BinaryOperator::Mult, integer(3), integer(4)))}))});

    expectParse("RETURN 1 / 2 CONTAINS 3 % 4 ^ 5",
                {query({},
                       returning({stringExpr(StringOperator::Contains,
                                             binary(BinaryOperator::Div, integer(1), integer(2)),
                                             binary(BinaryOperator::Mod,
                                                    integer(3),
                                                    binary(BinaryOperator::Pow, integer(4), integer(5))))}))});
}

TEST_F(PredicatePrecedenceTest, bindsInLooserThanArithmetic) {
    expectParse("RETURN 1 + 1 IN [2]",
                {query({},
                       returning({binary(BinaryOperator::In,
                                         binary(BinaryOperator::Add, integer(1), integer(1)),
                                         list({integer(2)}))}))});

    expectParse("RETURN 1 IN [1] + [2]",
                {query({},
                       returning({binary(BinaryOperator::In,
                                         integer(1),
                                         binary(BinaryOperator::Add, list({integer(1)}), list({integer(2)})))}))});
}

TEST_F(PredicatePrecedenceTest, bindsIsTestsLooserThanArithmetic) {
    expectParse("RETURN 1 + null IS NULL",
                {query({},
                       returning({binary(BinaryOperator::IsNull,
                                         binary(BinaryOperator::Add, integer(1), null()),
                                         null())}))});

    expectParse("RETURN 'a' + 'b' IS NOT NFD NORMALIZED",
                {query({},
                       returning({unary(UnaryOperator::Not,
                                        invocation({"is_normalized"},
                                                   {binary(BinaryOperator::Add, string("a"), string("b")),
                                                    normalForm(UnicodeNormalForm::NFD)}))}))});
}

TEST_F(PredicatePrecedenceTest, bindsPredicatesTighterThanComparisons) {
    expectParse("RETURN true = 'ab' STARTS WITH 'a'",
                {query({},
                       returning({binary(BinaryOperator::Equal,
                                         boolean(true),
                                         stringExpr(StringOperator::StartsWith, string("ab"), string("a")))}))});

    expectParse("RETURN 'ab' ENDS WITH 'b' <> false",
                {query({},
                       returning({binary(BinaryOperator::NotEqual,
                                         stringExpr(StringOperator::EndsWith, string("ab"), string("b")),
                                         boolean(false))}))});

    expectParse("RETURN false = true IN [true, false]",
                {query({},
                       returning({binary(BinaryOperator::Equal,
                                         boolean(false),
                                         binary(BinaryOperator::In,
                                                boolean(true),
                                                list({boolean(true), boolean(false)})))}))});

    expectParse("RETURN true IN [true] = false",
                {query({},
                       returning({binary(BinaryOperator::Equal,
                                         binary(BinaryOperator::In, boolean(true), list({boolean(true)})),
                                         boolean(false))}))});

    expectParse("RETURN false = null IS NULL",
                {query({},
                       returning({binary(BinaryOperator::Equal,
                                         boolean(false),
                                         binary(BinaryOperator::IsNull, null(), null()))}))});

    expectParse("RETURN 1 < null IS NOT NULL",
                {query({},
                       returning({binary(BinaryOperator::LessThan,
                                         integer(1),
                                         binary(BinaryOperator::IsNotNull, null(), null()))}))});

    expectParse("RETURN true = 'a' IS NORMALIZED",
                {query({},
                       returning({binary(BinaryOperator::Equal,
                                         boolean(true),
                                         invocation({"is_normalized"},
                                                    {string("a"), normalForm(UnicodeNormalForm::NFC)}))}))});
}

TEST_F(PredicatePrecedenceTest, chainsComparisonsOverPredicates) {
    expectParse("RETURN 1 = 2 IN [2] = true",
                {query({},
                       returning({comparisonChain(integer(1),
                                                  {{BinaryOperator::Equal,
                                                    binary(BinaryOperator::In, integer(2), list({integer(2)}))},
                                                   {BinaryOperator::Equal, boolean(true)}})}))});

    expectParse("RETURN 0 < null IS NULL <= 1",
                {query({},
                       returning({comparisonChain(integer(0),
                                                  {{BinaryOperator::LessThan,
                                                    binary(BinaryOperator::IsNull, null(), null())},
                                                   {BinaryOperator::LessThanOrEqual, integer(1)}})}))});
}

TEST_F(PredicatePrecedenceTest, appliesPredicatesFromLeftToRight) {
    expectParse("RETURN 'ab' STARTS WITH 'a' IN [true]",
                {query({},
                       returning({binary(BinaryOperator::In,
                                         stringExpr(StringOperator::StartsWith, string("ab"), string("a")),
                                         list({boolean(true)}))}))});

    expectParse("RETURN true IN [true] CONTAINS 'e'",
                {query({},
                       returning({stringExpr(StringOperator::Contains,
                                             binary(BinaryOperator::In, boolean(true), list({boolean(true)})),
                                             string("e"))}))});

    expectParse("RETURN null IS NULL IS NOT NULL",
                {query({},
                       returning({binary(BinaryOperator::IsNotNull,
                                         binary(BinaryOperator::IsNull, null(), null()),
                                         null())}))});

    expectParse("RETURN null IS NULL STARTS WITH 't'",
                {query({},
                       returning({stringExpr(StringOperator::StartsWith,
                                             binary(BinaryOperator::IsNull, null(), null()),
                                             string("t"))}))});
}

TEST_F(PredicatePrecedenceTest, bindsNegationLooserThanPredicates) {
    expectParse("RETURN NOT 'ab' + 'b' STARTS WITH 'a'",
                {query({},
                       returning({unary(UnaryOperator::Not,
                                        stringExpr(StringOperator::StartsWith,
                                                   binary(BinaryOperator::Add, string("ab"), string("b")),
                                                   string("a")))}))});

    expectParse("RETURN NOT false = true IN [true]",
                {query({},
                       returning({unary(UnaryOperator::Not,
                                        binary(BinaryOperator::Equal,
                                               boolean(false),
                                               binary(BinaryOperator::In, boolean(true), list({boolean(true)}))))}))});
}

TEST_F(PredicatePrecedenceTest, rejectsArithmeticAfterAnIsTest) {
    expectError("RETURN null IS NULL + 1",
                "     1 | RETURN null IS NULL + 1\n"
                "       |                     ^\n"
                "-------* syntax error, unexpected PLUS");
}
