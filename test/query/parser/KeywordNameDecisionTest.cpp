#include <gtest/gtest.h>

#include "ExpectedASTTest.h"

using namespace db;
using namespace turing::test;

// A keyword that opens a construct is also a name. These queries sit on either side of each
// place the parser tells the two apart, and pin what the keyword became.
class KeywordNameDecisionTest : public ExpectedASTTest {
};

TEST_F(KeywordNameDecisionTest, collectBeforeABraceOpensASubquery) {
    expectParse("RETURN COLLECT { MATCH (n) RETURN n }",
                {query({},
                       returning({collectSubquery(query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                                                        returning({symbol("n")})))}))});
}

TEST_F(KeywordNameDecisionTest, collectBeforeAParenthesisCallsTheAggregate) {
    expectParse("RETURN COLLECT(1)", {query({}, returning({invocation({"collect"}, {integer(1)})}))});
}

TEST_F(KeywordNameDecisionTest, collectBeforeAnythingElseIsAVariable) {
    expectParse("RETURN collect", {query({}, returning({symbol("collect")}))});
}

TEST_F(KeywordNameDecisionTest, existsBeforeABraceOpensASubquery) {
    expectParse("RETURN EXISTS { MATCH (n) }",
                {query({}, returning({exists(query({match({._pattern = pattern({path({node({._symbol = "n"})})})})}))}))});
}

TEST_F(KeywordNameDecisionTest, existsBeforeAnythingElseIsAVariable) {
    expectParse("RETURN exists", {query({}, returning({symbol("exists")}))});
}

TEST_F(KeywordNameDecisionTest, countBeforeABraceOpensASubquery) {
    expectParse("RETURN COUNT { MATCH (n) }",
                {query({},
                       returning({countSubquery(query({match({._pattern = pattern({path({node({._symbol = "n"})})})})}))}))});
}

TEST_F(KeywordNameDecisionTest, countBeforeAParenthesisCallsTheAggregate) {
    expectParse("RETURN COUNT(*)", {query({}, returning({invocation({"count"}, {wildcard()})}))});
}

TEST_F(KeywordNameDecisionTest, countBeforeADotIsAVariable) {
    expectParse("RETURN count.x", {query({}, returning({property({"count", "x"})}))});
}

TEST_F(KeywordNameDecisionTest, singleBeforeAParenthesisOpensAPredicate) {
    expectParse("RETURN single(x IN [1] WHERE x = 1)",
                {query({},
                       returning({listPredicate(ListPredicateExpr::Quantifier::Single,
                                                listComprehension("x",
                                                                  list({integer(1)}),
                                                                  binary(BinaryOperator::Equal,
                                                                         symbol("x"),
                                                                         integer(1))))}))});
}

TEST_F(KeywordNameDecisionTest, singleBeforeAnythingElseIsAVariable) {
    expectParse("RETURN single", {query({}, returning({symbol("single")}))});
}

TEST_F(KeywordNameDecisionTest, reduceBeforeAParenthesisOpensAFold) {
    expectParse("RETURN reduce(a = 0, x IN [1] | a + x)",
                {query({},
                       returning({reduce("a",
                                         integer(0),
                                         "x",
                                         list({integer(1)}),
                                         binary(BinaryOperator::Add, symbol("a"), symbol("x")))}))});
}

TEST_F(KeywordNameDecisionTest, reduceBeforeABracketIsAVariable) {
    expectParse("RETURN reduce[0]", {query({}, returning({index(symbol("reduce"), integer(0))}))});
}

TEST_F(KeywordNameDecisionTest, allReduceBeforeAParenthesisOpensAFold) {
    expectParse("RETURN allReduce(a = 0, x IN [1] | a + x, a > 0)",
                {query({},
                       returning({allReduce("a",
                                            integer(0),
                                            "x",
                                            list({integer(1)}),
                                            binary(BinaryOperator::Add, symbol("a"), symbol("x")),
                                            binary(BinaryOperator::GreaterThan, symbol("a"), integer(0)))}))});
}

TEST_F(KeywordNameDecisionTest, allReduceBeforeAnythingElseIsAVariable) {
    expectParse("RETURN allReduce", {query({}, returning({symbol("allReduce")}))});
}

TEST_F(KeywordNameDecisionTest, normalizeBeforeAParenthesisCallsTheFunction) {
    expectParse("RETURN NORMALIZE('a')",
                {query({}, returning({invocation({"normalize"}, {string("a"), normalForm(UnicodeNormalForm::NFC)})}))});
}

TEST_F(KeywordNameDecisionTest, normalizeBeforeAnythingElseIsAVariable) {
    expectParse("RETURN normalize", {query({}, returning({symbol("normalize")}))});
}

TEST_F(KeywordNameDecisionTest, noneBeforeAParenthesisOpensAPredicate) {
    expectParse("RETURN none(x IN [1] WHERE x = 1)",
                {query({},
                       returning({listPredicate(ListPredicateExpr::Quantifier::None,
                                                listComprehension("x",
                                                                  list({integer(1)}),
                                                                  binary(BinaryOperator::Equal,
                                                                         symbol("x"),
                                                                         integer(1))))}))});
}

TEST_F(KeywordNameDecisionTest, anyBeforeAnOperatorIsAVariable) {
    expectParse("RETURN any + 1", {query({}, returning({binary(BinaryOperator::Add, symbol("any"), integer(1))}))});
}

TEST_F(KeywordNameDecisionTest, quantifierBeforeAParenthesisWithoutWhereOpensAPredicate) {
    expectParse("RETURN any(x IN list)",
                {query({},
                       returning({listPredicate(ListPredicateExpr::Quantifier::Any,
                                                listComprehension("x", symbol("list")))}))});

    expectParse("RETURN none(x IN list)",
                {query({},
                       returning({listPredicate(ListPredicateExpr::Quantifier::None,
                                                listComprehension("x", symbol("list")))}))});

    expectParse("RETURN single(x IN list)",
                {query({},
                       returning({listPredicate(ListPredicateExpr::Quantifier::Single,
                                                listComprehension("x", symbol("list")))}))});
}

TEST_F(KeywordNameDecisionTest, filterBeforeAParenthesisCallsAFunction) {
    expectParse("RETURN filter(x IN list)",
                {query({},
                       returning({invocation({"filter"}, {binary(BinaryOperator::In, symbol("x"), symbol("list"))})}))});
}

TEST_F(KeywordNameDecisionTest, anyBeforeAnEqualsSignNamesAPath) {
    expectParse("MATCH any = (a)-[:R]->+(b) RETURN any",
                {query({match({._pattern = pattern({namedPath("any",
                                                              {node({._symbol = "a"}),
                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                     ._quantifier = quantifier(1, QuantifiedPath::UNBOUNDED),
                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                     ._labels = labels({"R"})})}}),
                                                               node({._symbol = "b"})})})})},
                       returning({symbol("any")}))});
}

TEST_F(KeywordNameDecisionTest, allBeforeAParenthesisOpensAPredicate) {
    expectParse("RETURN all(x IN [1] WHERE x = 1)",
                {query({},
                       returning({listPredicate(ListPredicateExpr::Quantifier::All,
                                                listComprehension("x",
                                                                  list({integer(1)}),
                                                                  binary(BinaryOperator::Equal,
                                                                         symbol("x"),
                                                                         integer(1))))}))});
}

TEST_F(KeywordNameDecisionTest, caseBeforeWhenOpensACase) {
    expectParse("RETURN CASE WHEN true THEN 1 END",
                {query({},
                       returning({caseExpr(nullptr, {{._tests = {{._value = boolean(true)}}, ._then = integer(1)}})}))});
}

TEST_F(KeywordNameDecisionTest, caseBeforeANameOpensACaseOnThatSubject) {
    expectParse("RETURN CASE where WHEN 1 THEN 2 END",
                {query({},
                       returning({caseExpr(symbol("where"),
                                           {{._tests = {{._value = integer(1)}}, ._then = integer(2)}})}))});
}

TEST_F(KeywordNameDecisionTest, caseBeforeAnAliasIsAVariable) {
    expectParse("RETURN case AS x", {query({}, returning({named(symbol("case"), "x")}))});
}

TEST_F(KeywordNameDecisionTest, caseBeforeAnInfixWordIsAVariable) {
    expectParse("RETURN case AND true",
                {query({}, returning({logical(LogicalOperator::And, {symbol("case"), boolean(true)})}))});
}

TEST_F(KeywordNameDecisionTest, caseBeforeAsWhereNoAliasFollowsOpensACase) {
    expectParse("MATCH (n) WHERE CASE as WHEN 1 THEN true END RETURN n",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})},
                                                   where(caseExpr(symbol("as"),
                                                                  {{._tests = {{._value = integer(1)}},
                                                                    ._then = boolean(true)}})))})},
                       returning({symbol("n")}))});
}

TEST_F(KeywordNameDecisionTest, notBeforeANameNegatesIt) {
    expectParse("RETURN NOT match.x", {query({}, returning({unary(UnaryOperator::Not, property({"match", "x"}))}))});
}

TEST_F(KeywordNameDecisionTest, notBeforeAnAliasIsAVariable) {
    expectParse("RETURN not AS x", {query({}, returning({named(symbol("not"), "x")}))});
}

TEST_F(KeywordNameDecisionTest, notBeforeAnInfixWordIsAVariable) {
    expectParse("RETURN not OR true",
                {query({}, returning({logical(LogicalOperator::Or, {symbol("not"), boolean(true)})}))});
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnItemProjectsDistinctRows) {
    expectParse("RETURN DISTINCT 1", {query({}, returning(projection({._items = {integer(1)}, ._distinct = true})))});
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnAliasIsAVariable) {
    expectParse("RETURN distinct AS x", {query({}, returning({named(symbol("distinct"), "x")}))});
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnArgumentOfCollectAggregatesDistinctValues) {
    expectParse("RETURN collect(DISTINCT 1)", {query({}, returning({distinct(invocation({"collect"}, {integer(1)}))}))});
}

TEST_F(KeywordNameDecisionTest, distinctBeforeTheCloseOfCollectIsAVariable) {
    expectParse("RETURN collect(DISTINCT)", {query({}, returning({invocation({"collect"}, {symbol("DISTINCT")})}))});
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAsInCollectAggregatesTheVariableAs) {
    expectParse("RETURN collect(DISTINCT as)", {query({}, returning({distinct(invocation({"collect"}, {symbol("as")}))}))});
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnArgumentOfCountCountsDistinctValues) {
    expectParse("RETURN count(DISTINCT 1)", {query({}, returning({distinct(invocation({"count"}, {integer(1)}))}))});
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnInfixWordInCountIsAVariable) {
    expectParse("RETURN count(distinct AND true)",
                {query({},
                       returning({invocation({"count"},
                                             {logical(LogicalOperator::And, {symbol("distinct"), boolean(true)})})}))});
}

TEST_F(KeywordNameDecisionTest, distinctBeforeTheCloseOfCountStaysTheKeyword) {
    expectError("RETURN count(DISTINCT)",
                "     1 | RETURN count(DISTINCT)\n"
                "       |        ^^^^^^^^^^^^^^^\n"
                "-------* Not implemented: count(DISTINCT)");
}

TEST_F(KeywordNameDecisionTest, distinctBeforeTheCloseOfAFunctionStaysTheKeyword) {
    expectError("RETURN toUpper(DISTINCT)",
                "     1 | RETURN toUpper(DISTINCT)\n"
                "       |        ^^^^^^^^^^^^^^^^^\n"
                "-------* DISTINCT requires an argument");
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnArgumentOfAFunctionAppliesItToDistinctValues) {
    expectParse("RETURN toUpper(DISTINCT 'a')", {query({}, returning({distinct(invocation({"toUpper"}, {string("a")}))}))});
}

TEST_F(KeywordNameDecisionTest, whenAfterCaseBeforeATestOpensABranch) {
    expectParse("RETURN CASE WHEN 1 THEN 2 END",
                {query({}, returning({caseExpr(nullptr, {{._tests = {{._value = integer(1)}}, ._then = integer(2)}})}))});
}

TEST_F(KeywordNameDecisionTest, whenAfterCaseBeforeADotIsTheSubject) {
    expectParse("RETURN CASE when.x WHEN 1 THEN 2 END",
                {query({},
                       returning({caseExpr(property({"when", "x"}),
                                           {{._tests = {{._value = integer(1)}}, ._then = integer(2)}})}))});
}

TEST_F(KeywordNameDecisionTest, whereAfterAParenthesisBeforeAnOperandOpensAPredicate) {
    LiteralExpr* filter = boolean(true);

    expectParse("RETURN (WHERE true)",
                {query({},
                       returning({patternPredicate(pattern({path({node({._where = where(filter)})})},
                                                           where(filter)))}))});
}

TEST_F(KeywordNameDecisionTest, whereAfterAParenthesisBeforeAParenthesisCallsAFunction) {
    expectParse("RETURN (where(1))", {query({}, returning({parenthesized(invocation({"where"}, {integer(1)}))}))});
}

TEST_F(KeywordNameDecisionTest, whereAfterAParenthesisBeforeTheCloseIsAVariable) {
    expectParse("RETURN (where)", {query({}, returning({parenthesized(symbol("where"))}))});
}

TEST_F(KeywordNameDecisionTest, whereFirstInANodeBeforeAnOperandOpensAPredicate) {
    LiteralExpr* filter = boolean(true);

    expectParse("MATCH (WHERE true) RETURN 1",
                {query({match({._pattern = pattern({path({node({._where = where(filter)})})}, where(filter))})},
                       returning({integer(1)}))});
}

TEST_F(KeywordNameDecisionTest, whereFirstInANodeBeforeABraceNamesTheNode) {
    expectParse("MATCH (where {a: 1}) RETURN where",
                {query({match({._pattern = pattern({path({node({._symbol = "where",
                                                                ._properties = properties({{"a", integer(1)}})})})})})},
                       returning({symbol("where")}))});
}

TEST_F(KeywordNameDecisionTest, isAfterAParenthesisBeforeALabelOpensAPredicate) {
    expectParse("RETURN (IS Person)",
                {query({}, returning({patternPredicate(pattern({path({node({._labels = labels({"Person"})})})}))}))});
}

TEST_F(KeywordNameDecisionTest, isAfterAParenthesisBeforeAnInfixWordIsAVariable) {
    expectParse("RETURN (is AND true)",
                {query({}, returning({parenthesized(logical(LogicalOperator::And, {symbol("is"), boolean(true)}))}))});
}

TEST_F(KeywordNameDecisionTest, isFirstInANodeBeforeANameOpensItsLabels) {
    expectParse("MATCH (IS AND) RETURN 1",
                {query({match({._pattern = pattern({path({node({._labels = labels({"AND"})})})})})},
                       returning({integer(1)}))});
}

TEST_F(KeywordNameDecisionTest, isFirstInANodeBeforeAColonNamesTheNode) {
    expectParse("MATCH (is:Person) RETURN is",
                {query({match({._pattern = pattern({path({node({._symbol = "is", ._labels = labels({"Person"})})})})})},
                       returning({symbol("is")}))});
}

TEST_F(KeywordNameDecisionTest, normalizedAfterIsOpensANormalizationTest) {
    expectParse("MATCH (x) WHERE x IS NORMALIZED RETURN x",
                {query({match({._pattern = pattern({path({node({._symbol = "x"})})},
                                                   where(invocation({"is_normalized"},
                                                                    {symbol("x"), normalForm(UnicodeNormalForm::NFC)})))})},
                       returning({symbol("x")}))});
}

TEST_F(KeywordNameDecisionTest, aNameAfterIsTestsALabel) {
    expectParse("MATCH (x) WHERE x IS Person RETURN x",
                {query({match({._pattern = pattern({path({node({._symbol = "x"})})}, where(entityType("x", labels({"Person"}))))})},
                       returning({symbol("x")}))});
}
