#include <gtest/gtest.h>

#include <initializer_list>
#include <string_view>

#include "ExpectedASTTest.h"

using namespace db;
using namespace turing::test;

// Each query reads as a pattern or as an expression until a token past the first one decides,
// or for good. These pin the reading the parser chooses on both sides of each decision.
class PatternOrExpressionTest : public ExpectedASTTest {
protected:
    void expectFilter(std::string_view text, std::string_view first, std::string_view second, Expr* filter) {
        expectParse(text,
                    {query({match({._pattern = pattern({path({node({._symbol = first})}), path({node({._symbol = second})})},
                                                       where(filter))})},
                           returning({symbol(first)}))});
    }

    ExistsExpr* pathPredicate(std::initializer_list<EntityPattern*> entities) {
        return patternPredicate(pattern({path(entities)}));
    }
};

TEST_F(PatternOrExpressionTest, readsEmptyBracketsBetweenDashesAsARelationship) {
    expectFilter("MATCH (a), (b) WHERE (a)-[]-(b) RETURN a",
                 "a",
                 "b",
                 pathPredicate({node({._symbol = "a"}), edge({}), node({._symbol = "b"})}));
}

TEST_F(PatternOrExpressionTest, readsAMapInBracketsBetweenDashesAsARelationship) {
    expectFilter("MATCH (a), (b) WHERE (a)-[{since: 2020}]-(b) RETURN a",
                 "a",
                 "b",
                 pathPredicate({node({._symbol = "a"}),
                                edge({._properties = properties({{"since", integer(2020)}})}),
                                node({._symbol = "b"})}));
}

TEST_F(PatternOrExpressionTest, readsALabelDisjunctionInBracketsAsRelationshipTypes) {
    expectFilter("MATCH (a), (b) WHERE (a)-[r:KNOWS|LIKES|FOLLOWS]->(b) RETURN a",
                 "a",
                 "b",
                 pathPredicate({node({._symbol = "a"}),
                                edge({._direction = EdgePattern::Direction::Forward,
                                      ._symbol = "r",
                                      ._labels = labelOr({labels({"KNOWS"}), labels({"LIKES"}), labels({"FOLLOWS"})})}),
                                node({._symbol = "b"})}));

    expectFilter("MATCH (a), (b) WHERE (a)-[r:KNOWS|LIKES|FOLLOWS]-(b) RETURN a",
                 "a",
                 "b",
                 pathPredicate({node({._symbol = "a"}),
                                edge({._symbol = "r",
                                      ._labels = labelOr({labels({"KNOWS"}), labels({"LIKES"}), labels({"FOLLOWS"})})}),
                                node({._symbol = "b"})}));
}

TEST_F(PatternOrExpressionTest, readsAParenthesizedVariableWithAMapAsANodeOnlyBeforeARelationship) {
    expectFilter("MATCH (n), (m) WHERE (n {name: 'Ann'})-->(m) RETURN n",
                 "n",
                 "m",
                 pathPredicate({node({._symbol = "n", ._properties = properties({{"name", string("Ann")}})}),
                                edge({._direction = EdgePattern::Direction::Forward}),
                                node({._symbol = "m"})}));

    expectFilter("MATCH (n), (m) WHERE (n {name: 'Ann'})--(m) RETURN n",
                 "n",
                 "m",
                 pathPredicate({node({._symbol = "n", ._properties = properties({{"name", string("Ann")}})}),
                                edge({}),
                                node({._symbol = "m"})}));

    expectFilter("MATCH (n), (m) WHERE (n {name: 'Ann'}) IS NOT NULL RETURN n",
                 "n",
                 "m",
                 binary(BinaryOperator::IsNotNull,
                        parenthesized(mapProjection("n",
                                                    {{._kind = MapProjectionExpr::ItemKind::Literal,
                                                      ._key = name("name"),
                                                      ._value = string("Ann")}})),
                        null()));
}

TEST_F(PatternOrExpressionTest, readsParenthesizedLabelsAsANodeOnlyBeforeARelationship) {
    expectFilter("MATCH (n), (m) WHERE (n:Person:Employee)-->(m) RETURN n",
                 "n",
                 "m",
                 pathPredicate({node({._symbol = "n", ._labels = labels({"Person", "Employee"})}),
                                edge({._direction = EdgePattern::Direction::Forward}),
                                node({._symbol = "m"})}));

    expectFilter("MATCH (n), (m) WHERE (n:Person:Employee) AND m.x RETURN n",
                 "n",
                 "m",
                 logical(LogicalOperator::And,
                         {parenthesized(entityType("n", labels({"Person", "Employee"}))), property({"m", "x"})}));
}

TEST_F(PatternOrExpressionTest, readsArrowsAfterAnOperandAsOperators) {
    expectParse("RETURN x<-1, y<--1, z--1",
                {query({},
                       returning({binary(BinaryOperator::LessThan, symbol("x"), integer(-1)),
                                  binary(BinaryOperator::LessThan, symbol("y"), integer(1)),
                                  binary(BinaryOperator::Sub, symbol("z"), integer(-1))}))});
}

TEST_F(PatternOrExpressionTest, readsAPatternInBracketsAsAComprehensionOnlyBeforeTheBar) {
    expectParse("RETURN [(a)-->(b) | b.name]",
                {query({},
                       returning({patternComprehension(match({._pattern = pattern({path({node({._symbol = "a"}),
                                                                                         edge({._direction = EdgePattern::Direction::Forward}),
                                                                                         node({._symbol = "b"})})})}),
                                                       property({"b", "name"}))}))});

    expectParse("RETURN [(a)-->(b), true]",
                {query({},
                       returning({list({pathPredicate({node({._symbol = "a"}),
                                                       edge({._direction = EdgePattern::Direction::Forward}),
                                                       node({._symbol = "b"})}),
                                        boolean(true)})}))});
}

TEST_F(PatternOrExpressionTest, readsANameBeforeAPatternInBracketsAsAPathOnlyBeforeTheBar) {
    expectParse("RETURN [p = (a)-->(b) | length(p)]",
                {query({},
                       returning({patternComprehension(match({._pattern = pattern({namedPath("p",
                                                                                             {node({._symbol = "a"}),
                                                                                              edge({._direction = EdgePattern::Direction::Forward}),
                                                                                              node({._symbol = "b"})})})}),
                                                       invocation({"length"}, {symbol("p")}))}))});

    expectParse("RETURN [p = (a)-->(b)]",
                {query({},
                       returning({list({binary(BinaryOperator::Equal,
                                               symbol("p"),
                                               pathPredicate({node({._symbol = "a"}),
                                                              edge({._direction = EdgePattern::Direction::Forward}),
                                                              node({._symbol = "b"})}))})}))});
}
