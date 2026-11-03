#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "CypherAST.h"
#include "CypherParser.h"
#include "FunctionInvocation.h"
#include "LabelExpr.h"
#include "NodePattern.h"
#include "ParserException.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Projection.h"
#include "QualifiedName.h"
#include "SinglePartQuery.h"
#include "Symbol.h"
#include "SymbolChain.h"
#include "WhereClause.h"
#include "expr/All.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/StmtContainer.h"

using namespace db;

// A keyword that opens a construct is also a name. These queries sit on either side of each
// place the parser tells the two apart, and pin what the keyword became.
class KeywordNameDecisionTest : public ::testing::Test {
protected:
    void parseQuery(std::string_view query) {
        _query = query;
        _ast = std::make_unique<CypherAST>(nullptr, _query);

        CypherParser parser(_ast.get());
        parser.parse(_query);
    }

    const SinglePartQuery* parseSingleQuery(std::string_view query) {
        parseQuery(query);
        return static_cast<const SinglePartQuery*>(_ast->queries().front());
    }

    const Projection* returnedProjection(std::string_view query) {
        return parseSingleQuery(query)->getReturnStmt()->getProjection();
    }

    const Expr* returnedItem(std::string_view query) {
        return std::get<Expr*>(returnedProjection(query)->items().front());
    }

    const MatchStmt* firstMatch(std::string_view query) {
        const SinglePartQuery* single = parseSingleQuery(query);
        return static_cast<const MatchStmt*>(single->getStmts()->stmts().front());
    }

    const NodePattern* matchedNode(std::string_view query) {
        const Pattern* pattern = firstMatch(query)->getPattern();
        return static_cast<const NodePattern*>(pattern->elements().front()->getEntities().front());
    }

    const Expr* matchFilter(std::string_view query) {
        return firstMatch(query)->getPattern()->getWhere()->getExpr();
    }

    static const NodePattern* predicateRoot(const Expr* expr) {
        const Pattern* pattern = static_cast<const ExistsExpr*>(expr)->getPredicatePattern();
        return static_cast<const NodePattern*>(pattern->elements().front()->getEntities().front());
    }

    static void expectSymbol(const Expr* expr, std::string_view name) {
        ASSERT_EQ(expr->getKind(), Expr::Kind::SYMBOL);
        EXPECT_EQ(static_cast<const SymbolExpr*>(expr)->getSymbol()->getName(), name);
    }

    static const FunctionInvocation* invocation(const Expr* expr) {
        EXPECT_EQ(expr->getKind(), Expr::Kind::FUNCTION_INVOCATION);
        return static_cast<const FunctionInvocationExpr*>(expr)->getFunctionInvocation();
    }

    static const Expr* firstArgument(const FunctionInvocation* invocation) {
        return invocation->getArguments()->getExprs().front();
    }

    static std::string_view labelOf(const LabelExpr* labels) {
        return labels->getLabels()->front()->getName();
    }

    void expectParseError(std::string_view query, std::string_view message) {
        try {
            parseQuery(query);
        } catch (const ParserException& error) {
            EXPECT_NE(std::string_view {error.what()}.find(message), std::string_view::npos) << error.what();
            return;
        }

        ADD_FAILURE() << "parsed: " << query;
    }

    std::string _query;
    std::unique_ptr<CypherAST> _ast;
};

TEST_F(KeywordNameDecisionTest, collectBeforeABraceOpensASubquery) {
    EXPECT_EQ(returnedItem("RETURN COLLECT { MATCH (n) RETURN n }")->getKind(), Expr::Kind::COLLECT_SUBQUERY);
}

TEST_F(KeywordNameDecisionTest, collectBeforeAParenthesisCallsTheAggregate) {
    const FunctionInvocation* collect = invocation(returnedItem("RETURN COLLECT(1)"));
    EXPECT_EQ(collect->getName()->names().front()->getName(), "collect");
}

TEST_F(KeywordNameDecisionTest, collectBeforeAnythingElseIsAVariable) {
    expectSymbol(returnedItem("RETURN collect"), "collect");
}

TEST_F(KeywordNameDecisionTest, existsBeforeABraceOpensASubquery) {
    EXPECT_EQ(returnedItem("RETURN EXISTS { MATCH (n) }")->getKind(), Expr::Kind::EXISTS);
}

TEST_F(KeywordNameDecisionTest, existsBeforeAnythingElseIsAVariable) {
    expectSymbol(returnedItem("RETURN exists"), "exists");
}

TEST_F(KeywordNameDecisionTest, countBeforeABraceOpensASubquery) {
    EXPECT_EQ(returnedItem("RETURN COUNT { MATCH (n) }")->getKind(), Expr::Kind::COUNT_SUBQUERY);
}

TEST_F(KeywordNameDecisionTest, countBeforeAParenthesisCallsTheAggregate) {
    const FunctionInvocation* count = invocation(returnedItem("RETURN COUNT(*)"));
    EXPECT_EQ(count->getName()->names().front()->getName(), "count");
}

TEST_F(KeywordNameDecisionTest, countBeforeADotIsAVariable) {
    EXPECT_EQ(returnedItem("RETURN count.x")->getKind(), Expr::Kind::PROPERTY);
}

TEST_F(KeywordNameDecisionTest, singleBeforeAParenthesisOpensAPredicate) {
    EXPECT_EQ(returnedItem("RETURN single(x IN [1] WHERE x = 1)")->getKind(), Expr::Kind::LIST_PREDICATE);
}

TEST_F(KeywordNameDecisionTest, singleBeforeAnythingElseIsAVariable) {
    expectSymbol(returnedItem("RETURN single"), "single");
}

TEST_F(KeywordNameDecisionTest, reduceBeforeAParenthesisOpensAFold) {
    EXPECT_EQ(returnedItem("RETURN reduce(a = 0, x IN [1] | a + x)")->getKind(), Expr::Kind::REDUCE);
}

TEST_F(KeywordNameDecisionTest, reduceBeforeABracketIsAVariable) {
    const Expr* index = returnedItem("RETURN reduce[0]");
    ASSERT_EQ(index->getKind(), Expr::Kind::INDEX);
    expectSymbol(static_cast<const IndexExpr*>(index)->getBase(), "reduce");
}

TEST_F(KeywordNameDecisionTest, allReduceBeforeAParenthesisOpensAFold) {
    EXPECT_EQ(returnedItem("RETURN allReduce(a = 0, x IN [1] | a + x, a > 0)")->getKind(), Expr::Kind::ALL_REDUCE);
}

TEST_F(KeywordNameDecisionTest, allReduceBeforeAnythingElseIsAVariable) {
    expectSymbol(returnedItem("RETURN allReduce"), "allReduce");
}

TEST_F(KeywordNameDecisionTest, normalizeBeforeAParenthesisCallsTheFunction) {
    const FunctionInvocation* normalize = invocation(returnedItem("RETURN NORMALIZE('a')"));
    EXPECT_EQ(normalize->getName()->names().front()->getName(), "normalize");
}

TEST_F(KeywordNameDecisionTest, normalizeBeforeAnythingElseIsAVariable) {
    expectSymbol(returnedItem("RETURN normalize"), "normalize");
}

TEST_F(KeywordNameDecisionTest, noneBeforeAParenthesisOpensAPredicate) {
    EXPECT_EQ(returnedItem("RETURN none(x IN [1] WHERE x = 1)")->getKind(), Expr::Kind::LIST_PREDICATE);
}

TEST_F(KeywordNameDecisionTest, anyBeforeAnOperatorIsAVariable) {
    const Expr* sum = returnedItem("RETURN any + 1");
    ASSERT_EQ(sum->getKind(), Expr::Kind::BINARY);
    expectSymbol(static_cast<const BinaryExpr*>(sum)->getLHS(), "any");
}

TEST_F(KeywordNameDecisionTest, allBeforeAParenthesisOpensAPredicate) {
    EXPECT_EQ(returnedItem("RETURN all(x IN [1] WHERE x = 1)")->getKind(), Expr::Kind::LIST_PREDICATE);
}

TEST_F(KeywordNameDecisionTest, caseBeforeWhenOpensACase) {
    const Expr* caseExpr = returnedItem("RETURN CASE WHEN true THEN 1 END");
    ASSERT_EQ(caseExpr->getKind(), Expr::Kind::CASE);
    EXPECT_EQ(static_cast<const CaseExpr*>(caseExpr)->getSubject(), nullptr);
}

TEST_F(KeywordNameDecisionTest, caseBeforeANameOpensACaseOnThatSubject) {
    const Expr* caseExpr = returnedItem("RETURN CASE where WHEN 1 THEN 2 END");
    ASSERT_EQ(caseExpr->getKind(), Expr::Kind::CASE);
    expectSymbol(static_cast<const CaseExpr*>(caseExpr)->getSubject(), "where");
}

TEST_F(KeywordNameDecisionTest, caseBeforeAnAliasIsAVariable) {
    const Expr* item = returnedItem("RETURN case AS x");
    expectSymbol(item, "case");
    EXPECT_EQ(item->getName(), "x");
}

TEST_F(KeywordNameDecisionTest, caseBeforeAnInfixWordIsAVariable) {
    EXPECT_EQ(returnedItem("RETURN case AND true")->getKind(), Expr::Kind::LOGICAL);
}

TEST_F(KeywordNameDecisionTest, caseBeforeAsWhereNoAliasFollowsOpensACase) {
    const Expr* caseExpr = matchFilter("MATCH (n) WHERE CASE as WHEN 1 THEN true END RETURN n");
    ASSERT_EQ(caseExpr->getKind(), Expr::Kind::CASE);
    expectSymbol(static_cast<const CaseExpr*>(caseExpr)->getSubject(), "as");
}

TEST_F(KeywordNameDecisionTest, notBeforeANameNegatesIt) {
    const Expr* negation = returnedItem("RETURN NOT match.x");
    ASSERT_EQ(negation->getKind(), Expr::Kind::UNARY);
    EXPECT_EQ(static_cast<const UnaryExpr*>(negation)->getOperator(), UnaryOperator::Not);
}

TEST_F(KeywordNameDecisionTest, notBeforeAnAliasIsAVariable) {
    const Expr* item = returnedItem("RETURN not AS x");
    expectSymbol(item, "not");
    EXPECT_EQ(item->getName(), "x");
}

TEST_F(KeywordNameDecisionTest, notBeforeAnInfixWordIsAVariable) {
    EXPECT_EQ(returnedItem("RETURN not OR true")->getKind(), Expr::Kind::LOGICAL);
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnItemProjectsDistinctRows) {
    const Projection* projection = returnedProjection("RETURN DISTINCT 1");
    EXPECT_TRUE(projection->isDistinct());
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnAliasIsAVariable) {
    const Projection* projection = returnedProjection("RETURN distinct AS x");
    EXPECT_FALSE(projection->isDistinct());

    const Expr* item = std::get<Expr*>(projection->items().front());
    expectSymbol(item, "distinct");
    EXPECT_EQ(item->getName(), "x");
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnArgumentOfCollectAggregatesDistinctValues) {
    EXPECT_TRUE(invocation(returnedItem("RETURN collect(DISTINCT 1)"))->isDistinct());
}

TEST_F(KeywordNameDecisionTest, distinctBeforeTheCloseOfCollectIsAVariable) {
    const FunctionInvocation* collect = invocation(returnedItem("RETURN collect(DISTINCT)"));
    EXPECT_FALSE(collect->isDistinct());
    expectSymbol(firstArgument(collect), "DISTINCT");
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAsInCollectAggregatesTheVariableAs) {
    const FunctionInvocation* collect = invocation(returnedItem("RETURN collect(DISTINCT as)"));
    EXPECT_TRUE(collect->isDistinct());
    expectSymbol(firstArgument(collect), "as");
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnArgumentOfCountCountsDistinctValues) {
    EXPECT_TRUE(invocation(returnedItem("RETURN count(DISTINCT 1)"))->isDistinct());
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnInfixWordInCountIsAVariable) {
    const FunctionInvocation* count = invocation(returnedItem("RETURN count(distinct AND true)"));
    EXPECT_FALSE(count->isDistinct());
    EXPECT_EQ(firstArgument(count)->getKind(), Expr::Kind::LOGICAL);
}

TEST_F(KeywordNameDecisionTest, distinctBeforeTheCloseOfCountStaysTheKeyword) {
    expectParseError("RETURN count(DISTINCT)", "Not implemented: count(DISTINCT)");
}

TEST_F(KeywordNameDecisionTest, distinctBeforeTheCloseOfAFunctionStaysTheKeyword) {
    expectParseError("RETURN toUpper(DISTINCT)", "DISTINCT requires an argument");
}

TEST_F(KeywordNameDecisionTest, distinctBeforeAnArgumentOfAFunctionAppliesItToDistinctValues) {
    EXPECT_TRUE(invocation(returnedItem("RETURN toUpper(DISTINCT 'a')"))->isDistinct());
}

TEST_F(KeywordNameDecisionTest, whenAfterCaseBeforeATestOpensABranch) {
    const Expr* caseExpr = returnedItem("RETURN CASE WHEN 1 THEN 2 END");
    ASSERT_EQ(caseExpr->getKind(), Expr::Kind::CASE);
    EXPECT_EQ(static_cast<const CaseExpr*>(caseExpr)->getSubject(), nullptr);
}

TEST_F(KeywordNameDecisionTest, whenAfterCaseBeforeADotIsTheSubject) {
    const Expr* caseExpr = returnedItem("RETURN CASE when.x WHEN 1 THEN 2 END");
    ASSERT_EQ(caseExpr->getKind(), Expr::Kind::CASE);
    EXPECT_EQ(static_cast<const CaseExpr*>(caseExpr)->getSubject()->getKind(), Expr::Kind::PROPERTY);
}

TEST_F(KeywordNameDecisionTest, whereAfterAParenthesisBeforeAnOperandOpensAPredicate) {
    const Expr* predicate = returnedItem("RETURN (WHERE true)");
    ASSERT_EQ(predicate->getKind(), Expr::Kind::EXISTS);
    EXPECT_NE(predicateRoot(predicate)->getWhere(), nullptr);
}

TEST_F(KeywordNameDecisionTest, whereAfterAParenthesisBeforeAParenthesisCallsAFunction) {
    const FunctionInvocation* where = invocation(returnedItem("RETURN (where(1))"));
    EXPECT_EQ(where->getName()->names().front()->getName(), "where");
}

TEST_F(KeywordNameDecisionTest, whereAfterAParenthesisBeforeTheCloseIsAVariable) {
    expectSymbol(returnedItem("RETURN (where)"), "where");
}

TEST_F(KeywordNameDecisionTest, whereFirstInANodeBeforeAnOperandOpensAPredicate) {
    const NodePattern* node = matchedNode("MATCH (WHERE true) RETURN 1");
    EXPECT_EQ(node->getSymbol(), nullptr);
    EXPECT_NE(node->getWhere(), nullptr);
}

TEST_F(KeywordNameDecisionTest, whereFirstInANodeBeforeABraceNamesTheNode) {
    const NodePattern* node = matchedNode("MATCH (where {a: 1}) RETURN where");
    ASSERT_NE(node->getSymbol(), nullptr);
    EXPECT_EQ(node->getSymbol()->getName(), "where");
    EXPECT_NE(node->getProperties(), nullptr);
}

TEST_F(KeywordNameDecisionTest, isAfterAParenthesisBeforeALabelOpensAPredicate) {
    const Expr* predicate = returnedItem("RETURN (IS Person)");
    ASSERT_EQ(predicate->getKind(), Expr::Kind::EXISTS);
    EXPECT_EQ(labelOf(predicateRoot(predicate)->getLabelExpr()), "Person");
}

TEST_F(KeywordNameDecisionTest, isAfterAParenthesisBeforeAnInfixWordIsAVariable) {
    EXPECT_EQ(returnedItem("RETURN (is AND true)")->getKind(), Expr::Kind::LOGICAL);
}

TEST_F(KeywordNameDecisionTest, isFirstInANodeBeforeANameOpensItsLabels) {
    const NodePattern* node = matchedNode("MATCH (IS AND) RETURN 1");
    EXPECT_EQ(node->getSymbol(), nullptr);
    EXPECT_EQ(labelOf(node->getLabelExpr()), "AND");
}

TEST_F(KeywordNameDecisionTest, isFirstInANodeBeforeAColonNamesTheNode) {
    const NodePattern* node = matchedNode("MATCH (is:Person) RETURN is");
    ASSERT_NE(node->getSymbol(), nullptr);
    EXPECT_EQ(node->getSymbol()->getName(), "is");
    EXPECT_EQ(labelOf(node->getLabelExpr()), "Person");
}
