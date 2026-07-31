#include <gtest/gtest.h>

#include <stdint.h>

#include <string_view>
#include <vector>

#include "CypherAST.h"
#include "FunctionInvocation.h"
#include "Literal.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "expr/BinaryExpr.h"
#include "expr/ConstantExpressionDetector.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/ListExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/Operators.h"
#include "expr/PropertyExpr.h"
#include "expr/StringExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"

using namespace db;

// The detector answers whether an expression holds the same value in every row, which
// is what lets a caller evaluate it once or drop it where only its variation matters.
// The trees here are built through the AST factories rather than parsed, so each test
// states exactly the shape it asks about.
class ConstantExpressionDetectorTest : public ::testing::Test {
protected:
    ConstantExpressionDetectorTest()
        : _ast(nullptr, "")
    {
    }

    Expr* integer(int64_t value) {
        return LiteralExpr::create(&_ast, IntegerLiteral::create(&_ast, value));
    }

    Expr* string(std::string_view value) {
        return LiteralExpr::create(&_ast, StringLiteral::create(&_ast, value));
    }

    Expr* add(Expr* lhs, Expr* rhs) {
        return BinaryExpr::create(&_ast, BinaryOperator::Add, lhs, rhs);
    }

    Expr* list(const std::vector<Expr*>& elements) {
        ListExpr* expr = ListExpr::create(&_ast);
        for (Expr* element : elements) {
            expr->addItem(element);
        }

        return expr;
    }

    Expr* listLiteral(const std::vector<Expr*>& items) {
        ListLiteral* literal = ListLiteral::create(&_ast);
        for (Expr* item : items) {
            literal->addItem(item);
        }

        return LiteralExpr::create(&_ast, literal);
    }

    // The keys are irrelevant to the answer, so each value is given one of its own,
    // named after the position it sits at
    Expr* mapLiteral(const std::vector<Expr*>& values) {
        const std::vector<std::string_view> keyNames = {"a", "b", "c"};

        MapLiteral* literal = MapLiteral::create(&_ast);
        for (size_t index = 0; index < values.size(); index++) {
            literal->set(Symbol::create(&_ast, keyNames[index]), values[index]);
        }

        return LiteralExpr::create(&_ast, literal);
    }

    Expr* symbol(std::string_view name) {
        return SymbolExpr::create(&_ast, Symbol::create(&_ast, name));
    }

    Expr* property(std::string_view name) {
        QualifiedName* fullName = QualifiedName::create(&_ast);
        fullName->addName(Symbol::create(&_ast, name));

        PropertyExpr* expr = PropertyExpr::create(&_ast, fullName);
        expr->setPropertyName(name);

        return expr;
    }

    Expr* call(std::string_view name, const std::vector<Expr*>& arguments) {
        QualifiedName* fullName = QualifiedName::create(&_ast);
        fullName->addName(Symbol::create(&_ast, name));

        ExprChain* chain = ExprChain::create(&_ast);
        for (Expr* argument : arguments) {
            chain->add(argument);
        }

        FunctionInvocation* invocation = FunctionInvocation::create(&_ast, fullName);
        invocation->setArguments(chain);

        return FunctionInvocationExpr::create(&_ast, invocation);
    }

    CypherAST _ast;
};

// Every literal written in the query text is one value, whatever its type.
TEST_F(ConstantExpressionDetectorTest, literalsAreConstant) {
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(integer(1)));
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(string("x")));
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(
        LiteralExpr::create(&_ast, BoolLiteral::create(&_ast, true))));
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(
        LiteralExpr::create(&_ast, DoubleLiteral::create(&_ast, 1.5))));
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(
        LiteralExpr::create(&_ast, NullLiteral::create(&_ast))));
}

// The * of COUNT(*) stands for the rows themselves, so it is not a value the way the
// other literals are.
TEST_F(ConstantExpressionDetectorTest, wildcardIsNotConstant) {
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(
        LiteralExpr::create(&_ast, WildcardLiteral::create(&_ast))));
}

// A variable and a property both read the row they are evaluated on.
TEST_F(ConstantExpressionDetectorTest, variablesAndPropertiesAreNotConstant) {
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(symbol("n")));
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(property("age")));
}

// An operator is as constant as what it is applied to, all the way down.
TEST_F(ConstantExpressionDetectorTest, operatorsFollowTheirOperands) {
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(add(integer(1), integer(2))));
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(add(add(integer(1), integer(2)), integer(3))));
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(
        UnaryExpr::create(&_ast, UnaryOperator::Minus, integer(3))));

    EXPECT_FALSE(ConstantExpressionDetector::isConstant(add(integer(1), property("age"))));
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(add(add(integer(1), symbol("n")), integer(3))));
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(
        UnaryExpr::create(&_ast, UnaryOperator::Minus, property("age"))));
}

TEST_F(ConstantExpressionDetectorTest, stringOperatorsFollowTheirOperands) {
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(
        StringExpr::create(&_ast, StringOperator::StartsWith, string("abc"), string("a"))));

    EXPECT_FALSE(ConstantExpressionDetector::isConstant(
        StringExpr::create(&_ast, StringOperator::StartsWith, property("name"), string("a"))));
}

// A list is written element by element, and one varying element is enough to make the
// whole of it vary.
TEST_F(ConstantExpressionDetectorTest, listsFollowTheirElements) {
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(list({integer(1), integer(2)})));
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(list({})));
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(list({integer(1), property("age")})));

    EXPECT_TRUE(ConstantExpressionDetector::isConstant(listLiteral({integer(1), integer(2)})));
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(listLiteral({integer(1), symbol("n")})));
}

// The keys of a map are symbols written in the query, so only its values can make it
// vary.
TEST_F(ConstantExpressionDetectorTest, mapsFollowTheirValues) {
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(mapLiteral({integer(1), string("x")})));
    EXPECT_TRUE(ConstantExpressionDetector::isConstant(mapLiteral({})));
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(mapLiteral({integer(1), property("age")})));
}

// A call is never reported as constant, not even over constant arguments: the detector
// knows nothing of what the function does, and one may aggregate over the rows or
// answer differently on every call.
TEST_F(ConstantExpressionDetectorTest, callsAreNotConstant) {
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(call("abs", {integer(-1)})));
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(call("rand", {})));
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(call("count", {symbol("n")})));
}

TEST_F(ConstantExpressionDetectorTest, noExpressionIsNotConstant) {
    EXPECT_FALSE(ConstantExpressionDetector::isConstant(nullptr));
}
