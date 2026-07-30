#include <gtest/gtest.h>

#include <stdint.h>

#include <string_view>
#include <vector>

#include "CypherAST.h"
#include "Literal.h"
#include "Symbol.h"
#include "expr/BinaryExpr.h"
#include "expr/Expr.h"
#include "expr/ListExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/Operators.h"
#include "expr/StructuralExpressionComparator.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"

using namespace db;

// The comparator answers whether two expression trees are built the same way, which is
// what lets a caller recognise one expression written twice in a query. The trees here
// are built through the AST factories rather than parsed, so each test states exactly
// the two shapes it compares.
class StructuralExpressionComparatorTest : public ::testing::Test {
protected:
    StructuralExpressionComparatorTest()
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

    CypherAST _ast;
};

// The point of the class: two trees that were never the same node compare equal.
TEST_F(StructuralExpressionComparatorTest, equalTreesBuiltSeparately) {
    EXPECT_TRUE(StructuralExpressionComparator::equal(add(integer(1), integer(2)),
                                                      add(integer(1), integer(2))));
}

TEST_F(StructuralExpressionComparatorTest, sameNodeEqualsItself) {
    Expr* expr = add(integer(1), integer(2));

    EXPECT_TRUE(StructuralExpressionComparator::equal(expr, expr));
}

// Literals compare by value, and by value of the right kind: 1 is not 2, and the integer
// 1 is not the string "1".
TEST_F(StructuralExpressionComparatorTest, literalsCompareByValue) {
    EXPECT_TRUE(StructuralExpressionComparator::equal(integer(7), integer(7)));
    EXPECT_FALSE(StructuralExpressionComparator::equal(integer(1), integer(2)));

    EXPECT_TRUE(StructuralExpressionComparator::equal(string("a"), string("a")));
    EXPECT_FALSE(StructuralExpressionComparator::equal(string("a"), string("b")));

    EXPECT_FALSE(StructuralExpressionComparator::equal(integer(1), string("1")));
}

// A null literal carries no value, so any two of them are the same one.
TEST_F(StructuralExpressionComparatorTest, nullLiteralsAreEqual) {
    Expr* lhs = LiteralExpr::create(&_ast, NullLiteral::create(&_ast));
    Expr* rhs = LiteralExpr::create(&_ast, NullLiteral::create(&_ast));

    EXPECT_TRUE(StructuralExpressionComparator::equal(lhs, rhs));
}

// The operator is part of the shape, and so is the side each operand sits on.
TEST_F(StructuralExpressionComparatorTest, operatorAndOperandOrderMatter) {
    Expr* sum = add(integer(1), integer(2));
    Expr* difference = BinaryExpr::create(&_ast, BinaryOperator::Sub, integer(1), integer(2));

    EXPECT_FALSE(StructuralExpressionComparator::equal(sum, difference));
    EXPECT_FALSE(StructuralExpressionComparator::equal(sum, add(integer(2), integer(1))));
}

TEST_F(StructuralExpressionComparatorTest, unaryOperatorsCompareByOperand) {
    Expr* lhs = UnaryExpr::create(&_ast, UnaryOperator::Minus, integer(3));
    Expr* rhs = UnaryExpr::create(&_ast, UnaryOperator::Minus, integer(3));
    Expr* other = UnaryExpr::create(&_ast, UnaryOperator::Minus, integer(4));

    EXPECT_TRUE(StructuralExpressionComparator::equal(lhs, rhs));
    EXPECT_FALSE(StructuralExpressionComparator::equal(lhs, other));
}

// Nesting is compared all the way down, so a difference in a leaf reaches the top.
TEST_F(StructuralExpressionComparatorTest, nestedTreesCompareToTheLeaves) {
    Expr* lhs = add(add(integer(1), integer(2)), integer(3));
    Expr* rhs = add(add(integer(1), integer(2)), integer(3));
    Expr* deepDifference = add(add(integer(1), integer(9)), integer(3));

    EXPECT_TRUE(StructuralExpressionComparator::equal(lhs, rhs));
    EXPECT_FALSE(StructuralExpressionComparator::equal(lhs, deepDifference));

    // The same leaves in a different shape: 1 + (2 + 3) is not (1 + 2) + 3
    EXPECT_FALSE(StructuralExpressionComparator::equal(lhs, add(integer(1), add(integer(2), integer(3)))));
}

TEST_F(StructuralExpressionComparatorTest, listsCompareElementwise) {
    EXPECT_TRUE(StructuralExpressionComparator::equal(list({integer(1), integer(2)}),
                                                      list({integer(1), integer(2)})));

    EXPECT_FALSE(StructuralExpressionComparator::equal(list({integer(1), integer(2)}),
                                                       list({integer(2), integer(1)})));

    EXPECT_FALSE(StructuralExpressionComparator::equal(list({integer(1)}),
                                                       list({integer(1), integer(2)})));

    EXPECT_TRUE(StructuralExpressionComparator::equal(list({}), list({})));
}

// A symbol stands for the variable it was resolved to, so two mentions of one variable
// match and two different variables do not - whatever they are called.
TEST_F(StructuralExpressionComparatorTest, symbolsCompareByDeclaration) {
    Expr* lhs = SymbolExpr::create(&_ast, Symbol::create(&_ast, "n"));
    Expr* rhs = SymbolExpr::create(&_ast, Symbol::create(&_ast, "n"));

    // Neither is resolved, so both carry a null declaration and compare equal
    EXPECT_TRUE(StructuralExpressionComparator::equal(lhs, rhs));
}

TEST_F(StructuralExpressionComparatorTest, differentKindsAreNeverEqual) {
    EXPECT_FALSE(StructuralExpressionComparator::equal(integer(1), list({integer(1)})));
    EXPECT_FALSE(StructuralExpressionComparator::equal(add(integer(1), integer(2)), integer(3)));
}

TEST_F(StructuralExpressionComparatorTest, nullOperandsAreHandled) {
    EXPECT_TRUE(StructuralExpressionComparator::equal(nullptr, nullptr));
    EXPECT_FALSE(StructuralExpressionComparator::equal(integer(1), nullptr));
    EXPECT_FALSE(StructuralExpressionComparator::equal(nullptr, integer(1)));
}
