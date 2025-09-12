#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "CypherAnalyzer.h"
#include "expr/All.h"
#include "expr/Literal.h"
#include "versioning/Transaction.h"

using namespace db;
using namespace db::v2;

class ExpressionTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        auto tx = _graph->openTransaction();
        auto view = tx.viewGraph();

        _analyzer = std::make_unique<CypherAnalyzer>(&_ast, view);
    }

protected:
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<CypherAnalyzer> _analyzer;

    CypherAST _ast {""};
};

TEST_F(ExpressionTest, LiteralExpressionTest) {
    LiteralExpr* boolLiteral = LiteralExpr::create(&_ast, BoolLiteral::create(&_ast, true));
    LiteralExpr* intLiteral = LiteralExpr::create(&_ast, IntegerLiteral::create(&_ast, 5));
    LiteralExpr* doubleLiteral = LiteralExpr::create(&_ast, DoubleLiteral::create(&_ast, 5.3));
    LiteralExpr* stringLiteral = LiteralExpr::create(&_ast, db::v2::StringLiteral::create(&_ast, "test"));
    LiteralExpr* charLiteral = LiteralExpr::create(&_ast, CharLiteral::create(&_ast, 'c'));

    ASSERT_TRUE(boolLiteral->getLiteral()->getKind() == Literal::Kind::BOOL);
    ASSERT_TRUE(intLiteral->getLiteral()->getKind() == Literal::Kind::INTEGER);
    ASSERT_TRUE(doubleLiteral->getLiteral()->getKind() == Literal::Kind::DOUBLE);
    ASSERT_TRUE(stringLiteral->getLiteral()->getKind() == Literal::Kind::STRING);
    ASSERT_TRUE(charLiteral->getLiteral()->getKind() == Literal::Kind::CHAR);

    _analyzer->analyze(boolLiteral);
    _analyzer->analyze(intLiteral);
    _analyzer->analyze(doubleLiteral);
    _analyzer->analyze(stringLiteral);
    _analyzer->analyze(charLiteral);

    EXPECT_EQ(boolLiteral->getType(), EvaluatedType::Bool);
    EXPECT_EQ(intLiteral->getType(), EvaluatedType::Integer);
    EXPECT_EQ(doubleLiteral->getType(), EvaluatedType::Double);
    EXPECT_EQ(stringLiteral->getType(), EvaluatedType::String);
    EXPECT_EQ(charLiteral->getType(), EvaluatedType::Char);
}

#define EXPECT_BINARY_ISVALID(a, op, b, eval)                       \
    {                                                               \
        LiteralExpr* lhs = LiteralExpr::create(&_ast, a);           \
        LiteralExpr* rhs = LiteralExpr::create(&_ast, b);           \
        BinaryExpr* expr = BinaryExpr::create(&_ast, op, lhs, rhs); \
        EXPECT_NO_THROW(_analyzer->analyze(expr));                  \
        EXPECT_EQ(expr->getType(), eval);                           \
    }

#define EXPECT_BINARY_INVALID(a, op, b)                             \
    {                                                               \
        LiteralExpr* lhs = LiteralExpr::create(&_ast, a);           \
        LiteralExpr* rhs = LiteralExpr::create(&_ast, b);           \
        BinaryExpr* expr = BinaryExpr::create(&_ast, op, lhs, rhs); \
        EXPECT_THROW(_analyzer->analyze(expr), AnalyzeException);   \
    }

TEST_F(ExpressionTest, BinaryExpressionTest) {
    // Type pairs
    {
        LiteralExpr* lhs = LiteralExpr::create(&_ast, db::v2::StringLiteral::create(&_ast, "test"));
        LiteralExpr* rhs = LiteralExpr::create(&_ast, BoolLiteral::create(&_ast, true));
        _analyzer->analyze(lhs);
        _analyzer->analyze(rhs);
        TypePairBitset pair {lhs->getType(), rhs->getType()};
        ASSERT_EQ(pair, TypePairBitset(EvaluatedType::String, EvaluatedType::Bool));
        ASSERT_EQ(pair, TypePairBitset(EvaluatedType::Bool, EvaluatedType::String));
        ASSERT_NE(pair, TypePairBitset(EvaluatedType::Bool, EvaluatedType::Integer));
        ASSERT_NE(pair, TypePairBitset(EvaluatedType::String, EvaluatedType::Integer));
        ASSERT_NE(pair, TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool));
    }


    // Or - Xor - And (Boolean operations - only bool + bool allowed)
    /// Valid
    BoolLiteral* trueLiteral = BoolLiteral::create(&_ast, true);
    BoolLiteral* falseLiteral = BoolLiteral::create(&_ast, false);
    EXPECT_BINARY_ISVALID(trueLiteral, BinaryOperator::Or, trueLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(falseLiteral, BinaryOperator::Or, trueLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(trueLiteral, BinaryOperator::Xor, falseLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(falseLiteral, BinaryOperator::And, falseLiteral, EvaluatedType::Bool);
    /// Invalid
    IntegerLiteral* fiveLiteral = IntegerLiteral::create(&_ast, 5);
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::Or, fiveLiteral);
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::Or, trueLiteral);
    EXPECT_BINARY_INVALID(db::v2::StringLiteral::create(&_ast, "test"), BinaryOperator::Or, fiveLiteral);
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.5), BinaryOperator::Xor, trueLiteral);
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::And, trueLiteral);

    // NotEqual - Equal (Equality comparisons)
    /// Valid
    IntegerLiteral* tenLiteral = IntegerLiteral::create(&_ast, 10);
    IntegerLiteral* fourtyTwoLiteral = IntegerLiteral::create(&_ast, 42);
    db::v2::StringLiteral* helloLiteral = db::v2::StringLiteral::create(&_ast, "hello");
    db::v2::StringLiteral* sameLiteral = db::v2::StringLiteral::create(&_ast, "same");
    db::v2::StringLiteral* stringLiteral = db::v2::StringLiteral::create(&_ast, "string");
    db::v2::StringLiteral* xLiteral = db::v2::StringLiteral::create(&_ast, "x");
    CharLiteral* aLiteral = db::v2::CharLiteral::create(&_ast, 'a');
    EXPECT_BINARY_ISVALID(trueLiteral, BinaryOperator::NotEqual, trueLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(falseLiteral, BinaryOperator::Equal, trueLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(fiveLiteral, BinaryOperator::NotEqual, tenLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(fourtyTwoLiteral, BinaryOperator::Equal, fourtyTwoLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(db::v2::StringLiteral::create(&_ast, "test"), BinaryOperator::NotEqual, helloLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(sameLiteral, BinaryOperator::Equal, sameLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(stringLiteral, BinaryOperator::NotEqual, aLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(xLiteral, BinaryOperator::Equal, xLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(aLiteral, BinaryOperator::NotEqual, aLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(CharLiteral::create(&_ast, 'z'), BinaryOperator::Equal, CharLiteral::create(&_ast, 'z'), EvaluatedType::Bool);
    /// Invalid
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::NotEqual, fiveLiteral);
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::Equal, trueLiteral);
    EXPECT_BINARY_INVALID(db::v2::StringLiteral::create(&_ast, "test"), BinaryOperator::NotEqual, fiveLiteral);
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.3), BinaryOperator::Equal, fiveLiteral);
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.3), BinaryOperator::NotEqual, DoubleLiteral::create(&_ast, 5.7));
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::Equal, db::v2::StringLiteral::create(&_ast, "true"));
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::NotEqual, fiveLiteral);
    EXPECT_BINARY_INVALID(fourtyTwoLiteral, BinaryOperator::Equal, CharLiteral::create(&_ast, '4'));

    // LessThan - GreaterThan - LessThanOrEqual - GreaterThanOrEqual (Numeric comparisons)
    /// Valid
    IntegerLiteral* fifteenLiteral = IntegerLiteral::create(&_ast, 15);
    IntegerLiteral* threeLiteral = IntegerLiteral::create(&_ast, 3);
    IntegerLiteral* sevenLiteral = IntegerLiteral::create(&_ast, 7);
    EXPECT_BINARY_ISVALID(fiveLiteral, BinaryOperator::LessThan, tenLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(fifteenLiteral, BinaryOperator::GreaterThan, threeLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(fiveLiteral, BinaryOperator::LessThanOrEqual, fiveLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(sevenLiteral, BinaryOperator::GreaterThanOrEqual, sevenLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 5.5), BinaryOperator::LessThan, DoubleLiteral::create(&_ast, 10.2), EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 15.7), BinaryOperator::GreaterThan, DoubleLiteral::create(&_ast, 3.14), EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 5.0), BinaryOperator::LessThanOrEqual, DoubleLiteral::create(&_ast, 5.0), EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 7.1), BinaryOperator::GreaterThanOrEqual, DoubleLiteral::create(&_ast, 7.1), EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(fiveLiteral, BinaryOperator::LessThan, DoubleLiteral::create(&_ast, 10.5), EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(fifteenLiteral, BinaryOperator::GreaterThan, DoubleLiteral::create(&_ast, 3.14), EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(fiveLiteral, BinaryOperator::LessThanOrEqual, DoubleLiteral::create(&_ast, 5.0), EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(sevenLiteral, BinaryOperator::GreaterThanOrEqual, DoubleLiteral::create(&_ast, 6.9), EvaluatedType::Bool);
    /// Invalid
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::LessThan, fiveLiteral);
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::GreaterThan, trueLiteral);
    EXPECT_BINARY_INVALID(db::v2::StringLiteral::create(&_ast, "test"), BinaryOperator::LessThanOrEqual, fiveLiteral);
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::GreaterThanOrEqual, fiveLiteral);
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.5), BinaryOperator::LessThan, db::v2::StringLiteral::create(&_ast, "10"));
    EXPECT_BINARY_INVALID(db::v2::StringLiteral::create(&_ast, "5"), BinaryOperator::GreaterThan, DoubleLiteral::create(&_ast, 3.14));
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::LessThanOrEqual, falseLiteral);

    // Add - Sub - Mult - Div - Mod - Pow (Arithmetic operations)
    /// Valid
    IntegerLiteral* fourLiteral = IntegerLiteral::create(&_ast, 4);
    IntegerLiteral* twentyLiteral = IntegerLiteral::create(&_ast, 20);
    IntegerLiteral* seventeenLiteral = IntegerLiteral::create(&_ast, 17);
    IntegerLiteral* twoLiteral = IntegerLiteral::create(&_ast, 2);
    IntegerLiteral* eightLiteral = IntegerLiteral::create(&_ast, 8);
    EXPECT_BINARY_ISVALID(fiveLiteral, BinaryOperator::Add, tenLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(fifteenLiteral, BinaryOperator::Sub, threeLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(fourLiteral, BinaryOperator::Mult, sevenLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(twentyLiteral, BinaryOperator::Div, fourLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(seventeenLiteral, BinaryOperator::Mod, fiveLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(twoLiteral, BinaryOperator::Pow, eightLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(fiveLiteral, BinaryOperator::Add, DoubleLiteral::create(&_ast, 10.2), EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(fifteenLiteral, BinaryOperator::Sub, DoubleLiteral::create(&_ast, 3.14), EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(fourLiteral, BinaryOperator::Mult, DoubleLiteral::create(&_ast, 7.5), EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(twentyLiteral, BinaryOperator::Div, DoubleLiteral::create(&_ast, 4.1), EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 15.7), BinaryOperator::Sub, DoubleLiteral::create(&_ast, 3.14), EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 4.0), BinaryOperator::Mult, DoubleLiteral::create(&_ast, 7.5), EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 20.4), BinaryOperator::Div, DoubleLiteral::create(&_ast, 4.1), EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 17.8), BinaryOperator::Mod, DoubleLiteral::create(&_ast, 5.2), EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 2.5), BinaryOperator::Pow, DoubleLiteral::create(&_ast, 3.0), EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 5.5), BinaryOperator::Add, tenLiteral, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 15.7), BinaryOperator::Sub, IntegerLiteral::create(&_ast, 3), EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 4.0), BinaryOperator::Mult, sevenLiteral, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 20.4), BinaryOperator::Div, fourLiteral, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 17.8), BinaryOperator::Mod, fiveLiteral, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(DoubleLiteral::create(&_ast, 2.5), BinaryOperator::Pow, threeLiteral, EvaluatedType::Double);
    /// Invalid
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::Add, fiveLiteral);
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::Sub, trueLiteral);
    EXPECT_BINARY_INVALID(db::v2::StringLiteral::create(&_ast, "test"), BinaryOperator::Mult, fiveLiteral);
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::Div, fiveLiteral);
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::Mod, db::v2::StringLiteral::create(&_ast, "10"));
    EXPECT_BINARY_INVALID(db::v2::StringLiteral::create(&_ast, "2"), BinaryOperator::Pow, eightLiteral);
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::Add, DoubleLiteral::create(&_ast, 5.5));
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.5), BinaryOperator::Sub, trueLiteral);
    EXPECT_BINARY_INVALID(db::v2::StringLiteral::create(&_ast, "test"), BinaryOperator::Mult, DoubleLiteral::create(&_ast, 5.5));
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::Div, DoubleLiteral::create(&_ast, 5.5));

    // In operator (membership testing)
    /// Valid - assuming you have list and map literals
    // EXPECT_BINARY_ISVALID(Literal {5}, BinaryOperator::In, ListLiteral {/* some list */});
    // EXPECT_BINARY_ISVALID(Literal {"key"}, BinaryOperator::In, MapLiteral {/* some map */});
    // EXPECT_BINARY_ISVALID(Literal {true}, BinaryOperator::In, ListLiteral {/* some list */});
    // EXPECT_BINARY_ISVALID(Literal {'c'}, BinaryOperator::In, ListLiteral {/* some list */});
    // EXPECT_BINARY_ISVALID(Literal {5.5}, BinaryOperator::In, MapLiteral {/* some map */});
    /// Invalid
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::In, tenLiteral);
    EXPECT_BINARY_INVALID(db::v2::StringLiteral::create(&_ast, "key"), BinaryOperator::In, db::v2::StringLiteral::create(&_ast, "string"));
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::In, DoubleLiteral::create(&_ast, 5.5));
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::In, trueLiteral);
    // EXPECT_BINARY_INVALID(ListLiteral {/* some list */}, BinaryOperator::In, ListLiteral {/* another list */});
    // EXPECT_BINARY_INVALID(MapLiteral {/* some map */}, BinaryOperator::In, MapLiteral {/* another map */});
    // EXPECT_BINARY_INVALID(ListLiteral {/* some list */}, BinaryOperator::In, Literal {5});
    // EXPECT_BINARY_INVALID(MapLiteral {/* some map */}, BinaryOperator::In, Literal {"test"});
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.3), BinaryOperator::Equal, DoubleLiteral::create(&_ast, 5.3));
}


int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 20;
    });
}
