#include "AnalyzeException.h"
#include "ExprAnalyzer.h"
#include "TuringTest.h"

#include "Literal.h"
#include "CypherAST.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "decl/DeclContext.h"
#include "expr/All.h"
#include "procedures/ProcedureBlueprintMap.h"
#include "versioning/Transaction.h"

using namespace db;

class ExpressionTest : public turing::test::TuringTest {
public:
    ExpressionTest()
        : _procedures(ProcedureBlueprintMap::create()),
        _ast(*_procedures, "")
    {
    }

    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        auto tx = _graph->openTransaction();
        auto view = tx.viewGraph();

        _declContext = DeclContext::create(&_ast, nullptr);
        _analyzer = std::make_unique<ExprAnalyzer>(&_ast, view);
        _analyzer->setDeclContext(_declContext);
    }

protected:
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ExprAnalyzer> _analyzer;
    std::unique_ptr<ProcedureBlueprintMap> _procedures;
    DeclContext* _declContext {nullptr};

    CypherAST _ast;
};

TEST_F(ExpressionTest, LiteralExpressionTest) {
    LiteralExpr* boolLiteral = LiteralExpr::create(&_ast, BoolLiteral::create(&_ast, true));
    LiteralExpr* intLiteral = LiteralExpr::create(&_ast, IntegerLiteral::create(&_ast, 5));
    LiteralExpr* doubleLiteral = LiteralExpr::create(&_ast, DoubleLiteral::create(&_ast, 5.3));
    LiteralExpr* stringLiteral = LiteralExpr::create(&_ast, StringLiteral::create(&_ast, "test"));
    LiteralExpr* charLiteral = LiteralExpr::create(&_ast, CharLiteral::create(&_ast, 'c'));

    ASSERT_TRUE(boolLiteral->getLiteral()->getKind() == Literal::Kind::BOOL);
    ASSERT_TRUE(intLiteral->getLiteral()->getKind() == Literal::Kind::INTEGER);
    ASSERT_TRUE(doubleLiteral->getLiteral()->getKind() == Literal::Kind::DOUBLE);
    ASSERT_TRUE(stringLiteral->getLiteral()->getKind() == Literal::Kind::STRING);
    ASSERT_TRUE(charLiteral->getLiteral()->getKind() == Literal::Kind::CHAR);

    _analyzer->analyzeRootExpr(boolLiteral);
    _analyzer->analyzeRootExpr(intLiteral);
    _analyzer->analyzeRootExpr(doubleLiteral);
    _analyzer->analyzeRootExpr(stringLiteral);
    _analyzer->analyzeRootExpr(charLiteral);

    EXPECT_EQ(boolLiteral->getType(), EvaluatedType::Bool);
    EXPECT_EQ(intLiteral->getType(), EvaluatedType::Integer);
    EXPECT_EQ(doubleLiteral->getType(), EvaluatedType::Double);
    EXPECT_EQ(stringLiteral->getType(), EvaluatedType::String);
    EXPECT_EQ(charLiteral->getType(), EvaluatedType::Char);
}

#define EXPECT_BINARY_VALID(a, op, b, eval)                         \
    {                                                               \
        LiteralExpr* lhs = LiteralExpr::create(&_ast, a);           \
        LiteralExpr* rhs = LiteralExpr::create(&_ast, b);           \
        BinaryExpr* expr = BinaryExpr::create(&_ast, op, lhs, rhs); \
        EXPECT_NO_THROW(_analyzer->analyzeRootExpr(expr));          \
        EXPECT_EQ(expr->getType(), eval);                           \
    }

#define EXPECT_BINARY_INVALID(a, op, b)                                   \
    {                                                                     \
        LiteralExpr* lhs = LiteralExpr::create(&_ast, a);                 \
        LiteralExpr* rhs = LiteralExpr::create(&_ast, b);                 \
        BinaryExpr* expr = BinaryExpr::create(&_ast, op, lhs, rhs);       \
        EXPECT_THROW(_analyzer->analyzeRootExpr(expr), AnalyzeException); \
    }

TEST_F(ExpressionTest, BinaryExpressionTest) {
    // Type pairs
    {
        LiteralExpr* lhs = LiteralExpr::create(&_ast, StringLiteral::create(&_ast, "test"));
        LiteralExpr* rhs = LiteralExpr::create(&_ast, BoolLiteral::create(&_ast, true));
        _analyzer->analyzeRootExpr(lhs);
        _analyzer->analyzeRootExpr(rhs);
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
    EXPECT_BINARY_VALID(trueLiteral, BinaryOperator::Or, trueLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(falseLiteral, BinaryOperator::Or, trueLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(trueLiteral, BinaryOperator::Xor, falseLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(falseLiteral, BinaryOperator::And, falseLiteral, EvaluatedType::Bool);
    /// Invalid
    IntegerLiteral* fiveLiteral = IntegerLiteral::create(&_ast, 5);
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::Or, fiveLiteral);
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::Or, trueLiteral);
    EXPECT_BINARY_INVALID(StringLiteral::create(&_ast, "test"), BinaryOperator::Or, fiveLiteral);
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.5), BinaryOperator::Xor, trueLiteral);
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::And, trueLiteral);

    // NotEqual - Equal (Equality comparisons)
    /// Valid
    IntegerLiteral* tenLiteral = IntegerLiteral::create(&_ast, 10);
    IntegerLiteral* fourtyTwoLiteral = IntegerLiteral::create(&_ast, 42);
    StringLiteral* helloLiteral = StringLiteral::create(&_ast, "hello");
    StringLiteral* sameLiteral = StringLiteral::create(&_ast, "same");
    StringLiteral* stringLiteral = StringLiteral::create(&_ast, "string");
    StringLiteral* xLiteral = StringLiteral::create(&_ast, "x");
    CharLiteral* aLiteral = CharLiteral::create(&_ast, 'a');
    EXPECT_BINARY_VALID(trueLiteral, BinaryOperator::NotEqual, trueLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(falseLiteral, BinaryOperator::Equal, trueLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(fiveLiteral, BinaryOperator::NotEqual, tenLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(fourtyTwoLiteral, BinaryOperator::Equal, fourtyTwoLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(StringLiteral::create(&_ast, "test"), BinaryOperator::NotEqual, helloLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(sameLiteral, BinaryOperator::Equal, sameLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(stringLiteral, BinaryOperator::NotEqual, aLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(xLiteral, BinaryOperator::Equal, xLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(aLiteral, BinaryOperator::NotEqual, aLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(CharLiteral::create(&_ast, 'z'), BinaryOperator::Equal, CharLiteral::create(&_ast, 'z'), EvaluatedType::Bool);
    /// Invalid
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::NotEqual, fiveLiteral);
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::Equal, trueLiteral);
    EXPECT_BINARY_INVALID(StringLiteral::create(&_ast, "test"), BinaryOperator::NotEqual, fiveLiteral);
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.3), BinaryOperator::Equal, fiveLiteral);
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.3), BinaryOperator::NotEqual, DoubleLiteral::create(&_ast, 5.7));
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::Equal, StringLiteral::create(&_ast, "true"));
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::NotEqual, fiveLiteral);
    EXPECT_BINARY_INVALID(fourtyTwoLiteral, BinaryOperator::Equal, CharLiteral::create(&_ast, '4'));

    // LessThan - GreaterThan - LessThanOrEqual - GreaterThanOrEqual (Numeric comparisons)
    /// Valid
    IntegerLiteral* fifteenLiteral = IntegerLiteral::create(&_ast, 15);
    IntegerLiteral* threeLiteral = IntegerLiteral::create(&_ast, 3);
    IntegerLiteral* sevenLiteral = IntegerLiteral::create(&_ast, 7);
    EXPECT_BINARY_VALID(fiveLiteral, BinaryOperator::LessThan, tenLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(fifteenLiteral, BinaryOperator::GreaterThan, threeLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(fiveLiteral, BinaryOperator::LessThanOrEqual, fiveLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(sevenLiteral, BinaryOperator::GreaterThanOrEqual, sevenLiteral, EvaluatedType::Bool);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 5.5), BinaryOperator::LessThan, DoubleLiteral::create(&_ast, 10.2), EvaluatedType::Bool);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 15.7), BinaryOperator::GreaterThan, DoubleLiteral::create(&_ast, 3.14), EvaluatedType::Bool);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 5.0), BinaryOperator::LessThanOrEqual, DoubleLiteral::create(&_ast, 5.0), EvaluatedType::Bool);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 7.1), BinaryOperator::GreaterThanOrEqual, DoubleLiteral::create(&_ast, 7.1), EvaluatedType::Bool);
    EXPECT_BINARY_VALID(fiveLiteral, BinaryOperator::LessThan, DoubleLiteral::create(&_ast, 10.5), EvaluatedType::Bool);
    EXPECT_BINARY_VALID(fifteenLiteral, BinaryOperator::GreaterThan, DoubleLiteral::create(&_ast, 3.14), EvaluatedType::Bool);
    EXPECT_BINARY_VALID(fiveLiteral, BinaryOperator::LessThanOrEqual, DoubleLiteral::create(&_ast, 5.0), EvaluatedType::Bool);
    EXPECT_BINARY_VALID(sevenLiteral, BinaryOperator::GreaterThanOrEqual, DoubleLiteral::create(&_ast, 6.9), EvaluatedType::Bool);
    /// Invalid
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::LessThan, fiveLiteral);
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::GreaterThan, trueLiteral);
    EXPECT_BINARY_INVALID(StringLiteral::create(&_ast, "test"), BinaryOperator::LessThanOrEqual, fiveLiteral);
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::GreaterThanOrEqual, fiveLiteral);
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.5), BinaryOperator::LessThan, StringLiteral::create(&_ast, "10"));
    EXPECT_BINARY_INVALID(StringLiteral::create(&_ast, "5"), BinaryOperator::GreaterThan, DoubleLiteral::create(&_ast, 3.14));
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::LessThanOrEqual, falseLiteral);

    // Add - Sub - Mult - Div - Mod - Pow (Arithmetic operations)
    /// Valid
    IntegerLiteral* fourLiteral = IntegerLiteral::create(&_ast, 4);
    IntegerLiteral* twentyLiteral = IntegerLiteral::create(&_ast, 20);
    IntegerLiteral* seventeenLiteral = IntegerLiteral::create(&_ast, 17);
    IntegerLiteral* twoLiteral = IntegerLiteral::create(&_ast, 2);
    IntegerLiteral* eightLiteral = IntegerLiteral::create(&_ast, 8);
    EXPECT_BINARY_VALID(fiveLiteral, BinaryOperator::Add, tenLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_VALID(fifteenLiteral, BinaryOperator::Sub, threeLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_VALID(fourLiteral, BinaryOperator::Mult, sevenLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_VALID(twentyLiteral, BinaryOperator::Div, fourLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_VALID(seventeenLiteral, BinaryOperator::Mod, fiveLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_VALID(twoLiteral, BinaryOperator::Pow, eightLiteral, EvaluatedType::Integer);
    EXPECT_BINARY_VALID(fiveLiteral, BinaryOperator::Add, DoubleLiteral::create(&_ast, 10.2), EvaluatedType::Double);
    EXPECT_BINARY_VALID(fifteenLiteral, BinaryOperator::Sub, DoubleLiteral::create(&_ast, 3.14), EvaluatedType::Double);
    EXPECT_BINARY_VALID(fourLiteral, BinaryOperator::Mult, DoubleLiteral::create(&_ast, 7.5), EvaluatedType::Double);
    EXPECT_BINARY_VALID(twentyLiteral, BinaryOperator::Div, DoubleLiteral::create(&_ast, 4.1), EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 15.7), BinaryOperator::Sub, DoubleLiteral::create(&_ast, 3.14), EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 4.0), BinaryOperator::Mult, DoubleLiteral::create(&_ast, 7.5), EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 20.4), BinaryOperator::Div, DoubleLiteral::create(&_ast, 4.1), EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 17.8), BinaryOperator::Mod, DoubleLiteral::create(&_ast, 5.2), EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 2.5), BinaryOperator::Pow, DoubleLiteral::create(&_ast, 3.0), EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 5.5), BinaryOperator::Add, tenLiteral, EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 15.7), BinaryOperator::Sub, IntegerLiteral::create(&_ast, 3), EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 4.0), BinaryOperator::Mult, sevenLiteral, EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 20.4), BinaryOperator::Div, fourLiteral, EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 17.8), BinaryOperator::Mod, fiveLiteral, EvaluatedType::Double);
    EXPECT_BINARY_VALID(DoubleLiteral::create(&_ast, 2.5), BinaryOperator::Pow, threeLiteral, EvaluatedType::Double);
    /// Invalid
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::Add, fiveLiteral);
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::Sub, trueLiteral);
    EXPECT_BINARY_INVALID(StringLiteral::create(&_ast, "test"), BinaryOperator::Mult, fiveLiteral);
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::Div, fiveLiteral);
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::Mod, StringLiteral::create(&_ast, "10"));
    EXPECT_BINARY_INVALID(StringLiteral::create(&_ast, "2"), BinaryOperator::Pow, eightLiteral);
    EXPECT_BINARY_INVALID(trueLiteral, BinaryOperator::Add, DoubleLiteral::create(&_ast, 5.5));
    EXPECT_BINARY_INVALID(DoubleLiteral::create(&_ast, 5.5), BinaryOperator::Sub, trueLiteral);
    EXPECT_BINARY_INVALID(StringLiteral::create(&_ast, "test"), BinaryOperator::Mult, DoubleLiteral::create(&_ast, 5.5));
    EXPECT_BINARY_INVALID(CharLiteral::create(&_ast, 'c'), BinaryOperator::Div, DoubleLiteral::create(&_ast, 5.5));

    // In operator (membership testing)
    /// Valid - assuming you have list and map literals
    // EXPECT_BINARY_VALID(Literal {5}, BinaryOperator::In, ListLiteral {/* some list */});
    // EXPECT_BINARY_VALID(Literal {"key"}, BinaryOperator::In, MapLiteral {/* some map */});
    // EXPECT_BINARY_VALID(Literal {true}, BinaryOperator::In, ListLiteral {/* some list */});
    // EXPECT_BINARY_VALID(Literal {'c'}, BinaryOperator::In, ListLiteral {/* some list */});
    // EXPECT_BINARY_VALID(Literal {5.5}, BinaryOperator::In, MapLiteral {/* some map */});
    /// Invalid
    EXPECT_BINARY_INVALID(fiveLiteral, BinaryOperator::In, tenLiteral);
    EXPECT_BINARY_INVALID(StringLiteral::create(&_ast, "key"), BinaryOperator::In, StringLiteral::create(&_ast, "string"));
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
