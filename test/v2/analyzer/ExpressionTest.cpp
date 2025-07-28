#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "CypherAnalyzer.h"
#include "expressions/All.h"
#include "versioning/Transaction.h"

using namespace db;

class ExpressionTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        auto tx = _graph->openTransaction();
        auto view = tx.viewGraph();

        _analyzer = std::make_unique<CypherAnalyzer>(_ast, view);
    }

protected:
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<CypherAnalyzer> _analyzer;

    CypherAST _ast {""};
};

TEST_F(ExpressionTest, LiteralExpressionTest) {
    LiteralExpression boolLiteral {Literal(true)};
    LiteralExpression intLiteral {Literal(5)};
    LiteralExpression doubleLiteral {Literal(5.3)};
    LiteralExpression stringLiteral {Literal("test")};
    LiteralExpression charLiteral {Literal('c')};

    ASSERT_TRUE(boolLiteral.literal().is<bool>());
    ASSERT_TRUE(intLiteral.literal().is<int64_t>());
    ASSERT_TRUE(doubleLiteral.literal().is<double>());
    ASSERT_TRUE(stringLiteral.literal().is<std::string_view>());
    ASSERT_TRUE(charLiteral.literal().is<char>());

    _analyzer->analyze(boolLiteral);
    _analyzer->analyze(intLiteral);
    _analyzer->analyze(doubleLiteral);
    _analyzer->analyze(stringLiteral);
    _analyzer->analyze(charLiteral);

    EXPECT_EQ(boolLiteral.type(), EvaluatedType::Bool);
    EXPECT_EQ(intLiteral.type(), EvaluatedType::Integer);
    EXPECT_EQ(doubleLiteral.type(), EvaluatedType::Double);
    EXPECT_EQ(stringLiteral.type(), EvaluatedType::String);
    EXPECT_EQ(charLiteral.type(), EvaluatedType::Char);
}

#define EXPECT_BINARY_ISVALID(a, op, b, eval)      \
    {                                              \
        LiteralExpression lhs {a};                 \
        LiteralExpression rhs {b};                 \
        BinaryExpression expr {&lhs, op, &rhs};    \
        EXPECT_NO_THROW(_analyzer->analyze(expr)); \
        EXPECT_EQ(expr.type(), eval);              \
    }

#define EXPECT_BINARY_INVALID(a, op, b)                           \
    {                                                             \
        LiteralExpression lhs {a};                                \
        LiteralExpression rhs {b};                                \
        BinaryExpression expr {&lhs, op, &rhs};                   \
        EXPECT_THROW(_analyzer->analyze(expr), AnalyzeException); \
    }

TEST_F(ExpressionTest, BinaryExpressionTest) {
    // Type pairs
    {
        LiteralExpression lhs {Literal("test")};
        LiteralExpression rhs {Literal(true)};
        _analyzer->analyze(lhs);
        _analyzer->analyze(rhs);
        TypePairBitset pair {lhs.type(), rhs.type()};
        ASSERT_EQ(pair, TypePairBitset(EvaluatedType::String, EvaluatedType::Bool));
        ASSERT_EQ(pair, TypePairBitset(EvaluatedType::Bool, EvaluatedType::String));
        ASSERT_NE(pair, TypePairBitset(EvaluatedType::Bool, EvaluatedType::Integer));
        ASSERT_NE(pair, TypePairBitset(EvaluatedType::String, EvaluatedType::Integer));
        ASSERT_NE(pair, TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool));
    }


    // Or - Xor - And (Boolean operations - only bool + bool allowed)
    /// Valid
    EXPECT_BINARY_ISVALID(Literal {true}, BinaryOperator::Or, Literal {true}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {false}, BinaryOperator::Or, Literal {true}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {true}, BinaryOperator::Xor, Literal {false}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {false}, BinaryOperator::And, Literal {false}, EvaluatedType::Bool);
    /// Invalid
    EXPECT_BINARY_INVALID(Literal {true}, BinaryOperator::Or, Literal {5});
    EXPECT_BINARY_INVALID(Literal {5}, BinaryOperator::Or, Literal {true});
    EXPECT_BINARY_INVALID(Literal {"test"}, BinaryOperator::Or, Literal {5});
    EXPECT_BINARY_INVALID(Literal {5.5}, BinaryOperator::Xor, Literal {true});
    EXPECT_BINARY_INVALID(Literal {'c'}, BinaryOperator::And, Literal {true});

    // NotEqual - Equal (Equality comparisons)
    /// Valid
    EXPECT_BINARY_ISVALID(Literal {true}, BinaryOperator::NotEqual, Literal {true}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {false}, BinaryOperator::Equal, Literal {true}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {5}, BinaryOperator::NotEqual, Literal {10}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {42}, BinaryOperator::Equal, Literal {42}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {"test"}, BinaryOperator::NotEqual, Literal {"hello"}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {"same"}, BinaryOperator::Equal, Literal {"same"}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {"string"}, BinaryOperator::NotEqual, Literal {'c'}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {"x"}, BinaryOperator::Equal, Literal {'x'}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {'a'}, BinaryOperator::NotEqual, Literal {'b'}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {'z'}, BinaryOperator::Equal, Literal {'z'}, EvaluatedType::Bool);
    /// Invalid
    EXPECT_BINARY_INVALID(Literal {true}, BinaryOperator::NotEqual, Literal {5});
    EXPECT_BINARY_INVALID(Literal {5}, BinaryOperator::Equal, Literal {true});
    EXPECT_BINARY_INVALID(Literal {"test"}, BinaryOperator::NotEqual, Literal {5});
    EXPECT_BINARY_INVALID(Literal {5.3}, BinaryOperator::Equal, Literal {5});
    EXPECT_BINARY_INVALID(Literal {5.3}, BinaryOperator::NotEqual, Literal {5.7});
    EXPECT_BINARY_INVALID(Literal {true}, BinaryOperator::Equal, Literal {"true"});
    EXPECT_BINARY_INVALID(Literal {'c'}, BinaryOperator::NotEqual, Literal {5});
    EXPECT_BINARY_INVALID(Literal {42}, BinaryOperator::Equal, Literal {'4'});

    // LessThan - GreaterThan - LessThanOrEqual - GreaterThanOrEqual (Numeric comparisons)
    /// Valid
    EXPECT_BINARY_ISVALID(Literal {5}, BinaryOperator::LessThan, Literal {10}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {15}, BinaryOperator::GreaterThan, Literal {3}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {5}, BinaryOperator::LessThanOrEqual, Literal {5}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {7}, BinaryOperator::GreaterThanOrEqual, Literal {7}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {5.5}, BinaryOperator::LessThan, Literal {10.2}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {15.7}, BinaryOperator::GreaterThan, Literal {3.14}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {5.0}, BinaryOperator::LessThanOrEqual, Literal {5.0}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {7.1}, BinaryOperator::GreaterThanOrEqual, Literal {7.1}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {5}, BinaryOperator::LessThan, Literal {10.5}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {15}, BinaryOperator::GreaterThan, Literal {3.14}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {5}, BinaryOperator::LessThanOrEqual, Literal {5.0}, EvaluatedType::Bool);
    EXPECT_BINARY_ISVALID(Literal {7}, BinaryOperator::GreaterThanOrEqual, Literal {6.9}, EvaluatedType::Bool);
    /// Invalid
    EXPECT_BINARY_INVALID(Literal {true}, BinaryOperator::LessThan, Literal {5});
    EXPECT_BINARY_INVALID(Literal {5}, BinaryOperator::GreaterThan, Literal {true});
    EXPECT_BINARY_INVALID(Literal {"test"}, BinaryOperator::LessThanOrEqual, Literal {5});
    EXPECT_BINARY_INVALID(Literal {'c'}, BinaryOperator::GreaterThanOrEqual, Literal {5});
    EXPECT_BINARY_INVALID(Literal {5.5}, BinaryOperator::LessThan, Literal {"10"});
    EXPECT_BINARY_INVALID(Literal {"5"}, BinaryOperator::GreaterThan, Literal {3.14});
    EXPECT_BINARY_INVALID(Literal {true}, BinaryOperator::LessThanOrEqual, Literal {false});

    // Add - Sub - Mult - Div - Mod - Pow (Arithmetic operations)
    /// Valid
    EXPECT_BINARY_ISVALID(Literal {5}, BinaryOperator::Add, Literal {10}, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(Literal {15}, BinaryOperator::Sub, Literal {3}, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(Literal {4}, BinaryOperator::Mult, Literal {7}, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(Literal {20}, BinaryOperator::Div, Literal {4}, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(Literal {17}, BinaryOperator::Mod, Literal {5}, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(Literal {2}, BinaryOperator::Pow, Literal {8}, EvaluatedType::Integer);
    EXPECT_BINARY_ISVALID(Literal {5}, BinaryOperator::Add, Literal {10.2}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {15}, BinaryOperator::Sub, Literal {3.14}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {4}, BinaryOperator::Mult, Literal {7.5}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {20}, BinaryOperator::Div, Literal {4.1}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {15.7}, BinaryOperator::Sub, Literal {3.14}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {4.0}, BinaryOperator::Mult, Literal {7.5}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {20.4}, BinaryOperator::Div, Literal {4.1}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {17.8}, BinaryOperator::Mod, Literal {5.2}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {2.5}, BinaryOperator::Pow, Literal {3.0}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {5.5}, BinaryOperator::Add, Literal {10}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {15.7}, BinaryOperator::Sub, Literal {3}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {4.0}, BinaryOperator::Mult, Literal {7}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {20.4}, BinaryOperator::Div, Literal {4}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {17.8}, BinaryOperator::Mod, Literal {5}, EvaluatedType::Double);
    EXPECT_BINARY_ISVALID(Literal {2.5}, BinaryOperator::Pow, Literal {3}, EvaluatedType::Double);
    /// Invalid
    EXPECT_BINARY_INVALID(Literal {true}, BinaryOperator::Add, Literal {5});
    EXPECT_BINARY_INVALID(Literal {5}, BinaryOperator::Sub, Literal {true});
    EXPECT_BINARY_INVALID(Literal {"test"}, BinaryOperator::Mult, Literal {5});
    EXPECT_BINARY_INVALID(Literal {'c'}, BinaryOperator::Div, Literal {5});
    EXPECT_BINARY_INVALID(Literal {5}, BinaryOperator::Mod, Literal {"10"});
    EXPECT_BINARY_INVALID(Literal {"2"}, BinaryOperator::Pow, Literal {8});
    EXPECT_BINARY_INVALID(Literal {true}, BinaryOperator::Add, Literal {5.5});
    EXPECT_BINARY_INVALID(Literal {5.5}, BinaryOperator::Sub, Literal {true});
    EXPECT_BINARY_INVALID(Literal {"test"}, BinaryOperator::Mult, Literal {5.5});
    EXPECT_BINARY_INVALID(Literal {'c'}, BinaryOperator::Div, Literal {5.5});

    // In operator (membership testing)
    /// Valid - assuming you have list and map literals
    // EXPECT_BINARY_ISVALID(Literal {5}, BinaryOperator::In, ListLiteral {/* some list */});
    // EXPECT_BINARY_ISVALID(Literal {"key"}, BinaryOperator::In, MapLiteral {/* some map */});
    // EXPECT_BINARY_ISVALID(Literal {true}, BinaryOperator::In, ListLiteral {/* some list */});
    // EXPECT_BINARY_ISVALID(Literal {'c'}, BinaryOperator::In, ListLiteral {/* some list */});
    // EXPECT_BINARY_ISVALID(Literal {5.5}, BinaryOperator::In, MapLiteral {/* some map */});
    /// Invalid
    EXPECT_BINARY_INVALID(Literal {5}, BinaryOperator::In, Literal {10});
    EXPECT_BINARY_INVALID(Literal {"key"}, BinaryOperator::In, Literal {"string"});
    EXPECT_BINARY_INVALID(Literal {true}, BinaryOperator::In, Literal {5.5});
    EXPECT_BINARY_INVALID(Literal {'c'}, BinaryOperator::In, Literal {true});
    // EXPECT_BINARY_INVALID(ListLiteral {/* some list */}, BinaryOperator::In, ListLiteral {/* another list */});
    // EXPECT_BINARY_INVALID(MapLiteral {/* some map */}, BinaryOperator::In, MapLiteral {/* another map */});
    // EXPECT_BINARY_INVALID(ListLiteral {/* some list */}, BinaryOperator::In, Literal {5});
    // EXPECT_BINARY_INVALID(MapLiteral {/* some map */}, BinaryOperator::In, Literal {"test"});
    EXPECT_BINARY_INVALID(Literal {5.3}, BinaryOperator::Equal, Literal {5.3});
}


int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 20;
    });
}
