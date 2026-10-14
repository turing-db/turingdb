#include <gtest/gtest.h>

#include <memory>
#include <string_view>

#include "TuringTest.h"

#include "CypherAST.h"
#include "ExprAnalyzer.h"
#include "Graph.h"
#include "Literal.h"
#include "Parameter.h"
#include "ProcedureManager.h"
#include "SimpleGraph.h"
#include "decl/DeclContext.h"
#include "expr/All.h"
#include "versioning/Transaction.h"

using namespace db;

class ParameterTypeTest : public turing::test::TuringTest {
public:
    ParameterTypeTest()
        : _procedures(std::make_unique<ProcedureManager>()),
        _ast(_procedures.get(), "")
    {
    }

    void initialize() override {
        _procedures->init();
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        const FrozenCommitTx transaction = _graph->openTransaction();

        _declContext = DeclContext::create(&_ast, nullptr);
        _analyzer = std::make_unique<ExprAnalyzer>(&_ast, transaction.viewGraph());
        _analyzer->setDeclContext(_declContext);
    }

protected:
    ParameterExpr* parameter(std::string_view name) {
        return ParameterExpr::create(&_ast, Parameter::create(&_ast, name));
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ExprAnalyzer> _analyzer;
    std::unique_ptr<ProcedureManager> _procedures;
    DeclContext* _declContext {nullptr};

    CypherAST _ast;
};

TEST_F(ParameterTypeTest, aParameterIsTypedAsAParameter) {
    ParameterExpr* p = parameter("p");
    _analyzer->analyzeRootExpr(p);

    EXPECT_EQ(p->getType(), EvaluatedType::Parameter);
    EXPECT_EQ(p->getCandidateTypes(), PARAMETER_VALUE_TYPES);
    EXPECT_NE(p->getExprVarDecl(), nullptr);
}

TEST_F(ParameterTypeTest, anExpressionOverAParameterStoresItsCandidates) {
    Expr* one = LiteralExpr::create(&_ast, IntegerLiteral::create(&_ast, 1));
    Expr* two = LiteralExpr::create(&_ast, IntegerLiteral::create(&_ast, 2));
    BinaryExpr* inner = BinaryExpr::create(&_ast, BinaryOperator::Sub, parameter("p"), one);
    BinaryExpr* outer = BinaryExpr::create(&_ast, BinaryOperator::Mult, inner, two);
    _analyzer->analyzeRootExpr(outer);

    const EvaluatedTypeSet numbers {EvaluatedType::Integer, EvaluatedType::Double};

    EXPECT_EQ(inner->getType(), EvaluatedType::Parameter);
    EXPECT_EQ(inner->getCandidateTypes(), numbers);
    EXPECT_EQ(outer->getType(), EvaluatedType::Parameter);
    EXPECT_EQ(outer->getCandidateTypes(), numbers);
}

TEST_F(ParameterTypeTest, aParameterIsNarrowedByTheExpressionItIsAnOperandOf) {
    ParameterExpr* p = parameter("p");
    Expr* one = LiteralExpr::create(&_ast, IntegerLiteral::create(&_ast, 1));
    _analyzer->analyzeRootExpr(BinaryExpr::create(&_ast, BinaryOperator::Sub, p, one));

    EXPECT_EQ(p->getType(), EvaluatedType::Parameter);
    EXPECT_EQ(p->getCandidateTypes(), (EvaluatedTypeSet {EvaluatedType::Integer, EvaluatedType::Double}));
}
