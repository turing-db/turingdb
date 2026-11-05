#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "TuringTest.h"

#include "AnalyzeException.h"
#include "CypherAST.h"
#include "ExprAnalyzer.h"
#include "Graph.h"
#include "Literal.h"
#include "Parameter.h"
#include "ParameterMap.h"
#include "ParameterValue.h"
#include "ProcedureManager.h"
#include "SimpleGraph.h"
#include "decl/DeclContext.h"
#include "decl/ParameterDecl.h"
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
        _analyzer = std::make_unique<ExprAnalyzer>(&_ast, transaction.viewGraph(), &_parameters);
        _analyzer->setDeclContext(_declContext);
    }

protected:
    ParameterExpr* parameter(std::string_view name) {
        return ParameterExpr::create(&_ast, Parameter::create(&_ast, name));
    }

    void bindInt64(std::string_view name, int64_t value) {
        ParameterValue parameter;
        parameter.setInt64(value);
        _parameters.set(name, parameter);
    }

    void bindDouble(std::string_view name, double value) {
        ParameterValue parameter;
        parameter.setDouble(value);
        _parameters.set(name, parameter);
    }

    void bindString(std::string_view name, std::string_view value) {
        ParameterValue parameter;
        parameter.setString(value);
        _parameters.set(name, parameter);
    }

    void bindNull(std::string_view name) {
        ParameterValue parameter;
        parameter.setNull();
        _parameters.set(name, parameter);
    }

    ParameterMap _parameters;
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ExprAnalyzer> _analyzer;
    std::unique_ptr<ProcedureManager> _procedures;
    DeclContext* _declContext {nullptr};

    CypherAST _ast;
};

TEST_F(ParameterTypeTest, aParameterTakesTheTypeOfItsValue) {
    bindInt64("i", 1);
    bindDouble("d", 1.5);
    bindString("s", "a");

    ParameterValue boolean;
    boolean.setBool(true);
    _parameters.set("b", boolean);

    ParameterValue list;
    list.setList().emplace_back().setInt64(1);
    _parameters.set("l", list);

    ParameterValue map;
    map.setMap();
    _parameters.set("m", map);

    ParameterValue wait;
    wait.setDuration(Duration {3'600'000'000});
    _parameters.set("wait", wait);

    ParameterValue when;
    when.setDateTime(DateTime {1'762'351'500'000'000});
    _parameters.set("when", when);

    const std::pair<std::string_view, EvaluatedType> expectations[] = {
        {"i", EvaluatedType::Integer},
        {"d", EvaluatedType::Double},
        {"s", EvaluatedType::String},
        {"b", EvaluatedType::Bool},
        {"l", EvaluatedType::List},
        {"m", EvaluatedType::Map},
        {"wait", EvaluatedType::Duration},
        {"when", EvaluatedType::DateTime},
    };

    for (const auto& [name, expected] : expectations) {
        ParameterExpr* p = parameter(name);
        _analyzer->analyzeRootExpr(p);

        EXPECT_EQ(p->getType(), expected) << name;
        EXPECT_NE(p->getExprVarDecl(), nullptr) << name;
    }
}

TEST_F(ParameterTypeTest, aNullIsTypedNull) {
    bindNull("p");

    ParameterExpr* p = parameter("p");
    _analyzer->analyzeRootExpr(p);

    EXPECT_EQ(p->getType(), EvaluatedType::Null);
}

TEST_F(ParameterTypeTest, anExpressionOverAParameterIsTypedByItsValue) {
    bindDouble("p", 1.5);

    Expr* one = LiteralExpr::create(&_ast, IntegerLiteral::create(&_ast, 1));
    BinaryExpr* difference = BinaryExpr::create(&_ast, BinaryOperator::Sub, parameter("p"), one);
    _analyzer->analyzeRootExpr(difference);

    EXPECT_EQ(difference->getType(), EvaluatedType::Double);
}

TEST_F(ParameterTypeTest, anUnboundParameterIsRejected) {
    try {
        _analyzer->analyzeRootExpr(parameter("p"));
        ADD_FAILURE() << "an unbound parameter was accepted";
    } catch (const AnalyzeException& error) {
        EXPECT_NE(std::string(error.what()).find("Parameter $p is not defined"), std::string::npos) << error.what();
    }
}

TEST_F(ParameterTypeTest, theOccurrencesOfAParameterShareOneDecl) {
    bindInt64("p", 1);
    bindString("q", "a");

    ParameterExpr* first = parameter("p");
    ParameterExpr* second = parameter("p");
    ParameterExpr* other = parameter("q");

    _analyzer->analyzeRootExpr(first);
    _analyzer->analyzeRootExpr(second);
    _analyzer->analyzeRootExpr(other);

    ASSERT_NE(first->getDecl(), nullptr);
    EXPECT_EQ(first->getDecl(), second->getDecl());
    EXPECT_NE(first->getDecl(), other->getDecl());
    EXPECT_EQ(first->getDecl()->getType(), EvaluatedType::Integer);
    EXPECT_EQ(other->getDecl()->getType(), EvaluatedType::String);
    EXPECT_EQ(_ast.parameterDecls().size(), size_t {2});
}
