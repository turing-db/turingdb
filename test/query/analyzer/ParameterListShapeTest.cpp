#include <gtest/gtest.h>

#include <stddef.h>
#include <memory>
#include <string_view>

#include "TuringTest.h"

#include "CypherAST.h"
#include "ExprAnalyzer.h"
#include "Graph.h"
#include "Parameter.h"
#include "ParameterMap.h"
#include "ParameterValue.h"
#include "ProcedureManager.h"
#include "SimpleGraph.h"
#include "decl/DeclContext.h"
#include "decl/ListShape.h"
#include "expr/All.h"
#include "versioning/Transaction.h"

using namespace db;

class ParameterListShapeTest : public turing::test::TuringTest {
public:
    ParameterListShapeTest()
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
    // The shape the analyzer gives $name
    ListShape shapeOf(std::string_view name) {
        ParameterExpr* expr = ParameterExpr::create(&_ast, Parameter::create(&_ast, name));
        _analyzer->analyzeRootExpr(expr);

        return expr->getListShape();
    }

    void expectShape(std::string_view name, EvaluatedType leafType, size_t depth) {
        const ListShape shape = shapeOf(name);
        EXPECT_EQ(shape.getLeafType(), leafType) << name;
        EXPECT_EQ(shape.getDepth(), depth) << name;
    }

    ParameterMap _parameters;
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ExprAnalyzer> _analyzer;
    std::unique_ptr<ProcedureManager> _procedures;
    DeclContext* _declContext {nullptr};
    CypherAST _ast;
};

TEST_F(ParameterListShapeTest, aListOfOneTypeIsShapedOverThatType) {
    ParameterValue integers;
    ParameterValue::List& elements = integers.setList();
    elements.emplace_back().setInt64(1);
    elements.emplace_back().setInt64(2);
    _parameters.set("ints", integers);

    ParameterValue withANull;
    ParameterValue::List& mixed = withANull.setList();
    mixed.emplace_back().setInt64(1);
    mixed.emplace_back().setNull();
    _parameters.set("intsAndNull", withANull);

    ParameterValue maps;
    maps.setList().emplace_back().setMap()["a"].setInt64(1);
    _parameters.set("maps", maps);

    expectShape("ints", EvaluatedType::Integer, 1);
    expectShape("intsAndNull", EvaluatedType::Integer, 1);
    expectShape("maps", EvaluatedType::Map, 1);
}

TEST_F(ParameterListShapeTest, aListOfTwoTypesIsShapedOverTaggedScalars) {
    ParameterValue mixed;
    ParameterValue::List& elements = mixed.setList();
    elements.emplace_back().setInt64(1);
    elements.emplace_back().setString("a");
    _parameters.set("mixed", mixed);

    expectShape("mixed", EvaluatedType::Invalid, 1);
}

TEST_F(ParameterListShapeTest, anEmptyListNamesNoType) {
    ParameterValue empty;
    empty.setList();
    _parameters.set("empty", empty);

    ParameterValue nulls;
    nulls.setList().emplace_back().setNull();
    _parameters.set("nulls", nulls);

    expectShape("empty", EvaluatedType::Null, 1);
    expectShape("nulls", EvaluatedType::Null, 1);
}

TEST_F(ParameterListShapeTest, aListOfListsIsOneLevelDeeper) {
    ParameterValue rows;
    ParameterValue::List& outer = rows.setList();
    ParameterValue::List& first = outer.emplace_back().setList();
    first.emplace_back().setInt64(1);
    first.emplace_back().setInt64(2);
    outer.emplace_back().setList().emplace_back().setInt64(3);
    _parameters.set("rows", rows);

    ParameterValue ragged;
    ParameterValue::List& uneven = ragged.setList();
    uneven.emplace_back().setList().emplace_back().setInt64(1);
    uneven.emplace_back().setInt64(2);
    _parameters.set("ragged", ragged);

    expectShape("rows", EvaluatedType::Integer, 2);
    expectShape("ragged", EvaluatedType::Invalid, 1);
}

TEST_F(ParameterListShapeTest, aDeeplyNestedListIsShapedWithoutRecursion) {
    constexpr size_t DEPTH = 100'000;

    ParameterValue deep;
    ParameterValue::List* level = &deep.setList();
    for (size_t nesting = 1; nesting < DEPTH; nesting++) {
        level = &level->emplace_back().setList();
    }
    _parameters.set("deep", deep);

    expectShape("deep", EvaluatedType::Null, DEPTH);
}
