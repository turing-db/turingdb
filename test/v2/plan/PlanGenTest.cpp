#include <gtest/gtest.h>

#include <iostream>

#include "Graph.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "columns/Block.h"
#include "SystemManager.h"
#include "SimpleGraph.h"
#include "CypherParser.h"
#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "PlanGraphGenerator.h"
#include "PlanGraphDebug.h"
#include "PlanGraphTester.h"

#include "TuringConfig.h"

using namespace db;
using namespace db::v2;

class PlanGenTest : public ::testing::Test {
public:
    PlanGenTest()
        : _sysMan(_config)
    {
    }

    void SetUp() override {
        _graph = _sysMan.createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }

    void TearDown() override {
    }

protected:
    TuringConfig _config;
    SystemManager _sysMan;
    Graph* _graph {nullptr};
};

TEST_F(PlanGenTest, matchAllNodes) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    const std::string queryStr = "MATCH (n) RETURN n";

    CypherAST ast(queryStr);
    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(queryStr));

    CypherAnalyzer analyzer(&ast, view);
    ASSERT_NO_THROW(analyzer.analyze());

    PlanGraphGenerator planGen(ast, view, callback);
    planGen.generate(ast.queries().front());
    const PlanGraph& planGraph = planGen.getPlanGraph();

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES)
        .expect(PlanGraphOpcode::FILTER_NODE)
        .expectVar("n")
        .expect(PlanGraphOpcode::PRODUCE_RESULTS)
        .validateComplete();
}

TEST_F(PlanGenTest, matchAllEdgesWithVar) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    const std::string queryStr = "MATCH (n)-[e]->(m) RETURN n,e,m";

    CypherAST ast(queryStr);
    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(queryStr));

    CypherAnalyzer analyzer(&ast, view);
    ASSERT_NO_THROW(analyzer.analyze());

    PlanGraphGenerator planGen(ast, view, callback);
    planGen.generate(ast.queries().front());
    const PlanGraph& planGraph = planGen.getPlanGraph();

    PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES)
        .expect(PlanGraphOpcode::FILTER_NODE)
        .expectVar("n")
        .expect(PlanGraphOpcode::GET_OUT_EDGES)
        .expect(PlanGraphOpcode::FILTER_EDGE)
        .expectVar("e")
        .expect(PlanGraphOpcode::GET_EDGE_TARGET)
        .expect(PlanGraphOpcode::FILTER_NODE)
        .expectVar("m")
        .expect(PlanGraphOpcode::PRODUCE_RESULTS)
        .validateComplete();
}

TEST_F(PlanGenTest, matchAllEdges2) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    const std::string queryStr = "MATCH (n)-->(m) RETURN n,m";

    CypherAST ast(queryStr);
    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(queryStr));

    CypherAnalyzer analyzer(&ast, view);
    ASSERT_NO_THROW(analyzer.analyze());

    PlanGraphGenerator planGen(ast, view, callback);
    planGen.generate(ast.queries().front());
    const PlanGraph& planGraph = planGen.getPlanGraph();

    PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES)
        .expect(PlanGraphOpcode::FILTER_NODE)
        .expectVar("n")
        .expect(PlanGraphOpcode::GET_OUT_EDGES)
        .expect(PlanGraphOpcode::FILTER_EDGE)
        .expectVar("v0")
        .expect(PlanGraphOpcode::GET_EDGE_TARGET)
        .expect(PlanGraphOpcode::FILTER_NODE)
        .expectVar("m")
        .expect(PlanGraphOpcode::PRODUCE_RESULTS)
        .validateComplete();
}

TEST_F(PlanGenTest, matchSingleByLabel) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    const std::string queryStr = "MATCH (n:Person) RETURN n";

    CypherAST ast(queryStr);
    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(queryStr));

    CypherAnalyzer analyzer(&ast, view);
    ASSERT_NO_THROW(analyzer.analyze());

    PlanGraphGenerator planGen(ast, view, callback);
    planGen.generate(ast.queries().front());
    const PlanGraph& planGraph = planGen.getPlanGraph();

    PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    // TODO, Find ways to better test this
    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES)
        .expect(PlanGraphOpcode::FILTER_NODE)
        .expectVar("n")
        .expect(PlanGraphOpcode::PRODUCE_RESULTS)
        .validateComplete();
}

TEST_F(PlanGenTest, matchLinear1) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    const std::string queryStr = "MATCH (n:Person)-[e:KNOWS_WELL]->(p:Person) RETURN n,p";

    CypherAST ast(queryStr);
    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(queryStr));

    CypherAnalyzer analyzer(&ast, view);
    ASSERT_NO_THROW(analyzer.analyze());

    PlanGraphGenerator planGen(ast, view, callback);
    planGen.generate(ast.queries().front());
    const PlanGraph& planGraph = planGen.getPlanGraph();

    PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES)
        .expect(PlanGraphOpcode::FILTER_NODE)
        .expectVar("n")
        .expect(PlanGraphOpcode::GET_OUT_EDGES)
        .expect(PlanGraphOpcode::FILTER_EDGE)
        .expectVar("e")
        .expect(PlanGraphOpcode::GET_EDGE_TARGET)
        .expect(PlanGraphOpcode::FILTER_NODE)
        .expectVar("p")
        .expect(PlanGraphOpcode::PRODUCE_RESULTS)
        .validateComplete();
}

TEST_F(PlanGenTest, matchExprConstraint1) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    const std::string queryStr = "MATCH (n:Person)-[e:KNOWS_WELL {isFrench: true}]->(p:Person) RETURN n,p";
    CypherAST ast(queryStr);
    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(queryStr));

    CypherAnalyzer analyzer(&ast, view);
    ASSERT_NO_THROW(analyzer.analyze());

    PlanGraphGenerator planGen(ast, view, callback);
    planGen.generate(ast.queries().front());
    const PlanGraph& planGraph = planGen.getPlanGraph();

    PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES)
        .expect(PlanGraphOpcode::FILTER_NODE)
        .expectVar("n")
        .expect(PlanGraphOpcode::GET_OUT_EDGES)
        .expect(PlanGraphOpcode::FILTER_EDGE)
        .expectVar("e")
        .expect(PlanGraphOpcode::GET_EDGE_TARGET)
        .expect(PlanGraphOpcode::FILTER_NODE)
        .expectVar("p")
        .expect(PlanGraphOpcode::PRODUCE_RESULTS)
        .validateComplete();
}

TEST_F(PlanGenTest, matchExprConstraint2) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    const std::string queryStr = "MATCH (n:Person {isFrench: true, hasPhD: true})-[e:KNOWS_WELL]->(p:Person {isFrench: false}) RETURN n,p";
    CypherAST ast(queryStr);
    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(queryStr));

    CypherAnalyzer analyzer(&ast, view);
    ASSERT_NO_THROW(analyzer.analyze());

    PlanGraphGenerator planGen(ast, view, callback);
    planGen.generate(ast.queries().front());

    //planGraph.dump(std::cout);

    /*
    PlanOptimizer optimizer(planGraph);
    optimizer.optimize();

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES_BY_LABEL)
        .expect(PlanGraphOpcode::FILTER_NODE_EXPR)
        .expect(PlanGraphOpcode::FILTER_NODE_EXPR)
        .expectVar("n")
        .expect(PlanGraphOpcode::GET_OUT_EDGES)
        .expect(PlanGraphOpcode::FILTER_EDGE_TYPE)
        .expectVar("e")
        .expect(PlanGraphOpcode::GET_EDGE_TARGET)
        .expect(PlanGraphOpcode::FILTER_NODE_LABEL)
        .expect(PlanGraphOpcode::FILTER_NODE_EXPR)
        .expectVar("p")
        .validateComplete();
    */
}

TEST_F(PlanGenTest, matchMultiTargetsLinear) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    const std::string queryStr = "MATCH (n:Person)-->(m), (m{isFrench:true})-->(z) RETURN n,z";

    CypherAST ast(queryStr);
    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(queryStr));

    CypherAnalyzer analyzer(&ast, view);
    ASSERT_NO_THROW(analyzer.analyze());

    PlanGraphGenerator planGen(ast, view, callback);
    planGen.generate(ast.queries().front());
    const PlanGraph& planGraph = planGen.getPlanGraph();

    PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);
}

TEST_F(PlanGenTest, matchMultiTargets1) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    const std::string queryStr = "MATCH (a)-->(b)-->(c), (b{isFrench:true})-->(z), (b)-->(y), (z)-->(n)-->(y) RETURN n,z";
    
    CypherAST ast(queryStr);
    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(queryStr));

    CypherAnalyzer analyzer(&ast, view);
    ASSERT_NO_THROW(analyzer.analyze());

    PlanGraphGenerator planGen(ast, view, callback);
    planGen.generate(ast.queries().front());
    const PlanGraph& planGraph = planGen.getPlanGraph();

    PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);
}
