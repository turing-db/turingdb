#include <gtest/gtest.h>

#include <iostream>

#include "ASTContext.h"
#include "QueryParser.h"
#include "Graph.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "columns/Block.h"
#include "SystemManager.h"
#include "SimpleGraph.h"
#include "PlanGraphGenerator.h"
#include "QueryAnalyzer.h"
#include "PlanGraphTester.h"

using namespace db;

class PlanGenTest : public ::testing::Test {
    void SetUp() override {
        _graph = _sysMan.createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }

    void TearDown() override {
    }

protected:
    SystemManager _sysMan;
    Graph* _graph {nullptr};
};

TEST_F(PlanGenTest, matchAllNodes) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const std::string queryStr = "MATCH n RETURN n";
    QueryCommand* queryCmd = parser.parse(queryStr);
    ASSERT_TRUE(queryCmd);

    QueryAnalyzer analyzer(view, &ctxt);
    const bool anaRes = analyzer.analyze(queryCmd);
    ASSERT_TRUE(anaRes);

    PlanGraphGenerator planGen(view, callback);
    planGen.generate(queryCmd);
    const PlanGraph& planGraph = planGen.getPlanGraph();

    planGraph.dump(std::cout);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES)
        .expectVar("n")
        .validateComplete();
}

TEST_F(PlanGenTest, matchAllEdgesWithVar) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const std::string queryStr = "MATCH (n)-[e]-(m) RETURN n,e,m";
    QueryCommand* queryCmd = parser.parse(queryStr);
    ASSERT_TRUE(queryCmd);

    QueryAnalyzer analyzer(view, &ctxt);
    const bool anaRes = analyzer.analyze(queryCmd);
    ASSERT_TRUE(anaRes);

    PlanGraphGenerator planGen(view, callback);
    planGen.generate(queryCmd);
    const PlanGraph& planGraph = planGen.getPlanGraph();

    planGraph.dump(std::cout);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES)
        .expectVar("n")
        .expect(PlanGraphOpcode::GET_OUT_EDGES)
        .expectVar("e")
        .expect(PlanGraphOpcode::GET_EDGE_TARGET)
        .expectVar("m")
        .validateComplete();
}

TEST_F(PlanGenTest, matchAllEdges2) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const std::string queryStr = "MATCH (n)--(m) RETURN n,m";
    QueryCommand* queryCmd = parser.parse(queryStr);
    ASSERT_TRUE(queryCmd);

    QueryAnalyzer analyzer(view, &ctxt);
    const bool anaRes = analyzer.analyze(queryCmd);
    ASSERT_TRUE(anaRes);

    PlanGraphGenerator planGen(view, callback);
    planGen.generate(queryCmd);
    const PlanGraph& planGraph = planGen.getPlanGraph();

    planGraph.dump(std::cout);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES)
        .expectVar("n")
        .expect(PlanGraphOpcode::GET_OUT_EDGES)
        .expectVar("0")
        .expect(PlanGraphOpcode::GET_EDGE_TARGET)
        .expectVar("m")
        .validateComplete();
}

TEST_F(PlanGenTest, matchSingleByLabel) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const std::string queryStr = "MATCH (n:Person) RETURN n";
    QueryCommand* queryCmd = parser.parse(queryStr);
    ASSERT_TRUE(queryCmd);

    QueryAnalyzer analyzer(view, &ctxt);
    const bool anaRes = analyzer.analyze(queryCmd);
    ASSERT_TRUE(anaRes);

    PlanGraphGenerator planGen(view, callback);
    planGen.generate(queryCmd);
    const PlanGraph& planGraph = planGen.getPlanGraph();

    planGraph.dump(std::cout);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES_BY_LABEL)
        .expectVar("n")
        .validateComplete();
}

TEST_F(PlanGenTest, matchLinear1) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const std::string queryStr = "MATCH (n:Person)-[e:KNOWS_WELL]-(p:Person) RETURN n,p";
    QueryCommand* queryCmd = parser.parse(queryStr);
    ASSERT_TRUE(queryCmd);

    QueryAnalyzer analyzer(view, &ctxt);
    const bool anaRes = analyzer.analyze(queryCmd);
    ASSERT_TRUE(anaRes);

    PlanGraphGenerator planGen(view, callback);
    planGen.generate(queryCmd);
    const PlanGraph& planGraph = planGen.getPlanGraph();

    planGraph.dump(std::cout);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES_BY_LABEL)
        .expectVar("n")
        .expect(PlanGraphOpcode::GET_OUT_EDGES)
        .expect(PlanGraphOpcode::FILTER_EDGE_TYPE)
        .expectVar("e")
        .expect(PlanGraphOpcode::GET_EDGE_TARGET)
        .expect(PlanGraphOpcode::FILTER_NODE_LABEL)
        .expectVar("p")
        .validateComplete();
}

TEST_F(PlanGenTest, matchExprConstraint1) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const std::string queryStr = "MATCH (n:Person)-[e:KNOWS_WELL {isFrench: true}]-(p:Person) RETURN n,p";
    QueryCommand* queryCmd = parser.parse(queryStr);
    ASSERT_TRUE(queryCmd);

    QueryAnalyzer analyzer(view, &ctxt);
    const bool anaRes = analyzer.analyze(queryCmd);
    ASSERT_TRUE(anaRes);

    PlanGraphGenerator planGen(view, callback);
    planGen.generate(queryCmd);
    const PlanGraph& planGraph = planGen.getPlanGraph();

    planGraph.dump(std::cout);

    std::vector<PlanGraphNode*> roots;
    planGraph.getRoots(roots);

    PlanGraphTester(roots.front())
        .expect(PlanGraphOpcode::SCAN_NODES_BY_LABEL)
        .expectVar("n")
        .expect(PlanGraphOpcode::GET_OUT_EDGES)
        .expect(PlanGraphOpcode::FILTER_EDGE_TYPE)
        .expect(PlanGraphOpcode::FILTER_EDGE_EXPR)
        .expectVar("e")
        .expect(PlanGraphOpcode::GET_EDGE_TARGET)
        .expect(PlanGraphOpcode::FILTER_NODE_LABEL)
        .expectVar("p")
        .validateComplete();
}

TEST_F(PlanGenTest, matchExprConstraint2) {
    const Transaction transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const std::string queryStr = "MATCH (n:Person {isFrench: true, hasPhD: true})-[e:KNOWS_WELL]-(p:Person {isFrench: false}) RETURN n,p";
    QueryCommand* queryCmd = parser.parse(queryStr);
    ASSERT_TRUE(queryCmd);

    QueryAnalyzer analyzer(view, &ctxt);
    const bool anaRes = analyzer.analyze(queryCmd);
    ASSERT_TRUE(anaRes);

    PlanGraphGenerator planGen(view, callback);
    planGen.generate(queryCmd);
    const PlanGraph& planGraph = planGen.getPlanGraph();

    planGraph.dump(std::cout);

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
}
