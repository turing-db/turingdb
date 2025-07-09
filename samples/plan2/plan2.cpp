#include <stdlib.h>

#include <iostream>

#include "SystemManager.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "PlanGraphGenerator.h"
#include "PlanGraph.h"
#include "QueryAnalyzer.h"
#include "QueryParser.h"
#include "ASTContext.h"
#include "QueryCommand.h"
#include "ParserException.h"
#include "AnalyzeException.h"

using namespace db;

void runPlan2(const std::string& query);

int main(int argc, char** argv) {
    std::string queryStr = "match (n:Person{hasPhD: true})--(m:Person) return m";
    if (argc == 2) {
       queryStr = argv[1];
    }

    runPlan2(queryStr);

    return EXIT_SUCCESS;
}

void runPlan2(const std::string& query) {
    SystemManager sysMan;
    Graph* graph = sysMan.createGraph("simpledb");
    SimpleGraph::createSimpleGraph(graph);

    const Transaction transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    ASTContext ctxt;
    QueryParser parser(&ctxt);

    QueryCommand* queryCmd {nullptr};
    try {
        queryCmd = parser.parse(query);
        if (!queryCmd) {
            std::cerr << "Failed to parse query" << std::endl;
            return;
        }
    } catch (const ParserException& e) {
        std::cerr << "Syntax error: " << e.what() << std::endl;
        return;
    }

    QueryAnalyzer analyzer(view, &ctxt);
    try {
        analyzer.analyze(queryCmd);
    } catch (const AnalyzeException& e) {
        std::cerr << "Analyze error: " << e.what() << std::endl;
        return;
    }

    PlanGraphGenerator planGen(view, callback);
    planGen.generate(queryCmd);
    const PlanGraph& planGraph = planGen.getPlanGraph();

    planGraph.dump(std::cout);
}
