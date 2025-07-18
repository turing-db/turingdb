#include <stdlib.h>

#include <iostream>

#include "SystemManager.h"
#include "Time.h"
#include "PlanGraphDebug.h"
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
#include "FileReader.h"

using namespace db;

void runPlan2(std::string_view query);

int main(int argc, char** argv) {
    std::string queryStr;

    if (argc == 2) {
        queryStr = argv[1];

    } else {
        fs::Path path(SAMPLE_DIR "/query.txt");
        fmt::print("Reading query from file: {}\n", path.get());
        fs::File file = fs::File::open(path).value();
        fs::FileReader reader;

        reader.setFile(&file);
        reader.read();

        auto it = reader.iterateBuffer();

        queryStr = it.get<char>(file.getInfo()._size);
    }

    runPlan2(queryStr);

    return EXIT_SUCCESS;
}

void runPlan2(std::string_view query) {
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
        auto t0 = Clock::now();
        queryCmd = parser.parse(query);
        if (!queryCmd) {
            std::cerr << "Failed to parse query" << std::endl;
            return;
        }
        fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, Clock::now()));
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

    PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);
}
