#include <stdlib.h>

#include <iostream>
#include <sstream>

#include "SystemManager.h"
#include "Time.h"
#include "PlanGraphDebug.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "YCypherScanner.h"
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
void runParser2(const std::string& query);

int main(int argc, char** argv) {
    std::string queryStr;

    if (argc == 2) {
        queryStr = argv[1];

    } else {
        fs::Path path(SAMPLE_DIR "/parser-queries.txt");
        fmt::print("Reading query from file: {}\n", path.get());
        fs::File file = fs::File::open(path).value();
        fs::FileReader reader;

        reader.setFile(&file);
        reader.read();

        auto it = reader.iterateBuffer();

        queryStr = it.get<char>(file.getInfo()._size);
    }

    runParser2(queryStr);

    return EXIT_SUCCESS;
}

void runParser2(const std::string& query) {
    YCypherScanner yscanner;
    yscanner.setQuery(query);

    YCypherParser yparser(yscanner);

    std::istringstream iss;
    iss.rdbuf()->pubsetbuf((char*)query.data(), query.size());

    yscanner.switch_streams(&iss, NULL);

    try {
        auto t0 = Clock::now();
        yparser.parse();
        fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, Clock::now()));
    } catch (const ParserException& e) {
        fmt::print("{}\n", e.what());
    }
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
