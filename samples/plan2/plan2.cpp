#include <stdlib.h>

#include <iostream>

#include "PlannerException.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "TuringTime.h"
#include "PlanGraphDebug.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "PlanGraphGenerator.h"
#include "PlanGraph.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "CypherAST.h"
#include "ParserException.h"
#include "AnalyzeException.h"
#include "FileReader.h"
#include "TuringConfig.h"

using namespace db;
using namespace db::v2;

static void runPlan2(std::string_view query);

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
    const fs::Path turingDir = fs::Path(SAMPLE_DIR) / ".turing";
    if (turingDir.exists()) {
        turingDir.rm();
    }

    TuringConfig config = TuringConfig::createDefault();
    config.setTuringDirectory(fs::Path(SAMPLE_DIR) / ".turing");
    TuringDB db(&config);
    db.run();

    Graph* graph = db.getSystemManager().createGraph("simpledb");
    SimpleGraph::createSimpleGraph(graph);

    const Transaction transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    auto callback = [](const Block& block) {};

    CypherAST ast(query);
    CypherParser parser(&ast);

    try {
        auto t0 = Clock::now();
        parser.parse(query);
        fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, Clock::now()));
    } catch (const ParserException& e) {
        std::cerr << "Syntax error:\n"
                  << e.what() << std::endl;
        return;
    }

    CypherAnalyzer analyzer(&ast, view);
    try {
        auto t0 = Clock::now();
        analyzer.analyze();
        fmt::print("Query analyzed in {} us\n", duration<Microseconds>(t0, Clock::now()));
    } catch (const AnalyzeException& e) {
        std::cerr << "Analyze error:\n"
                  << e.what() << std::endl;
        return;
    }

    PlanGraphGenerator planGen(ast, view, callback);
    try {
        auto t0 = Clock::now();
        planGen.generate(ast.queries().front());
        fmt::print("Query plan generated in {} us\n", duration<Microseconds>(t0, Clock::now()));
    } catch (const PlannerException& e) {
        std::cerr << "Plan error:\n"
                  << e.what() << std::endl;
        return;
    }

    const PlanGraph& planGraph = planGen.getPlanGraph();

    PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);
}
