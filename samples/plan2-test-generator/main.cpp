#include <stdlib.h>

#include <iostream>

#include "SystemManager.h"
#include "TuringDB.h"
#include "TuringTime.h"
#include "PlanGraphDebug.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "procedures/ProcedureBlueprintMap.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "PlanGraphGenerator.h"
#include "PlanGraph.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "CypherAST.h"
#include "CompilerException.h"
#include "FileReader.h"
#include "TuringConfig.h"

using namespace db;

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

    TuringConfig config;
    config.setTuringDirectory(turingDir);
    TuringDB db(&config);
    db.init();

    Graph* graph = db.getSystemManager().createGraph("simpledb");
    SimpleGraph::createSimpleGraph(graph);
    
    auto procedures = ProcedureBlueprintMap::create();

    const Transaction transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    CypherAST ast(*procedures, query);
    CypherParser parser(&ast);

    try {
        auto t0 = Clock::now();
        parser.parse(query);
        fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, Clock::now()));
    } catch (const CompilerException& e) {
        fmt::println(std::cerr, "/// QUERY\n{}\n", query);
        fmt::println(std::cerr, "/// RESULT");
        fmt::println(std::cerr, "PARSE ERROR");
        fmt::println(std::cerr, "{}", e.what());
        return;
    }

    CypherAnalyzer analyzer(&ast, view);
    try {
        auto t0 = Clock::now();
        analyzer.analyze();
        fmt::print("Query analyzed in {} us\n", duration<Microseconds>(t0, Clock::now()));
    } catch (const CompilerException& e) {
        fmt::println(std::cerr, "/// QUERY\n{}\n", query);
        fmt::println(std::cerr, "/// RESULT");
        fmt::println(std::cerr, "ANALYZE ERROR");
        fmt::println(std::cerr, "{}", e.what());
        return;
    }

    PlanGraphGenerator planGen(ast, view);
    try {
        const auto t0 = Clock::now();
        planGen.generate(ast.queries().front());
        fmt::print("Query plan generated in {} us\n", duration<Microseconds>(t0, Clock::now()));
    } catch (const CompilerException& e) {
        fmt::println(std::cerr, "/// QUERY\n{}\n", query);
        fmt::println(std::cerr, "/// RESULT");
        fmt::println(std::cerr, "PLAN ERROR");
        fmt::println(std::cerr, "{}", e.what());
        return;
    }

    const PlanGraph& planGraph = planGen.getPlanGraph();

    fmt::println("/// QUERY\n{}\n", query);
    fmt::println("/// RESULT");
    PlanGraphDebug::dumpMermaidContent(std::cout, view, planGraph);
}
