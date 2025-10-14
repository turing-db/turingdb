#include <iostream>
#include <memory>

#include "CypherASTDumper.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "SourceManager.h"
#include "TuringTime.h"
#include "FileReader.h"
#include "SimpleGraph.h"
#include "CypherAST.h"
#include "ASTException.h"
#include "versioning/Transaction.h"
#include "PlanGraphGenerator.h"
#include "PlanGraph.h"
#include "PlanGraphDebug.h"

using namespace db;
using namespace db::v2;

int main(int argc, char** argv) {
    std::string queryStr;

    std::unique_ptr<Graph> graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const Transaction transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    if (argc > 1 && strlen(argv[1]) > 0) {
        queryStr = argv[1];

    } else {
        fs::Path path(SAMPLE_DIR "/queries.txt");
        fmt::print("Reading query from file: {}\n", path.get());
        fs::File file = fs::File::open(path).value();
        fs::FileReader reader;

        reader.setFile(&file);
        reader.read();

        auto it = reader.iterateBuffer();

        queryStr = it.get<char>(file.getInfo()._size);
    }

    CypherAST ast(queryStr);
    ast.getSourceManager()->setDebugLocations(true);

    {
        CypherParser parser(&ast);
        parser.allowNotImplemented(false);

        try {
            auto t0 = Clock::now();
            parser.parse(queryStr);
            auto t1 = Clock::now();
            fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const ASTException& e) {
            fmt::print("{}\n", e.what());
            return 0;
        }

        CypherAnalyzer analyzer(&ast, view);

        try {
            auto t0 = Clock::now();
            analyzer.analyze();
            auto t1 = Clock::now();
            fmt::print("Query analyzed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const ASTException& e) {
            fmt::print("{}\n", e.what());
            return EXIT_FAILURE;
        }


        CypherASTDumper dumper(analyzer.getAST());
        dumper.dump(std::cout);
    }

    {
        PlanGraphGenerator planGen(ast, view);
        try {
            auto t0 = Clock::now();
            planGen.generate(ast.queries().front());
            auto t1 = Clock::now();
            fmt::print("Query plan generated in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const ASTException& e) {
            fmt::print("{}\n", e.what());
            return EXIT_FAILURE;
        }
        const PlanGraph& planGraph = planGen.getPlanGraph();

        PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);
    }

    return EXIT_SUCCESS;
}
