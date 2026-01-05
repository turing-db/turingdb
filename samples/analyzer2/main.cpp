#include <iostream>
#include <memory>

#include "CypherASTDumper.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "TuringTime.h"
#include "FileReader.h"
#include "SimpleGraph.h"
#include "CypherAST.h"
#include "CompilerException.h"
#include "procedures/ProcedureBlueprintMap.h"
#include "versioning/Transaction.h"

using namespace db;

int main(int argc, char** argv) {
    std::string queryStr;

    std::unique_ptr<Graph> graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    auto procedures = ProcedureBlueprintMap::create();

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

    CypherAST ast(*procedures, queryStr);

    {
        CypherParser parser(&ast);

        try {
            auto t0 = Clock::now();
            parser.parse(queryStr);
            auto t1 = Clock::now();
            fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const CompilerException& e) {
            fmt::print("{}\n", e.what());
            return 0;
        }

        CypherAnalyzer analyzer(&ast, view);

        try {
            auto t0 = Clock::now();
            analyzer.analyze();
            auto t1 = Clock::now();
            fmt::print("Query analyzed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const CompilerException& e) {
            fmt::print("{}\n", e.what());
            return EXIT_FAILURE;
        }


        CypherASTDumper dumper(analyzer.getAST());
        dumper.dump(std::cout);
    }

    return EXIT_SUCCESS;
}
