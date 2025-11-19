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
#include "versioning/Transaction.h"
#include "PlanGraphGenerator.h"
#include "PlanGraph.h"
#include "PlanGraphDebug.h"
#include "PipelineV2.h"
#include "LocalMemory.h"
#include "PipelineGenerator.h"
#include "PipelineExecutor.h"
#include "ExecutionContext.h"

using namespace db;
using namespace db::v2;

int main(int argc, char** argv) {
    std::string queryStr;

    std::unique_ptr<Graph> graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const Transaction transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    bool pipelineGenEnabled = false;
    if (argc >= 2 && strlen(argv[1]) > 0) {
        queryStr = argv[1];
        pipelineGenEnabled = true;
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

    {
        if (argc >= 3 && strcmp(argv[2], "-nopipeline") == 0) {
            pipelineGenEnabled = false;
        }
    }

    CypherAST ast(queryStr);

    {
        CypherParser parser(&ast);
        parser.allowNotImplemented(false);

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

    PlanGraphGenerator planGen(ast, view);
    const PlanGraph& planGraph = planGen.getPlanGraph();
    {
        try {
            auto t0 = Clock::now();
            planGen.generate(ast.queries().front());
            auto t1 = Clock::now();
            fmt::print("Query plan generated in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const CompilerException& e) {
            fmt::print("{}\n", e.what());
            return EXIT_FAILURE;
        }

        PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);
    }

    if (pipelineGenEnabled) {
        LocalMemory mem;
        PipelineV2 pipeline;
        {
            auto callback = [](const Dataframe* dataframe) {};

            PipelineGenerator pipelineGen(&planGraph, view, &pipeline, &mem, ast.getSourceManager(), callback);
            try {
                auto t0 = Clock::now();
                pipelineGen.generate();
                auto t1 = Clock::now();
                fmt::print("Query pipeline generated in {} us\n", duration<Microseconds>(t0, t1));
            } catch (const CompilerException& e) {
                fmt::print("{}\n", e.what());
                return EXIT_FAILURE;
            }
            const PlanGraph& planGraph = planGen.getPlanGraph();

            PlanGraphDebug::dumpMermaid(std::cout, view, planGraph);
        }

        {
            ExecutionContext execCtxt(view);
            PipelineExecutor executor(&pipeline, &execCtxt);
            try {
                auto t0 = Clock::now();
                executor.execute();
                auto t1 = Clock::now();
                fmt::print("Query pipeline executed in {} us\n", duration<Microseconds>(t0, t1));
            } catch (const CompilerException& e) {
                fmt::print("{}\n", e.what());
                return EXIT_FAILURE;
            }
        }
    }

    return EXIT_SUCCESS;
}
