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
#include "PlanGraphGenerator.h"
#include "PlanGraph.h"
#include "PlanGraphDebug.h"
#include "PipelineV2.h"
#include "LocalMemory.h"
#include "PipelineGenerator.h"
#include "PlanOptimizer.h"
#include "PipelineExecutor.h"
#include "ExecutionContext.h"
#include "SystemManager.h"
#include "JobSystem.h"
#include "TuringConfig.h"

using namespace db;

int main(int argc, char** argv) {
    std::string queryStr;

    TuringConfig config;
    config.setSyncedOnDisk(false);

    const auto jobSystem = JobSystem::create();
    SystemManager sysMan(&config);
    Graph* graph = sysMan.createGraph("simpledb");
    SimpleGraph::createSimpleGraph(graph);

    auto procedures = ProcedureBlueprintMap::create();

    Transaction transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    bool pipelineGenEnabled = false;
    bool planOptEnabled = true;
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
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "-nopipelinegen")) {
                pipelineGenEnabled = false;
            } else if (!strcmp(argv[i], "-noplanopt")) {
                planOptEnabled = false;
            }
        }
    }

    CypherAST ast(*procedures, queryStr);

    {
        CypherParser parser(&ast);

        fmt::print("\n=== Cypher parsing ===\n\n");

        try {
            auto t0 = Clock::now();
            parser.parse(queryStr);
            auto t1 = Clock::now();
            fmt::print("Query parsed in {} us\n", duration<Microseconds>(t0, t1));
        } catch (const CompilerException& e) {
            fmt::print("{}\n", e.what());
            return 0;
        }

        fmt::print("\n=== Cypher analysis ===\n\n");

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
    PlanGraph& planGraph = planGen.getPlanGraph();
    {
        fmt::print("\n=== Query plan generation ===\n\n");
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

    if (planOptEnabled) {
        fmt::print("\n=== Query plan optimisation ===\n\n");

        try {
            auto t0 = Clock::now();
            PlanOptimizer planOpt(&planGraph);
            planOpt.optimize();
            auto t1 = Clock::now();
            fmt::print("Query plan optimised in {} us\n", duration<Microseconds>(t0, t1));
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
            fmt::print("\n=== Pipeline generation ===\n\n");

            const auto callback = [](const Dataframe* dataframe) {};

            PipelineGenerator pipelineGen(&planGraph,
                                          view,
                                          &pipeline,
                                          &mem,
                                          *procedures,
                                          callback);
            try {
                auto t0 = Clock::now();
                pipelineGen.generate();
                auto t1 = Clock::now();
                fmt::print("Query pipeline generated in {} us\n", duration<Microseconds>(t0, t1));
            } catch (const CompilerException& e) {
                fmt::print("{}\n", e.what());
                return EXIT_FAILURE;
            }
        }

        {
            fmt::print("\n=== Execution ===\n\n");

            ExecutionContext execCtxt(&sysMan, view);
            execCtxt.setTransaction(&transaction);
            execCtxt.setJobSystem(jobSystem.get());

            PipelineExecutor executor(&pipeline, &execCtxt);
            try {
                const auto t0 = Clock::now();
                executor.execute();
                const auto t1 = Clock::now();
                fmt::print("Query pipeline executed in {} us\n", duration<Microseconds>(t0, t1));
            } catch (const CompilerException& e) {
                fmt::print("{}\n", e.what());
                return EXIT_FAILURE;
            }
        }
    }

    return EXIT_SUCCESS;
}
