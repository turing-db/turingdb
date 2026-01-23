#include <stdlib.h>
#include <sstream>
#include <iostream>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "dump/GraphLoader.h"
#include "dump/GraphDumper.h"
#include "TuringDB.h"
#include "Graph.h"
#include "reader/GraphReader.h"
#include "SystemManager.h"
#include "SimpleGraph.h"
#include "GraphReport.h"
#include "versioning/Transaction.h"
#include "TuringConfig.h"

#include "ToolInit.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("simpledb");

    auto& argParser = toolInit.getArgParser();

    std::string turingDirArg;
    argParser.add_argument("-turing-dir")
             .metavar("path")
             .store_into(turingDirArg)
             .help("Root Turing directory (defaults to SAMPLE_DIR/.turing)");

    toolInit.init(argc, argv);

    fs::Path turingDir;
    if (!turingDirArg.empty()) {
        turingDir = fs::Path(turingDirArg);
        if (!turingDir.toAbsolute()) {
            spdlog::error("Failed to get absolute path of turing directory {}", turingDirArg);
            return EXIT_FAILURE;
        }
    } else {
        turingDir = fs::Path(SAMPLE_DIR) / ".turing";
    }

    if (turingDir.exists()) {
        turingDir.rm();
    }

    const auto& outDir = fs::Path(toolInit.getOutputsDir()) / "simpledb";

    TuringConfig config;
    config.setTuringDirectory(turingDir);

    TuringDB db(&config);
    db.init();

    {
        spdlog::info("Create company graph");
        auto* graph = db.getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(graph);

        spdlog::info("Graph created");
        const FrozenCommitTx tx = graph->openTransaction();
        {
            std::stringstream sstream;
            GraphReport::getReport(tx.readGraph(), sstream);
            std::cout << sstream.str() << '\n';
        }

        spdlog::info("Dump graph into {}", outDir.c_str());
        const auto dumpRes = GraphDumper::dump(*graph, outDir);
        if (!dumpRes) {
            spdlog::error("{}", dumpRes.error().fmtMessage());
            return EXIT_FAILURE;
        }
    }

    {
        auto graph = Graph::create();
        const auto loadRes = GraphLoader::load(graph.get(), outDir);
        const auto tx = graph->openTransaction();

        const PropertyType name = tx.viewGraph().metadata().propTypes().get("name").value();
        const auto reader = tx.readGraph();
        spdlog::info("Nodes:");
        {
            auto it = reader.scanNodeProperties<types::String>(name._id).begin();
            for (; it.isValid(); ++it) {
                spdlog::info(" - {} {}", it.getCurrentNodeID(), it.get());
            }
        }

        spdlog::info("Edges:");
        {
            auto it = reader.scanEdgeProperties<types::String>(name._id).begin();
            for (; it.isValid(); ++it) {
                spdlog::info(" - {} {}", it.getCurrentEdgeID(), it.get());
            }
        }
    }

    return EXIT_SUCCESS;
}
