#include <stdlib.h>
#include <sstream>
#include <iostream>

#include <spdlog/spdlog.h>

#include "GraphLoader.h"
#include "TuringDB.h"
#include "Graph.h"
#include "reader/GraphReader.h"
#include "SystemManager.h"
#include "SimpleGraph.h"
#include "GraphDumper.h"
#include "GraphReport.h"

#include "ToolInit.h"
#include "versioning/Transaction.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("simpledb");
    toolInit.init(argc, argv);

    const auto& outDir = fs::Path(toolInit.getOutputsDir()) / "simpledb";

    TuringDB db;

    {
        spdlog::info("Create company graph");
        auto* graph = db.getSystemManager().getDefaultGraph();
        SimpleGraph::createSimpleGraph(graph);

        const Graph* defaultGraph = db.getSystemManager().getDefaultGraph();

        spdlog::info("Graph created");
        const Transaction tx = defaultGraph->openTransaction();
        {
            std::stringstream sstream;
            GraphReport::getReport(tx.readGraph(), sstream);
            std::cout << sstream.str() << '\n';
        }

        spdlog::info("Dump graph into {}", outDir.c_str());
        const auto dumpRes = GraphDumper::dump(*defaultGraph, outDir);
        if (!dumpRes) {
            spdlog::error("{}", dumpRes.error().fmtMessage());
            return EXIT_FAILURE;
        }
    }


    {
        auto graph = Graph::createEmptyGraph();
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
