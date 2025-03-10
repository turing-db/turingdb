#include <stdlib.h>
#include <sstream>
#include <iostream>

#include <spdlog/spdlog.h>

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

    spdlog::info("Create company graph");
    SimpleGraph::createSimpleGraph(db);

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

    const PropertyType name = defaultGraph->getMetadata()->propTypes().get("name");
    const auto reader = tx.readGraph();
    spdlog::info("Nodes:");
    for (auto name : reader.scanNodeProperties<types::String>(name._id)) {
        spdlog::info(" - {}", name);
    }
    spdlog::info("Edges:");
    for (auto name : reader.scanEdgeProperties<types::String>(name._id)) {
        spdlog::info(" - {}", name);
    }

    return EXIT_SUCCESS;
}
