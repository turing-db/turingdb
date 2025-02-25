#include <stdlib.h>
#include <sstream>
#include <iostream>

#include "TuringDB.h"
#include "Graph.h"
#include "reader/GraphReader.h"
#include "SystemManager.h"
#include "SimpleGraph.h"
#include "GraphDumper.h"
#include "GraphReport.h"

#include "ToolInit.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("simpledb");
    toolInit.init(argc, argv);

    const auto& outDir = fs::Path(toolInit.getOutputsDir());

    TuringDB db;

    std::cout << "* Create company graph\n";
    SimpleGraph::createSimpleGraph(db);

    const Graph* defaultGraph = db.getSystemManager().getDefaultGraph();

    std::cout << "* Graph created\n";
    { 
        std::stringstream sstream;
        GraphReport::getReport(defaultGraph->read(), sstream);
        std::cout << sstream.str() << '\n';
    }

    std::cout << "* Dump graph\n";
    const auto dumpRes = GraphDumper::dump(*defaultGraph, outDir);

    return EXIT_SUCCESS;
}
