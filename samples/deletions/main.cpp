#include "DeleteTest.h"

#include <spdlog/spdlog.h>

#include "Graph.h"
#include "SimpleGraph.h"
#include "dump/GraphDumper.h"

using namespace db;

namespace {

void getSimpleDB() {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());
    if (auto res = GraphDumper::dump(*graph, DeleteTest::WORKING_PATH); !res) {
        spdlog::error("{}\n", res.error().fmtMessage());
        std::abort();
    }
}

}

int main() {

    { // Add simpledb to script directory
        getSimpleDB();
    }

    {
        DeleteTest tester {"DELETE NODES MATCH NODES", "simpledb"};
        tester.addQuery("match (n) return n");
        tester.deleteNodes({0,1,2});
        tester.run();
    }

    {
        DeleteTest tester {"DELETE EDGES MATCH EDGES", "simpledb"};
        tester.addQuery("match (n)-[e]-(m) return e");
        tester.deleteEdges({10, 11});
        tester.run();
    }

    {
        DeleteTest tester {"DELETE NODES MATCH EDGES", "simpledb"};
        tester.addQuery("match (n)-[e]-(m) return e");
        tester.deleteNodes({0});
        tester.run();
    }

    return 0;
}
