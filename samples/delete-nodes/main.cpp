#include "NodeContainer.h"

#include "Graph.h"
#include "reader/GraphReader.h"
#include "SimpleGraph.h"
#include "spdlog/spdlog.h"
#include "versioning/Transaction.h"
#include "versioning/NodeContainerModifier.h"

using namespace db;

int main() {
    auto g = Graph::create();
    SimpleGraph::createSimpleGraph(g.get());

    auto dps = g->openTransaction().readGraph().dataparts();
    spdlog::info("Old size: {}", dps[0]->nodes().size());

    std::set<NodeID> toDel{0};
    auto newContainer = NodeContainerModifier::deleteNode(dps[0]->nodes(), toDel);
    spdlog::info("New size: {}", newContainer->size());
}
