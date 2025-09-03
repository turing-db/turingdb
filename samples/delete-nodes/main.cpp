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
    auto dp = dps[0];
    spdlog::info("Old size: {}", dp->nodes().size());
    for (const auto& [lblSetHdl, ndRng]: dp->nodes().getLabelSetIndexer()) {
        spdlog::info("Label set: {}, Range: {}-{}", lblSetHdl.getID(), ndRng._first,
                     ndRng._first + ndRng._count - 1);
    }

    std::set<NodeID> toDel{0};
    auto newContainer = NodeContainerModifier::deleteNode(dp->nodes(), toDel);
    spdlog::info("New size: {}", newContainer->size());
    for (const auto& [lblSetHdl, ndRng] : newContainer->getLabelSetIndexer()) {
        spdlog::info("Label set: {}, Range: {}-{}", lblSetHdl.getID(), ndRng._first,
                     ndRng._first + ndRng._count - 1);
    }
}
