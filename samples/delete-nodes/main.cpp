#include "ArcManager.h"
#include "NodeContainer.h"

#include "Graph.h"
#include "reader/GraphReader.h"
#include "SimpleGraph.h"
#include "spdlog/spdlog.h"
#include "versioning/DataPartModifier.h"
#include "versioning/Transaction.h"
#include "versioning/NodeContainerModifier.h"

using namespace db;

void nodeContainerTest() {
    auto g = Graph::create();
    SimpleGraph::createSimpleGraph(g.get());

    // Get original container
    auto dps = g->openTransaction().readGraph().dataparts();
    auto dp = dps[0];
    spdlog::info("Orignal num nodes: {}", dp->nodes().size());
    for (const auto& [lblSetHdl, ndRng]: dp->nodes().getLabelSetIndexer()) {
        spdlog::info("Label set: {}, Range: {}-{}", lblSetHdl.getID(), ndRng._first,
                     ndRng._first + ndRng._count - 1);
    }

    {
        spdlog::info("After deleting 0:");
        std::set<NodeID> toDel {0};
        auto newContainer = NodeContainerModifier::deleteNodes(dp->nodes(), toDel);
        spdlog::info("New num nodes: {}", newContainer->size());
        for (const auto& [lblSetHdl, ndRng] : newContainer->getLabelSetIndexer()) {
            spdlog::info("Label set: {}, Range: {}-{}", lblSetHdl.getID(), ndRng._first,
                         ndRng._first + ndRng._count - 1);
        }
    }

    {
        spdlog::info("After deleting 0,1,2,3:");
        std::set<NodeID> toDel {0, 1, 2, 3};
        auto newContainer = NodeContainerModifier::deleteNodes(dp->nodes(), toDel);
        spdlog::info("New num nodes: {}", newContainer->size());
        for (const auto& [lblSetHdl, ndRng] : newContainer->getLabelSetIndexer()) {
            spdlog::info("Label set: {}, Range: {}-{}", lblSetHdl.getID(), ndRng._first,
                         ndRng._first + ndRng._count - 1);
        }
    }

    {
        spdlog::info("After deleting all nodes:");
        std::set<NodeID> toDel {0, 1, 2, 3, 4, 5, 6};
        auto newContainer = NodeContainerModifier::deleteNodes(dp->nodes(), toDel);
        spdlog::info("New num nodes: {}", newContainer->size());
        for (const auto& [lblSetHdl, ndRng] : newContainer->getLabelSetIndexer()) {
            spdlog::info("Label set: {}, Range: {}-{}", lblSetHdl.getID(), ndRng._first,
                         ndRng._first + ndRng._count - 1);
        }
    }
}

void datapartTest() {
    spdlog::info("Testing deletion using DataPartModifier");
    auto g = Graph::create();
    SimpleGraph::createSimpleGraph(g.get());

    const WeakArc<DataPart> oldDP = g->openTransaction().readGraph().dataparts()[0];

    spdlog::info("Orignal size: {}", oldDP->nodes().size());
    for (const auto& [lblSetHdl, ndRng] : oldDP->nodes().getLabelSetIndexer()) {
        spdlog::info("Label set: {}, Range: {}-{}", lblSetHdl.getID(), ndRng._first,
                     ndRng._first + ndRng._count - 1);
    }

    // TODO: modularise
    {
        spdlog::info("Attempting to delete nodes: {0}, and edges: {}");
        std::set<NodeID> nodesToDelete {0};

        WeakArc<DataPart> newDP = DataPartModifier::newDPinGraph(g.get());
        auto modifier = DataPartModifier::create(oldDP, newDP, nodesToDelete, {});

        modifier->applyDeletions();

        spdlog::info("New num nodes: {}", newDP->nodes().size());
        for (const auto& [lblSetHdl, ndRng] : newDP->nodes().getLabelSetIndexer()) {
            spdlog::info("Label set: {}, Range: {}-{}", lblSetHdl.getID(), ndRng._first,
                         ndRng._first + ndRng._count - 1);
        }
    }
}

int main() {
    // nodeContainerTest();
    datapartTest();
}
