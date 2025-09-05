#include "properties/PropertyContainer.h"
#include "spdlog/spdlog.h"
#include "tabulate/table.hpp"

#include "DataPart.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "ArcManager.h"
#include "NodeContainer.h"
#include "Graph.h"
#include "reader/GraphReader.h"
#include "SimpleGraph.h"
#include "versioning/DataPartModifier.h"
#include "versioning/Transaction.h"
#include "versioning/NodeContainerModifier.h"
#include "LocalMemory.h"

using namespace db;

namespace {
    // Assuming LabelSetID and NodeID are integer types
    [[maybe_unused]] void prettyPrintNodeContainer(const NodeContainer& cont) {
        using namespace tabulate;
        auto& indexer = cont.getLabelSetIndexer();

        Table table;
        table.add_row({"LabelSetID", "NodeIDs"});

        for (const auto& [handle, range] : indexer) {
            std::ostringstream oss;

            for (size_t i = 0; i < range._count; ++i) {
                if (i > 0) {
                    oss << ", ";
                }
                oss << (range._first + i);
            }

            table.add_row({std::to_string(handle.getID()), oss.str()});
        }

        // Optional styling
        table.format()
            .font_align(FontAlign::center)
            .border_top("─")
            .border_bottom("─")
            .border_left("│")
            .border_right("│")
            .corner("┼");

        std::cout << table << std::endl << std::endl << std::endl;
    }

    using StrPropContainer = TypedPropertyContainer<types::String>;
    // Assuming LabelSetID and NodeID are integer types
    [[maybe_unused]] void prettyPrintStrPropContainer(const StrPropContainer& cont) {
        using namespace tabulate;

        Table table;
        table.add_row({"EdgeID", "NodeIDs"});

        for (const auto& [id, val] : cont.zipped()) {
            std::ostringstream oss;
            table.add_row({std::to_string(id.getValue()), val});
        }

        // Optional styling
        table.format()
            .font_align(FontAlign::center)
            .border_top("─")
            .border_bottom("─")
            .border_left("│")
            .border_right("│")
            .corner("┼");

        std::cout << table << std::endl << std::endl << std::endl;
    }

    [[maybe_unused]] void prettyPrintEdgeContainer(const EdgeContainer& cont) {
        using namespace tabulate;
        auto outEdges = cont.getOuts();

        Table table;
        table.add_row({"EdgeID", "SrcID", "TgtID"});

        for (const EdgeRecord& r : outEdges) {
            table.add_row({std::to_string(r._edgeID),
                           std::to_string(r._nodeID),
                           std::to_string(r._otherID)});
        }

        // Optional styling
        table.format()
            .font_align(FontAlign::center)
            .border_top("─")
            .border_bottom("─")
            .border_left("│")
            .border_right("│")
            .corner("┼");

        std::cout << table << std::endl << std::endl << std::endl;
    }
}

void nodeContainerTest() {
    auto g = Graph::create();
    SimpleGraph::createSimpleGraph(g.get());

    // Get original container
    auto dps = g->openTransaction().readGraph().dataparts();
    auto dp = dps[0];
    spdlog::info("Orignal num nodes: {}", dp->nodes().size());
    prettyPrintNodeContainer(dp->nodes());

    {
        spdlog::info("After deleting 0:");
        std::set<NodeID> toDel {0};

        auto newContainer = NodeContainer::create(0, std::vector<LabelSetHandle> {});
        NodeContainerModifier::deleteNodes(dp->nodes(), *newContainer, toDel);

        spdlog::info("New num nodes: {}", newContainer->size());
        prettyPrintNodeContainer(*newContainer);
        
    }

    {
        spdlog::info("After deleting 0,1,2,3:");
        std::set<NodeID> toDel {0, 1, 2, 3};
        auto newContainer = NodeContainer::create(0, std::vector<LabelSetHandle> {});
        NodeContainerModifier::deleteNodes(dp->nodes(), *newContainer, toDel);
        spdlog::info("New num nodes: {}", newContainer->size());
        prettyPrintNodeContainer(*newContainer);
    }

    {
        spdlog::info("After deleting all nodes:");
        std::set<NodeID> toDel {0, 1, 2, 3, 4, 5, 6};
        auto newContainer = NodeContainer::create(0, std::vector<LabelSetHandle> {});
        NodeContainerModifier::deleteNodes(dp->nodes(), *newContainer, toDel);
        spdlog::info("New num nodes: {}", newContainer->size());
        prettyPrintNodeContainer(*newContainer);
    }
}

void datapartTest() {
    spdlog::info("Testing deletion using DataPartModifier");
    auto g = Graph::create();
    SimpleGraph::createSimpleGraph(g.get());

    const WeakArc<DataPart> oldDP = g->openTransaction().readGraph().dataparts()[0];

    spdlog::info("Orignal size: {}", oldDP->nodes().size());
    prettyPrintNodeContainer(oldDP->nodes());
    prettyPrintEdgeContainer(oldDP->edges());

    // TODO: modularise
    {
        spdlog::info("Attempting to delete nodes: {0}, and edges: {}");
        std::set<NodeID> nodesToDelete {0};

        WeakArc<DataPart> newDP = DataPartModifier::newDPinGraph(g.get());
        auto modifier = DataPartModifier::create(oldDP, newDP, nodesToDelete, {});

        modifier->applyDeletions();

        spdlog::info("New num nodes: {}", newDP->nodes().size());
        prettyPrintNodeContainer(newDP->nodes());
        prettyPrintEdgeContainer(newDP->edges());
    }

}


void reconstructTest() {
    spdlog::info("Testing deletion using DataPartModifier::reconstruct");

    TuringDB db;
    LocalMemory mem;

    db.getSystemManager().loadGraph("simpledb", db.getJobSystem());
    Graph* g = db.getSystemManager().getGraph("simpledb");

    const WeakArc<DataPart> oldDP = g->openTransaction().readGraph().dataparts()[0];
    spdlog::info("Orignal size: {}", oldDP->nodes().size());
    prettyPrintNodeContainer(oldDP->nodes());
    prettyPrintEdgeContainer(oldDP->edges());

    const StrPropContainer& oldSPC =
        oldDP->edgeProperties().getContainer<types::String>(0);
    prettyPrintStrPropContainer(oldSPC);

    db.query("CALL properties ()", "simpledb", &mem);

    {
        spdlog::info("Attempting to delete nodes: {0}, and edges: {}");
        std::set<NodeID> nodesToDelete {0};

        auto modifier = DataPartModifier::create(oldDP, {}, nodesToDelete, {});
        modifier->applyModifications();

        DataPart newDP = DataPart(0, 0);
        newDP.load(g->openTransaction().viewGraph(), db.getJobSystem(), modifier->builder());

        prettyPrintNodeContainer(newDP.nodes());
        prettyPrintEdgeContainer(newDP.edges());
    }
}

int main() {
    // nodeContainerTest();
    // datapartTest();
    reconstructTest();
}
