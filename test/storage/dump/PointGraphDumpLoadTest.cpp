#include "TuringTest.h"

#include <stdint.h>

#include <vector>

#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "Path.h"
#include "TuringException.h"

#include "Graph.h"
#include "comparators/GraphComparator.h"
#include "list/ListContainer.h"
#include "map/MapContainer.h"
#include "metadata/Point.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

using ListItem = ListContainer::ListItemVariant;
using MapEntry = MapContainer::MapKeyValuePair;

constexpr PointCRS CRS_CYCLE[] = {PointCRS::Cartesian, PointCRS::Cartesian3D, PointCRS::WGS84, PointCRS::WGS84_3D};

Point pointAt(size_t index) {
    const double coordinate = static_cast<double>(index) - 40.0;
    const PointCRS crs = CRS_CYCLE[index % 4];

    return Point(crs, coordinate, coordinate / 2.0, coordinate * 3.0);
}

}

class PointGraphDumpLoadTest : public TuringTest {
protected:
    void initialize() override {
    }

    void dumpAndCompare(Graph* graph) {
        const fs::Path dumpPath = fs::Path {_outDir} / "dump";

        const auto res = GraphDumper::dump(graph, dumpPath);
        if (!res) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }

        auto loadedGraph = Graph::create();
        const auto loadRes = GraphLoader::load(loadedGraph.get(), dumpPath);
        if (!loadRes) {
            throw TuringException("Failed to load graph:\n" + loadRes.error().fmtMessage());
        }

        ASSERT_TRUE(GraphComparator::same(*graph, *loadedGraph));
    }
};

TEST_F(PointGraphDumpLoadTest, NodePoints) {
    constexpr size_t nodeCount = 80;

    auto graph = Graph::create("pointgraph", fs::Path {_outDir} / "original");

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"City"});
            writer.addNodeProperty<types::Point>(node, "at", pointAt(i));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(PointGraphDumpLoadTest, EdgePoints) {
    constexpr size_t edgeCount = 40;

    auto graph = Graph::create("pointgraph", fs::Path {_outDir} / "original");

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        const NodeID source = writer.addNode({"City"});

        for (size_t i = 0; i < edgeCount; i++) {
            const NodeID target = writer.addNode({"City"});
            const EdgeRecord edge = writer.addEdge("ROAD", source, target);

            writer.addEdgeProperty<types::Point>(edge, "midpoint", pointAt(i));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(PointGraphDumpLoadTest, ListsAndMapsHoldingPoints) {
    constexpr size_t nodeCount = 20;

    auto graph = Graph::create("pointgraph", fs::Path {_outDir} / "original");
    ListContainer lists;
    MapContainer maps;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"City"});

            const std::vector<ListItem> route = {pointAt(i), pointAt(i + 1), static_cast<int64_t>(i)};
            writer.addNodeProperty<types::List>(node, "route", lists.insert(route));

            const std::vector<MapEntry> entries = {{"at", pointAt(i)}, {"n", static_cast<int64_t>(i)}};
            writer.addNodeProperty<types::Map>(node, "attrs", maps.insert(entries));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}
