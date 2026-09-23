#include "TuringTest.h"

#include <string>
#include <vector>

#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "Path.h"
#include "TuringException.h"

#include "Graph.h"
#include "comparators/GraphComparator.h"
#include "map/MapContainer.h"
#include "map/MapView.h"
#include "metadata/DateTime.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

using MapEntry = MapContainer::MapKeyValuePair;

}

class MapDateTimeGraphDumpLoadTest : public TuringTest {
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

TEST_F(MapDateTimeGraphDumpLoadTest, MapsHoldingInstants) {
    constexpr size_t nodeCount = 20;
    constexpr int64_t microsecondsPerHour = 3600LL * 1000 * 1000;

    auto graph = Graph::create("mapgraph", fs::Path {_outDir} / "original");
    MapContainer maps;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Event"});
            const DateTime at {(static_cast<int64_t>(i) - 10) * microsecondsPerHour};

            const std::vector<MapEntry> entries = {{"at", at}, {"n", static_cast<int64_t>(i)}};
            writer.addNodeProperty<types::Map>(node, "attrs", maps.insert(entries));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}
