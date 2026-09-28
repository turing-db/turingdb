#include "TuringTest.h"

#include <stdint.h>

#include <string>
#include <vector>

#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "Path.h"
#include "TuringException.h"

#include "Graph.h"
#include "comparators/GraphComparator.h"
#include "map/MapContainer.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

using MapEntry = MapContainer::MapKeyValuePair;

constexpr int64_t microsecondsPerMinute = 60LL * 1000000;

}

class DurationGraphDumpLoadTest : public TuringTest {
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

TEST_F(DurationGraphDumpLoadTest, NodeDurations) {
    constexpr size_t nodeCount = 100;

    auto graph = Graph::create("durationgraph", fs::Path {_outDir} / "original");

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Task"});
            const int64_t minutes = static_cast<int64_t>(i) - static_cast<int64_t>(nodeCount / 2);

            writer.addNodeProperty<types::Duration>(node, "took", Duration {minutes * microsecondsPerMinute});
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(DurationGraphDumpLoadTest, EdgeDurations) {
    constexpr size_t edgeCount = 50;

    auto graph = Graph::create("durationgraph", fs::Path {_outDir} / "original");

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        const NodeID source = writer.addNode({"Person"});

        for (size_t i = 0; i < edgeCount; i++) {
            const NodeID target = writer.addNode({"Person"});
            const EdgeRecord edge = writer.addEdge("CALLED", source, target);

            writer.addEdgeProperty<types::Duration>(edge, "for", Duration {static_cast<int64_t>(i) * microsecondsPerMinute});
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

// A Duration, a DateTime and an Int64 carry the same eight bytes under three tags, so a
// dump holding all three is what would catch the loader reading one as another
TEST_F(DurationGraphDumpLoadTest, DurationsBesideInstantsAndIntegersOfTheSameValue) {
    constexpr size_t nodeCount = 64;

    auto graph = Graph::create("durationgraph", fs::Path {_outDir} / "original");

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Task"});
            const int64_t microseconds = static_cast<int64_t>(i) * microsecondsPerMinute;

            writer.addNodeProperty<types::Int64>(node, "raw", int64_t {microseconds});
            writer.addNodeProperty<types::DateTime>(node, "at", DateTime {microseconds});
            writer.addNodeProperty<types::Duration>(node, "took", Duration {microseconds});
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(DurationGraphDumpLoadTest, MapsHoldingDurations) {
    constexpr size_t nodeCount = 20;

    auto graph = Graph::create("durationgraph", fs::Path {_outDir} / "original");
    MapContainer maps;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Task"});
            const Duration took {(static_cast<int64_t>(i) - 10) * microsecondsPerMinute};

            const std::vector<MapEntry> entries = {{"n", static_cast<int64_t>(i)}, {"took", took}};
            writer.addNodeProperty<types::Map>(node, "attrs", maps.insert(entries));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}
