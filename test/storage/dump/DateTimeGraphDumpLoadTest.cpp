#include "TuringTest.h"

#include <stdint.h>

#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "Path.h"
#include "TuringException.h"

#include "Graph.h"
#include "comparators/GraphComparator.h"
#include "metadata/DateTime.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr int64_t microsecondsPerMinute = 60LL * 1000000;
constexpr int64_t microsecondsPerHour = 60 * microsecondsPerMinute;
constexpr int64_t microsecondsPerDay = 24 * microsecondsPerHour;

}

// A datetime property survives a dump and a load. It is a trivial 8-byte value, so the
// generic trivial dumper writes its microseconds straight into the page and the loader
// reads them back; what this pins is that the ValueType tag on disk resolves to
// types::DateTime rather than to the Int64 that shares its width, which the comparator
// checks before it compares a single value.
class DateTimeGraphDumpLoadTest : public TuringTest {
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

TEST_F(DateTimeGraphDumpLoadTest, NodeInstants) {
    constexpr size_t nodeCount = 100;

    auto graph = Graph::create("datetimegraph", fs::Path {_outDir} / "original");

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Event"});
            writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));
            writer.addNodeProperty<types::DateTime>(
                node, "at", DateTime {static_cast<int64_t>(i) * microsecondsPerHour});
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

// Both divisions the renderer makes floor rather than truncate, and both halves of the
// timeline go through the same page, so a dump holding instants on either side of the
// epoch is worth a case of its own
TEST_F(DateTimeGraphDumpLoadTest, InstantsOnBothSidesOfTheEpoch) {
    constexpr size_t nodeCount = 40;

    auto graph = Graph::create("datetimegraph", fs::Path {_outDir} / "original");

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Event"});

            const int64_t daysFromEpoch = static_cast<int64_t>(i) - static_cast<int64_t>(nodeCount / 2);
            writer.addNodeProperty<types::DateTime>(
                node, "at", DateTime {daysFromEpoch * microsecondsPerDay});
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(DateTimeGraphDumpLoadTest, EdgeInstants) {
    constexpr size_t edgeCount = 50;

    auto graph = Graph::create("datetimegraph", fs::Path {_outDir} / "original");

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        const NodeID source = writer.addNode({"Person"});

        for (size_t i = 0; i < edgeCount; i++) {
            const NodeID target = writer.addNode({"Person"});
            const EdgeRecord edge = writer.addEdge("MET", source, target);

            writer.addEdgeProperty<types::DateTime>(
                edge, "at", DateTime {static_cast<int64_t>(i) * microsecondsPerMinute});
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

// An Int64 and a DateTime property of the same graph carry the same bytes under different
// tags, so a dump holding both is what would catch the loader reading one as the other
TEST_F(DateTimeGraphDumpLoadTest, InstantsBesideIntegersOfTheSameValue) {
    constexpr size_t nodeCount = 64;

    auto graph = Graph::create("datetimegraph", fs::Path {_outDir} / "original");

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Event"});
            const int64_t microseconds = static_cast<int64_t>(i) * microsecondsPerHour;

            writer.addNodeProperty<types::Int64>(node, "raw", int64_t {microseconds});
            writer.addNodeProperty<types::DateTime>(node, "at", DateTime {microseconds});
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}
