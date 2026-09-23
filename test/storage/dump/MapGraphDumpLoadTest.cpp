#include "TuringTest.h"

#include <string>
#include <vector>

#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "Path.h"
#include "TuringException.h"

#include "Graph.h"
#include "comparators/GraphComparator.h"
#include "list/ListContainer.h"
#include "list/ListView.h"
#include "map/MapContainer.h"
#include "map/MapView.h"
#include "metadata/PropertyNull.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

using MapEntry = MapContainer::MapKeyValuePair;
using ListItem = ListContainer::ListItemVariant;

}

class MapGraphDumpLoadTest : public TuringTest {
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

TEST_F(MapGraphDumpLoadTest, NodeScalarMaps) {
    constexpr size_t nodeCount = 100;

    auto graph = Graph::create("mapgraph", fs::Path {_outDir} / "original");
    MapContainer maps;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Item"});
            writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));

            const std::vector<MapEntry> entries = {{"a", static_cast<int64_t>(i)},
                                                   {"b", static_cast<double>(i) / 2.0},
                                                   {"c", types::Bool::Primitive {i % 2 == 0}}};
            writer.addNodeProperty<types::Map>(node, "attrs", maps.insert(entries));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(MapGraphDumpLoadTest, MapsWithStringPayloads) {
    constexpr size_t nodeCount = 30;

    auto graph = Graph::create("mapgraph", fs::Path {_outDir} / "original");
    MapContainer maps;
    std::vector<std::string> names;
    names.reserve(nodeCount);

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Item"});

            const std::string& name = names.emplace_back("item_" + std::to_string(i));
            const std::vector<MapEntry> entries = {{"name", types::String::Primitive {name}},
                                                   {"none", PropertyNull {}}};

            writer.addNodeProperty<types::Map>(node, "attrs", maps.insert(entries));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(MapGraphDumpLoadTest, NestedMapsAndListValues) {
    constexpr size_t nodeCount = 20;

    auto graph = Graph::create("mapgraph", fs::Path {_outDir} / "original");
    MapContainer maps;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Item"});

            const std::vector<ListItem> innerList = {static_cast<int64_t>(i)};
            const std::vector<ListItem> outerList = {maps.getLists().insert(innerList),
                                                     static_cast<int64_t>(i + 1)};
            const ListView list = maps.getLists().insert(outerList);

            const std::vector<MapEntry> innerEntries = {{"deep", static_cast<int64_t>(i)}};
            const MapView inner = maps.insert(innerEntries);

            const std::vector<MapEntry> entries = {{"inner", inner}, {"list", list}};
            writer.addNodeProperty<types::Map>(node, "attrs", maps.insert(entries));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(MapGraphDumpLoadTest, NodeAndEdgeMaps) {
    constexpr size_t nodeCount = 50;

    auto graph = Graph::create("mapgraph", fs::Path {_outDir} / "original");
    MapContainer maps;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        std::vector<NodeID> nodes;
        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Entity"});

            const std::vector<MapEntry> entries = {{"n", static_cast<int64_t>(i)}};
            writer.addNodeProperty<types::Map>(node, "attrs", maps.insert(entries));
            nodes.push_back(node);
        }

        for (size_t i = 0; i + 1 < nodeCount; i++) {
            const EdgeRecord edge = writer.addEdge("LINKS", nodes[i], nodes[i + 1]);

            const std::vector<MapEntry> entries = {{"from", static_cast<int64_t>(i)},
                                                   {"to", static_cast<int64_t>(i + 1)}};
            writer.addEdgeProperty<types::Map>(edge, "attrs", maps.insert(entries));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(MapGraphDumpLoadTest, SparseAndEmptyMaps) {
    constexpr size_t nodeCount = 60;

    auto graph = Graph::create("mapgraph", fs::Path {_outDir} / "original");
    MapContainer maps;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Item"});
            writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));

            if (i % 3 == 0) {
                continue;
            }

            const std::vector<MapEntry> entries =
                i % 3 == 1 ? std::vector<MapEntry> {} : std::vector<MapEntry> {{"n", static_cast<int64_t>(i)}};

            writer.addNodeProperty<types::Map>(node, "attrs", maps.insert(entries));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(MapGraphDumpLoadTest, MapsSpanningManyPages) {
    constexpr size_t nodeCount = 40;
    constexpr size_t entryCount = 400;

    auto graph = Graph::create("mapgraph", fs::Path {_outDir} / "original");
    MapContainer maps;
    std::vector<std::string> keys;
    keys.reserve(entryCount);
    for (size_t entry = 0; entry < entryCount; entry++) {
        keys.push_back("key_" + std::to_string(entry));
    }

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Item"});

            std::vector<MapEntry> entries;
            entries.reserve(entryCount);
            for (size_t entry = 0; entry < entryCount; entry++) {
                entries.push_back({keys[entry], static_cast<int64_t>(i * entryCount + entry)});
            }

            writer.addNodeProperty<types::Map>(node, "attrs", maps.insert(entries));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}
