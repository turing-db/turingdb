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
#include "metadata/PropertyNull.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

using ListItem = ListContainer::ListItemVariant;

}

// A list property survives a dump and a load: the encoding the dumper writes carries the
// whole value - elements, their tags, and the payloads a view alone would leave behind -
// so the loaded graph holds lists equal to the ones dumped rather than views into a
// container that no longer exists.
class ListGraphDumpLoadTest : public TuringTest {
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

TEST_F(ListGraphDumpLoadTest, NodeIntegerLists) {
    constexpr size_t nodeCount = 100;

    auto graph = Graph::create("listgraph", fs::Path {_outDir} / "original");
    ListContainer lists;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Item"});
            writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));

            const std::vector<ListItem> elements = {static_cast<int64_t>(i),
                                                    static_cast<int64_t>(i * 2),
                                                    static_cast<int64_t>(i * 3)};
            writer.addNodeProperty<types::List>(node, "tags", lists.insert(elements));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(ListGraphDumpLoadTest, HeterogeneousListsWithPayloads) {
    constexpr size_t nodeCount = 30;

    auto graph = Graph::create("listgraph", fs::Path {_outDir} / "original");
    ListContainer lists;
    std::vector<std::string> names;
    names.reserve(nodeCount);

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Item"});

            const std::string& name = names.emplace_back("item_" + std::to_string(i));
            const std::vector<ListItem> elements = {static_cast<int64_t>(i),
                                                    types::String::Primitive {name},
                                                    types::Bool::Primitive {i % 2 == 0},
                                                    static_cast<double>(i) / 2.0,
                                                    PropertyNull {}};

            writer.addNodeProperty<types::List>(node, "mixed", lists.insert(elements));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(ListGraphDumpLoadTest, NestedLists) {
    constexpr size_t nodeCount = 20;

    auto graph = Graph::create("listgraph", fs::Path {_outDir} / "original");
    ListContainer lists;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Item"});

            const std::vector<ListItem> inner = {static_cast<int64_t>(i), static_cast<int64_t>(i + 1)};
            const std::vector<ListItem> outer = {lists.insert(inner), static_cast<int64_t>(i)};

            writer.addNodeProperty<types::List>(node, "nested", lists.insert(outer));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(ListGraphDumpLoadTest, NodeAndEdgeLists) {
    constexpr size_t nodeCount = 50;

    auto graph = Graph::create("listgraph", fs::Path {_outDir} / "original");
    ListContainer lists;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        std::vector<NodeID> nodes;
        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Entity"});

            const std::vector<ListItem> elements = {static_cast<int64_t>(i)};
            writer.addNodeProperty<types::List>(node, "tags", lists.insert(elements));
            nodes.push_back(node);
        }

        for (size_t i = 0; i + 1 < nodeCount; i++) {
            const EdgeRecord edge = writer.addEdge("LINKS", nodes[i], nodes[i + 1]);

            const std::vector<ListItem> elements = {static_cast<int64_t>(i),
                                                    static_cast<int64_t>(i + 1)};
            writer.addEdgeProperty<types::List>(edge, "weights", lists.insert(elements));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(ListGraphDumpLoadTest, SparseAndEmptyLists) {
    constexpr size_t nodeCount = 60;

    auto graph = Graph::create("listgraph", fs::Path {_outDir} / "original");
    ListContainer lists;

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

            const std::vector<ListItem> elements =
                i % 3 == 1 ? std::vector<ListItem> {} : std::vector<ListItem> {static_cast<int64_t>(i)};

            writer.addNodeProperty<types::List>(node, "tags", lists.insert(elements));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}

TEST_F(ListGraphDumpLoadTest, ListsSpanningManyPages) {
    constexpr size_t nodeCount = 40;
    constexpr size_t elementCount = 400;

    auto graph = Graph::create("listgraph", fs::Path {_outDir} / "original");
    ListContainer lists;

    {
        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(graph.get(), &jobSystem);

        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Item"});

            std::vector<ListItem> elements;
            elements.reserve(elementCount);
            for (size_t element = 0; element < elementCount; element++) {
                elements.push_back(static_cast<int64_t>(i * elementCount + element));
            }

            writer.addNodeProperty<types::List>(node, "tags", lists.insert(elements));
        }

        writer.submit();
    }

    dumpAndCompare(graph.get());
}
