#include "TuringTest.h"

#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "Path.h"
#include "TuringException.h"

#include "Graph.h"
#include "comparators/GraphComparator.h"
#include "writers/GraphWriter.h"

using namespace db;
using namespace turing::test;

class EmbeddingGraphDumpLoadTest : public TuringTest {
protected:
    void initialize() override {
    }
};

TEST_F(EmbeddingGraphDumpLoadTest, NodeEmbeddings) {
    const fs::Path dumpPath = fs::Path {_outDir} / "dump";
    const size_t dimension = 4;
    const size_t nodeCount = 100;

    auto graph = Graph::create("embgraph", fs::Path {_outDir} / "original");
    {
        GraphWriter writer(graph.get());

        std::vector<float> embedding(dimension);
        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Item"});
            writer.addNodeProperty<types::String>(node, "name",
                                                  std::string_view("item_" + std::to_string(i)));
            writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));

            for (size_t d = 0; d < dimension; d++) {
                embedding[d] = static_cast<float>(i * dimension + d);
            }
            writer.addNodeProperty<types::Embedding>(node, "vec",
                                                     std::span<const float>(embedding));
        }
        writer.submit();
    }

    GraphDumper dumper;
    auto res = dumper.dump(*graph, dumpPath);
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

TEST_F(EmbeddingGraphDumpLoadTest, NodeAndEdgeEmbeddings) {
    const fs::Path dumpPath = fs::Path {_outDir} / "dump";
    const size_t dimension = 3;
    const size_t nodeCount = 50;

    auto graph = Graph::create("embgraph2", fs::Path {_outDir} / "original");
    {
        GraphWriter writer(graph.get());

        std::vector<float> emb(dimension);
        std::vector<NodeID> nodes;
        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Entity"});
            writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));

            for (size_t d = 0; d < dimension; d++) {
                emb[d] = static_cast<float>(i * dimension + d);
            }
            writer.addNodeProperty<types::Embedding>(node, "vec",
                                                     std::span<const float>(emb));
            nodes.push_back(node);
        }

        for (size_t i = 0; i + 1 < nodeCount; i++) {
            const auto edge = writer.addEdge("LINKS", nodes[i], nodes[i + 1]);
            for (size_t d = 0; d < dimension; d++) {
                emb[d] = static_cast<float>((i + nodeCount) * dimension + d);
            }
            writer.addEdgeProperty<types::Embedding>(edge, "edge_vec",
                                                     std::span<const float>(emb));
        }

        writer.submit();
    }

    GraphDumper dumper;
    auto res = dumper.dump(*graph, dumpPath);
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

TEST_F(EmbeddingGraphDumpLoadTest, SparseEmbeddings) {
    const fs::Path dumpPath = fs::Path {_outDir} / "dump";
    const size_t dimension = 4;
    const size_t nodeCount = 100;

    auto graph = Graph::create("sparse", fs::Path {_outDir} / "original");
    {
        GraphWriter writer(graph.get());

        std::vector<float> embedding(dimension);
        for (size_t i = 0; i < nodeCount; i++) {
            const NodeID node = writer.addNode({"Node"});
            writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));

            if (i % 2 == 0) {
                for (size_t d = 0; d < dimension; d++) {
                    embedding[d] = static_cast<float>(i * dimension + d);
                }
                writer.addNodeProperty<types::Embedding>(node, "vec",
                                                         std::span<const float>(embedding));
            }
        }
        writer.submit();
    }

    GraphDumper dumper;
    auto res = dumper.dump(*graph, dumpPath);
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

TEST_F(EmbeddingGraphDumpLoadTest, MultiCommitEmbeddings) {
    const fs::Path dumpPath = fs::Path {_outDir} / "dump";
    const size_t dimension = 4;

    auto graph = Graph::create("multicommit", fs::Path {_outDir} / "original");
    {
        GraphWriter writer(graph.get());

        std::vector<float> embedding(dimension);

        for (size_t i = 0; i < 20; i++) {
            const NodeID node = writer.addNode({"Item"});
            writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));
            for (size_t d = 0; d < dimension; d++) {
                embedding[d] = static_cast<float>(i * dimension + d);
            }
            writer.addNodeProperty<types::Embedding>(node, "vec",
                                                     std::span<const float>(embedding));
        }
        writer.commit();

        for (size_t i = 20; i < 40; i++) {
            const NodeID node = writer.addNode({"Item"});
            writer.addNodeProperty<types::Int64>(node, "idx", static_cast<int64_t>(i));
            for (size_t d = 0; d < dimension; d++) {
                embedding[d] = static_cast<float>(i * dimension + d);
            }
            writer.addNodeProperty<types::Embedding>(node, "vec",
                                                     std::span<const float>(embedding));
        }
        writer.submit();
    }

    GraphDumper dumper;
    auto res = dumper.dump(*graph, dumpPath);
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

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 1; });
}
