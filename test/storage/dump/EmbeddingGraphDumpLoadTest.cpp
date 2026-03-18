#include "TuringTest.h"

#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "Path.h"
#include "TuringException.h"

#include "Graph.h"
#include "comparators/GraphComparator.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
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

    // Build a graph with nodes carrying both scalar and embedding properties
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

    // Dump
    {
        GraphDumper dumper;
        auto res = dumper.dump(*graph, dumpPath);
        if (!res) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }
    }

    // Load
    auto loadedGraph = Graph::create();
    {
        const auto loadRes = GraphLoader::load(loadedGraph.get(), dumpPath);
        if (!loadRes) {
            throw TuringException("Failed to load graph:\n" + loadRes.error().fmtMessage());
        }
    }

    ASSERT_TRUE(GraphComparator::same(*graph, *loadedGraph));

    // Verify embedding values survive the round-trip
    const FrozenCommitTx tx = loadedGraph->openTransaction();
    const GraphReader reader = tx.readGraph();

    const auto& propTypes = tx.viewGraph().metadata().propTypes();
    const auto vecType = propTypes.get("vec");
    const auto idxType = propTypes.get("idx");
    ASSERT_TRUE(vecType.has_value());
    ASSERT_TRUE(idxType.has_value());
    ASSERT_EQ(vecType->_valueType, ValueType::Embedding);

    ColumnNodeIDs nodeIDs;
    for (size_t i = 0; i < nodeCount; i++) {
        nodeIDs.push_back(NodeID(i));
    }

    const auto vecRange = reader.getNodeProperties<types::Embedding>(vecType->_id, &nodeIDs);

    size_t total = 0;
    for (auto it = vecRange.begin(); it.isValid(); it.next()) {
        const NodeID nodeID = it.getCurrentEntityID();
        const auto view = it.get();
        ASSERT_EQ(view.size(), dimension);

        const auto* origIdx = reader.tryGetNodeProperty<types::Int64>(idxType->_id, nodeID);
        ASSERT_NE(origIdx, nullptr);
        const size_t i = static_cast<size_t>(*origIdx);

        for (size_t d = 0; d < dimension; d++) {
            ASSERT_EQ(view[d], static_cast<float>(i * dimension + d));
        }
        total++;
    }

    ASSERT_EQ(total, nodeCount);
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

        // Create edges between consecutive nodes with embedding properties
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

    // Dump
    {
        GraphDumper dumper;
        auto res = dumper.dump(*graph, dumpPath);
        if (!res) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }
    }

    // Load
    auto loadedGraph = Graph::create();
    {
        const auto loadRes = GraphLoader::load(loadedGraph.get(), dumpPath);
        if (!loadRes) {
            throw TuringException("Failed to load graph:\n" + loadRes.error().fmtMessage());
        }
    }

    ASSERT_TRUE(GraphComparator::same(*graph, *loadedGraph));

    // Verify node embeddings in the loaded graph
    const FrozenCommitTx tx = loadedGraph->openTransaction();
    const GraphReader reader = tx.readGraph();

    const auto& propTypes = tx.viewGraph().metadata().propTypes();
    const auto vecType = propTypes.get("vec");
    const auto idxType = propTypes.get("idx");
    ASSERT_TRUE(vecType.has_value());
    ASSERT_TRUE(idxType.has_value());

    ColumnNodeIDs nodeIDs;
    for (size_t i = 0; i < nodeCount; i++) {
        nodeIDs.push_back(NodeID(i));
    }

    const auto vecRange = reader.getNodeProperties<types::Embedding>(vecType->_id, &nodeIDs);

    size_t total = 0;
    for (auto it = vecRange.begin(); it.isValid(); it.next()) {
        const NodeID nodeID = it.getCurrentEntityID();
        const auto view = it.get();
        ASSERT_EQ(view.size(), dimension);

        const auto* origIdx = reader.tryGetNodeProperty<types::Int64>(idxType->_id, nodeID);
        ASSERT_NE(origIdx, nullptr);
        const size_t i = static_cast<size_t>(*origIdx);

        for (size_t d = 0; d < dimension; d++) {
            ASSERT_EQ(view[d], static_cast<float>(i * dimension + d));
        }
        total++;
    }

    ASSERT_EQ(total, nodeCount);
}

TEST_F(EmbeddingGraphDumpLoadTest, SparseEmbeddings) {
    const fs::Path dumpPath = fs::Path {_outDir} / "dump";
    const size_t dimension = 4;
    const size_t nodeCount = 100;

    // Build a graph where only even-indexed nodes have embeddings
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

    // Dump
    {
        GraphDumper dumper;
        auto res = dumper.dump(*graph, dumpPath);
        if (!res) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }
    }

    // Load
    auto loadedGraph = Graph::create();
    {
        const auto loadRes = GraphLoader::load(loadedGraph.get(), dumpPath);
        if (!loadRes) {
            throw TuringException("Failed to load graph:\n" + loadRes.error().fmtMessage());
        }
    }

    ASSERT_TRUE(GraphComparator::same(*graph, *loadedGraph));

    // Verify: even-indexed nodes have embeddings, odd-indexed do not
    const FrozenCommitTx tx = loadedGraph->openTransaction();
    const GraphReader reader = tx.readGraph();

    const auto& propTypes = tx.viewGraph().metadata().propTypes();
    const auto vecType = propTypes.get("vec");
    const auto idxType = propTypes.get("idx");
    ASSERT_TRUE(vecType.has_value());
    ASSERT_TRUE(idxType.has_value());

    ColumnNodeIDs nodeIDs;
    for (size_t i = 0; i < nodeCount; i++) {
        nodeIDs.push_back(NodeID(i));
    }

    const auto range = reader.getNodePropertiesWithNull<types::Embedding>(vecType->_id, &nodeIDs);

    size_t total = 0;
    for (auto it = range.begin(); it.isValid(); it.next()) {
        const NodeID nodeID = it.getCurrentID();
        const auto value = it.get();

        const auto* origIdx = reader.tryGetNodeProperty<types::Int64>(idxType->_id, nodeID);
        ASSERT_NE(origIdx, nullptr);
        const size_t i = static_cast<size_t>(*origIdx);

        if (i % 2 == 0) {
            ASSERT_TRUE(value.has_value());
            const auto view = value.value();
            ASSERT_EQ(view.size(), dimension);
            for (size_t d = 0; d < dimension; d++) {
                ASSERT_EQ(view[d], static_cast<float>(i * dimension + d));
            }
        } else {
            ASSERT_FALSE(value.has_value());
        }

        total++;
    }

    ASSERT_EQ(total, nodeCount);
}

TEST_F(EmbeddingGraphDumpLoadTest, MultiCommitEmbeddings) {
    const fs::Path dumpPath = fs::Path {_outDir} / "dump";
    const size_t dimension = 4;

    auto graph = Graph::create("multicommit", fs::Path {_outDir} / "original");
    {
        GraphWriter writer(graph.get());

        std::vector<float> embedding(dimension);

        // First commit: add some nodes with embeddings
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

        // Second commit: add more nodes with embeddings
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

    // Dump
    {
        GraphDumper dumper;
        auto res = dumper.dump(*graph, dumpPath);
        if (!res) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }
    }

    // Load
    auto loadedGraph = Graph::create();
    {
        const auto loadRes = GraphLoader::load(loadedGraph.get(), dumpPath);
        if (!loadRes) {
            throw TuringException("Failed to load graph:\n" + loadRes.error().fmtMessage());
        }
    }

    ASSERT_TRUE(GraphComparator::same(*graph, *loadedGraph));

    // Verify all 40 embeddings survive
    const FrozenCommitTx tx = loadedGraph->openTransaction();
    const GraphReader reader = tx.readGraph();

    const auto& propTypes = tx.viewGraph().metadata().propTypes();
    const auto vecType = propTypes.get("vec");
    const auto idxType = propTypes.get("idx");
    ASSERT_TRUE(vecType.has_value());
    ASSERT_TRUE(idxType.has_value());

    ColumnNodeIDs nodeIDs;
    for (size_t i = 0; i < 40; i++) {
        nodeIDs.push_back(NodeID(i));
    }

    const auto vecRange = reader.getNodeProperties<types::Embedding>(vecType->_id, &nodeIDs);

    size_t total = 0;
    for (auto it = vecRange.begin(); it.isValid(); it.next()) {
        const NodeID nodeID = it.getCurrentEntityID();
        const auto view = it.get();
        ASSERT_EQ(view.size(), dimension);

        const auto* origIdx = reader.tryGetNodeProperty<types::Int64>(idxType->_id, nodeID);
        ASSERT_NE(origIdx, nullptr);
        const size_t i = static_cast<size_t>(*origIdx);

        for (size_t d = 0; d < dimension; d++) {
            ASSERT_EQ(view[d], static_cast<float>(i * dimension + d));
        }
        total++;
    }

    ASSERT_EQ(total, 40);
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 3; });
}
