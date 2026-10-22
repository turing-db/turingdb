#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <optional>
#include <string_view>

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/ScanInEdgesByTargetLabelIterator.h"
#include "iterators/ScanOutEdgesBySourceLabelIterator.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

template <typename ChunkWriterType>
size_t countScannedEdges(const GraphView& view, const LabelSet& labelset) {
    ChunkWriterType chunkWriter(view, LabelSetHandle(labelset));

    ColumnNodeIDs sources;
    ColumnEdgeIDs edgeIDs;
    ColumnEdgeTypes edgeTypes;
    ColumnNodeIDs targets;
    chunkWriter.setSrcIDs(&sources);
    chunkWriter.setEdgeIDs(&edgeIDs);
    chunkWriter.setEdgeTypes(&edgeTypes);
    chunkWriter.setTgtIDs(&targets);

    size_t count = 0;
    while (chunkWriter.isValid()) {
        chunkWriter.fill(ChunkConfig::CHUNK_SIZE);
        count += edgeIDs.size();
    }

    return count;
}

}

// The edges a by-label edge index holds under a label set are the edges a scan of that index
// walks. A later part adds edges out of and into nodes of earlier ones, which its index
// holds as patch nodes, so the counts have to take those spans in as well.
class EdgeCountMatchingLabelsetTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        GraphWriter writer(_graph.get(), _jobSystem.get());
        const NodeID remy {0};
        const NodeID adam {1};
        const NodeID ghosts {6};
        const NodeID chess = writer.addNode({"Interest"});
        writer.addEdge("KNOWS_WELL", remy, adam);
        writer.addEdge("INTERESTED_IN", adam, ghosts);
        writer.addEdge("INTERESTED_IN", remy, chess);
        writer.submit();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void expectCountsMatchTheScans(std::initializer_list<std::string_view> names) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();
        ASSERT_GT(reader.getDatapartCount(), 1u);

        LabelSet labelset;
        for (const std::string_view name : names) {
            const std::optional<LabelID> label = reader.getMetadata().labels().get(name);
            ASSERT_TRUE(label.has_value());
            labelset.set(*label);
        }

        const LabelSetHandle handle(labelset);
        EXPECT_EQ(reader.getOutEdgeCountMatchingLabelset(handle),
                  countScannedEdges<ScanOutEdgesBySourceLabelChunkWriter>(view, labelset));
        EXPECT_EQ(reader.getInEdgeCountMatchingLabelset(handle),
                  countScannedEdges<ScanInEdgesByTargetLabelChunkWriter>(view, labelset));
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(EdgeCountMatchingLabelsetTest, countsTheEdgesOfOneLabel) {
    expectCountsMatchTheScans({"Person"});
    expectCountsMatchTheScans({"Interest"});
    expectCountsMatchTheScans({"Exotic"});
}

TEST_F(EdgeCountMatchingLabelsetTest, countsTheEdgesOfNodesCarryingEveryLabel) {
    expectCountsMatchTheScans({"Person", "Founder"});
    expectCountsMatchTheScans({"Interest", "Exotic"});
}
