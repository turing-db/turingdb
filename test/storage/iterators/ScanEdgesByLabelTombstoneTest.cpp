#include <stddef.h>

#include <memory>
#include <optional>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/ScanInEdgesByTargetLabelIterator.h"
#include "iterators/ScanOutEdgesBySourceLabelIterator.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"
#include "SimpleGraph.h"

using namespace db;
using namespace turing::test;

namespace {

// The four columns one fill of a by-label edge scan wrote.
struct ScannedChunk {
    ColumnNodeIDs _sources;
    ColumnEdgeIDs _edgeIDs;
    ColumnEdgeTypes _edgeTypes;
    ColumnNodeIDs _targets;
};

template <typename ChunkWriterType>
void fillChunk(ChunkWriterType& chunkWriter, ScannedChunk& chunk) {
    chunkWriter.setSrcIDs(&chunk._sources);
    chunkWriter.setEdgeIDs(&chunk._edgeIDs);
    chunkWriter.setEdgeTypes(&chunk._edgeTypes);
    chunkWriter.setTgtIDs(&chunk._targets);

    chunkWriter.fill(ChunkConfig::CHUNK_SIZE);
}

bool holdsEdge(const ColumnEdgeIDs& edgeIDs, EdgeID edgeID) {
    for (size_t row = 0; row < edgeIDs.size(); row++) {
        if (edgeIDs[row] == edgeID) {
            return true;
        }
    }

    return false;
}

}

// Deleting an edge leaves a tombstone the chunk writers filter out. Every column a writer
// fills has to lose the same rows, or the four stop being row-aligned and a reader pairs an
// edge with another edge's endpoints.
class ScanEdgesByLabelTombstoneTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void personLabelSet(LabelSet& labelset) const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        const std::optional<LabelID> person = reader.getMetadata().labels().get("Person");
        ASSERT_TRUE(person.has_value());

        labelset.set(*person);
    }

    // The first edge a Person scan reports, which both directions then have to drop once it
    // is deleted: it runs between two Person nodes.
    EdgeID firstPersonToPersonEdge(const LabelSet& labelset) const {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        ScannedChunk chunk;
        ScanOutEdgesBySourceLabelChunkWriter chunkWriter(reader.getView(), LabelSetHandle(labelset));
        fillChunk(chunkWriter, chunk);

        for (size_t row = 0; row < chunk._edgeIDs.size(); row++) {
            const LabelSetHandle targetLabels = reader.getNodeLabelSet(chunk._targets[row]);
            if (targetLabels.isValid() && targetLabels.hasAtLeastLabels(LabelSetHandle(labelset))) {
                return chunk._edgeIDs[row];
            }
        }

        return EdgeID {};
    }

    void deleteEdge(EdgeID edgeID) {
        GraphWriter writer(_graph.get(), _jobSystem.get());
        writer.deleteEdge(edgeID);
        writer.submit();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(ScanEdgesByLabelTombstoneTest, theOutScanDropsADeletedEdgeFromEveryColumn) {
    LabelSet labelset;
    personLabelSet(labelset);

    const EdgeID deleted = firstPersonToPersonEdge(labelset);
    deleteEdge(deleted);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ScannedChunk chunk;
    ScanOutEdgesBySourceLabelChunkWriter chunkWriter(reader.getView(), LabelSetHandle(labelset));
    fillChunk(chunkWriter, chunk);

    EXPECT_FALSE(holdsEdge(chunk._edgeIDs, deleted));
    EXPECT_EQ(chunk._sources.size(), chunk._edgeIDs.size());
    EXPECT_EQ(chunk._edgeTypes.size(), chunk._edgeIDs.size());
    EXPECT_EQ(chunk._targets.size(), chunk._edgeIDs.size());
}

TEST_F(ScanEdgesByLabelTombstoneTest, theInScanDropsADeletedEdgeFromEveryColumn) {
    LabelSet labelset;
    personLabelSet(labelset);

    const EdgeID deleted = firstPersonToPersonEdge(labelset);
    deleteEdge(deleted);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ScannedChunk chunk;
    ScanInEdgesByTargetLabelChunkWriter chunkWriter(reader.getView(), LabelSetHandle(labelset));
    fillChunk(chunkWriter, chunk);

    EXPECT_FALSE(holdsEdge(chunk._edgeIDs, deleted));
    EXPECT_EQ(chunk._sources.size(), chunk._edgeIDs.size());
    EXPECT_EQ(chunk._edgeTypes.size(), chunk._edgeIDs.size());
    EXPECT_EQ(chunk._targets.size(), chunk._edgeIDs.size());
}
