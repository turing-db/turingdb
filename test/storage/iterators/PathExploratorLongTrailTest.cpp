#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "PathExplorationReference.h"
#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/PathExplorationDir.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

void distinctPairs(const std::vector<PathRow>& rows, std::vector<PathRow>& pairs) {
    pairs.clear();
    for (const PathRow& row : rows) {
        pairs.push_back({row._index, row._target, {}});
    }

    std::sort(pairs.begin(), pairs.end());
    pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
}

}

// Trails longer than the 64 bits of the path signature, whose every candidate the signature
// cannot rule out: the check that an edge is on the path then decides each of them
class PathExploratorLongTrailTest : public TuringTest {
protected:
    static constexpr size_t ringSize = 90;
    static constexpr size_t chordEvery = 9;
    static constexpr size_t chordSpan = 4;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            const LabelSet labels = LabelSet::fromList({metadata.getOrCreateLabel("N")});
            for (size_t node = 0; node < ringSize; node++) {
                builder.addNode(labels);
            }

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        {
            auto change = _graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            const EdgeTypeID type = builder.getMetadata().getOrCreateEdgeType("A");

            for (size_t node = 0; node < ringSize; node++) {
                builder.addEdge(type, NodeID(node), NodeID((node + 1) % ringSize));
                if (node % chordEvery == 0) {
                    builder.addEdge(type, NodeID(node), NodeID((node + chordSpan) % ringSize));
                }
            }

            const auto submitted = change->access().submit(*_jobSystem);
            ASSERT_TRUE(submitted);
        }

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        buildAdjacency(reader.getView(), ringSize, _adjacency);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    Adjacency _adjacency;
};

TEST_F(PathExploratorLongTrailTest, trailsPastTheSignatureWidthAgreeWithTheReference) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    ColumnNodeIDs input;
    for (const size_t seed : {size_t {0}, size_t {5}, size_t {37}}) {
        input.push_back(NodeID(seed));
    }

    for (const PathExplorationDir direction : {PathExplorationDir::FORWARD, PathExplorationDir::BACKWARD, PathExplorationDir::BOTH}) {
        for (const uint64_t hops : {uint64_t {70}, uint64_t {88}}) {
            SCOPED_TRACE("direction " + std::to_string(static_cast<int>(direction)) + " hops " + std::to_string(hops));

            ReferenceEnumerator reference(_adjacency, direction, hops, hops);
            std::vector<PathRow> expected;
            reference.enumerate(input, expected);
            EXPECT_FALSE(expected.empty());

            std::vector<PathRow> walked;
            collectPaths(view, input, direction, hops, hops, ExplorationOptions {}, walked);
            expectSameRows(expected, walked);

            std::vector<PathRow> expectedEnds;
            distinctPairs(expected, expectedEnds);

            ExplorationOptions distinct;
            distinct._distinctEnds = true;
            distinct._collectPaths = false;

            std::vector<PathRow> ends;
            collectPaths(view, input, direction, hops, hops, distinct, ends);
            expectSameRows(expectedEnds, ends);
        }
    }
}
