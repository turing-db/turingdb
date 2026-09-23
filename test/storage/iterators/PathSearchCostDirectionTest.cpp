#include <memory>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "iterators/PartDirectory.h"
#include "iterators/PathDistanceIndex.h"
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

// The index of an exploration is searched from the ends against the walk, so its cost is the
// cost of the reverse traversal. On a graph that fans out along its out-edges the two
// directions are worth very different amounts, and the estimate has to price the one the
// search actually takes.
//
// The graph: ten A nodes, each with an edge to every one of ten B nodes, and each B with an
// edge to ten C nodes of its own. Out-edges spread - A reaches ten B, which reach a hundred
// C - while in-edges converge - the hundred C come from ten B, which come from ten A.
class PathSearchCostDirectionTest : public TuringTest {
protected:
    static constexpr size_t layerSize = 10;
    static constexpr size_t sinksPerNode = 10;

    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        const EdgeTypeID type = metadata.getOrCreateEdgeType("A");

        std::vector<NodeID> sources;
        for (size_t node = 0; node < layerSize; node++) {
            sources.push_back(builder.addNode(plain));
        }

        std::vector<NodeID> middle;
        for (size_t node = 0; node < layerSize; node++) {
            middle.push_back(builder.addNode(plain));
        }

        for (const NodeID source : sources) {
            for (const NodeID hub : middle) {
                builder.addEdge(type, source, hub);
            }
        }

        for (const NodeID hub : middle) {
            for (size_t sink = 0; sink < sinksPerNode; sink++) {
                builder.addEdge(type, hub, builder.addNode(plain));
            }
        }

        const auto submitted = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitted);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(PathSearchCostDirectionTest, aForwardWalkIsPricedByTheBackwardSearchItDrives) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const PartDirectory parts(reader.getView());

    const std::span<const EdgeTypeID> everyType;
    constexpr uint64_t maxHops = 4;

    const double forwardWalk = PathDistanceIndex::estimatedSearchChecks(parts, PathExplorationDir::FORWARD, everyType, 1, maxHops);
    const double backwardWalk = PathDistanceIndex::estimatedSearchChecks(parts, PathExplorationDir::BACKWARD, everyType, 1, maxHops);

    // A forward walk's index is searched along in-edges, which converge on the ten A nodes;
    // a backward walk's is searched along out-edges, which spread over the hundred C nodes
    EXPECT_LT(forwardWalk, backwardWalk);
}
