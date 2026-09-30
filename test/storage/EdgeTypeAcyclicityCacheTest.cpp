#include <gtest/gtest.h>

#include <memory>
#include <string_view>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SimpleGraph.h"
#include "metadata/EdgeTypeAcyclicityCache.h"
#include "metadata/EdgeTypeMap.h"
#include "metadata/GraphMetadata.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"

using namespace db;
using namespace turing::test;

// simpledb sorted by type: INTERESTED_IN runs from people to interests and never back,
// KNOWS_WELL closes on Remy and Adam, and the two together close on Remy through Ghosts
class EdgeTypeAcyclicityCacheTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void writeEdge(std::string_view edgeType, NodeID source, NodeID target) {
        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();

        const EdgeTypeID type = builder.getMetadata().getOrCreateEdgeType(edgeType);
        builder.addEdge(type, source, target);

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    static std::vector<EdgeTypeID> types(const GraphView& view, std::initializer_list<std::string_view> names) {
        std::vector<EdgeTypeID> typeIDs;
        for (const std::string_view name : names) {
            typeIDs.push_back(view.metadata().edgeTypes().get(name).value());
        }

        return typeIDs;
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(EdgeTypeAcyclicityCacheTest, sortsEachTypeSetOnItsOwn) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    EXPECT_TRUE(view.isAcyclicOver(types(view, {"INTERESTED_IN"})));
    EXPECT_FALSE(view.isAcyclicOver(types(view, {"KNOWS_WELL"})));
    EXPECT_FALSE(view.isAcyclicOver(types(view, {"INTERESTED_IN", "KNOWS_WELL"})));
    EXPECT_FALSE(view.isAcyclicOver(types(view, {"KNOWS_WELL", "INTERESTED_IN"})));
    EXPECT_TRUE(view.isAcyclicOver(types(view, {})));
}

TEST_F(EdgeTypeAcyclicityCacheTest, sortsASelfLoopAsACycle) {
    writeEdge("LOOPS", NodeID {0}, NodeID {0});

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    EXPECT_FALSE(view.isAcyclicOver(types(view, {"LOOPS"})));
    EXPECT_TRUE(view.isAcyclicOver(types(view, {"INTERESTED_IN"})));
}

// Remy is interested in Ghosts; a commit making Ghosts interested in Remy closes
// INTERESTED_IN in the views holding the new part, and an entry sorted on fewer parts than
// a view holds is sorted again for it
TEST_F(EdgeTypeAcyclicityCacheTest, sortsAgainForAViewHoldingMoreParts) {
    const FrozenCommitTx before = _graph->openTransaction();
    const GraphView viewBefore = before.viewGraph();
    const std::vector<EdgeTypeID> interestedIn = types(viewBefore, {"INTERESTED_IN"});

    EdgeTypeAcyclicityCache cache;
    EXPECT_TRUE(cache.isAcyclic(viewBefore.dataparts(), interestedIn));

    writeEdge("INTERESTED_IN", NodeID {6}, NodeID {0});

    const FrozenCommitTx after = _graph->openTransaction();
    const GraphView viewAfter = after.viewGraph();
    ASSERT_GT(viewAfter.dataparts().size(), viewBefore.dataparts().size());

    EXPECT_FALSE(cache.isAcyclic(viewAfter.dataparts(), interestedIn));
    EXPECT_TRUE(cache.isAcyclic(viewBefore.dataparts(), interestedIn));

    EXPECT_FALSE(viewAfter.isAcyclicOver(interestedIn));
    EXPECT_TRUE(viewBefore.isAcyclicOver(interestedIn));
}
