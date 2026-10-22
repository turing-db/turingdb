#include <gtest/gtest.h>

#include <stddef.h>

#include <initializer_list>
#include <memory>
#include <optional>
#include <string_view>

#include "Graph.h"
#include "JobSystem.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "reader/GraphReader.h"
#include "versioning/Tombstones.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

// Deleted nodes are left out of the count of the nodes matching a label set. The 7 deleted
// Interest nodes are fewer than the 10 Interest nodes and more than the 2 Exotic ones, so
// the counts below take both ways of leaving them out: checking each matched node against
// the deletions, and checking each deletion against the labels.
class NodeCountMatchingLabelsetTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        const NodeID eighties {3};
        const NodeID bio {4};
        const NodeID cooking {5};
        const NodeID padel {7};
        const NodeID gym {13};
        const NodeID travel {14};
        const NodeID jiuJitsu {16};

        {
            GraphWriter writer(_graph.get(), _jobSystem.get());
            writer.deleteNodes({eighties, bio, cooking, padel});
            writer.submit();
        }

        {
            GraphWriter writer(_graph.get(), _jobSystem.get());
            writer.deleteNodes({gym, travel, jiuJitsu});
            writer.submit();
        }
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void expectCount(std::initializer_list<std::string_view> names, size_t expectedCount) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();
        const Tombstones& tombstones = view.tombstones();
        ASSERT_EQ(tombstones.numNodes(), 7u);

        LabelSet labelset;
        for (const std::string_view name : names) {
            const std::optional<LabelID> label = reader.getMetadata().labels().get(name);
            ASSERT_TRUE(label.has_value());
            labelset.set(*label);
        }

        const LabelSetHandle handle(labelset);

        size_t matchingNodes = 0;
        for (size_t id = 0; id < reader.getTotalNodesAllocated(); id++) {
            const NodeID nodeID {id};
            const LabelSetHandle nodeLabels = reader.getNodeLabelSet(nodeID);
            if (nodeLabels.isValid() && nodeLabels.hasAtLeastLabels(handle) && !tombstones.containsNode(nodeID)) {
                matchingNodes++;
            }
        }

        EXPECT_EQ(matchingNodes, expectedCount);
        EXPECT_EQ(reader.getNodeCountMatchingLabelset(handle), expectedCount);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(NodeCountMatchingLabelsetTest, leavesOutDeletedNodesFewerThanTheMatchedOnes) {
    expectCount({"Interest"}, 3);
    expectCount({"Person"}, 8);
    expectCount({}, 11);
}

TEST_F(NodeCountMatchingLabelsetTest, leavesOutDeletedNodesMoreThanTheMatchedOnes) {
    expectCount({"Exotic"}, 1);
    expectCount({"Interest", "SleepDisturber"}, 2);
    expectCount({"Founder"}, 2);
}
