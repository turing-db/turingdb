#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include "DBMetadata.h"
#include "EdgeContainer.h"
#include "EdgeIndexer.h"
#include "EntityID.h"
#include "NodeContainer.h"

using namespace db;

class EdgeIndexerTest : public ::testing::Test {
protected:
    void SetUp() override {
    }

    void TearDown() override {
    }

    DBMetadata _metadata;
};

TEST_F(EdgeIndexerTest, General) {
    const EntityID firstNodeID = 0;
    const EntityID firstEdgeID = 0;

    DBMetadata _metadata;
    std::vector<LabelSetID> nodeLabelSets = {0, 0, 1, 1, 1, 2, 3, 3, 3, 4, 4, 5};
    auto nodes = NodeContainer::create(0,
                                       &_metadata,
                                       nodeLabelSets);

    std::map<EntityID, LabelSetID> patchNodeLabelSets;

    std::vector<EdgeRecord> outEdges = {
        {0, 1, 2, 0},
        {1, 3, 4, 0},
        {2, 4, 3, 0},
        {3, 1, 8, 0},
        {4, 6, 7, 0},
    };

    auto edges = EdgeContainer::create(firstNodeID,
                                       firstEdgeID,
                                       std::move(outEdges));

    auto indexer = EdgeIndexer::create(*edges,
                                       *nodes,
                                       0,
                                       patchNodeLabelSets,
                                       3,
                                       0,
                                       0,
                                       _metadata);

    {
        std::vector<EdgeRecord> compareSet = {
            {3, 4, 3, 0},
        };

        auto it = compareSet.begin();
        for (const auto& in : indexer->getNodeInEdges({3})) {
            spdlog::info("[{}: {}->{}]", in._edgeID, in._otherID, in._nodeID);
            ASSERT_EQ(it->_edgeID, in._edgeID);
            ASSERT_EQ(it->_nodeID, in._otherID);
            ASSERT_EQ(it->_otherID, in._nodeID);
            ++it;
        }
    }
}
