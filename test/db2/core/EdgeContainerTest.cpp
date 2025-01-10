#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include "GraphMetadata.h"
#include "EdgeContainer.h"
#include "EntityID.h"

using namespace db;

class EdgeContainerTest : public ::testing::Test {
protected:
    void SetUp() override {
    }

    void TearDown() override {
    }

    GraphMetadata _metadata;
};

TEST_F(EdgeContainerTest, General) {
    const EntityID firstNodeID = 0;
    const EntityID firstEdgeID = 0;

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

    {
        std::vector<EdgeRecord> compareSet = {
            {0, 1, 2, 0},
            {1, 1, 8, 0},
            {2, 3, 4, 0},
            {3, 4, 3, 0},
            {4, 6, 7, 0},
        };

        auto it = compareSet.begin();
        for (const EdgeRecord& out : edges->getOuts()) {
            spdlog::info("[{}: {}->{}]", out._edgeID, out._nodeID, out._otherID);
            ASSERT_EQ(it->_edgeID, out._edgeID);
            ASSERT_EQ(it->_nodeID, out._nodeID);
            ASSERT_EQ(it->_otherID, out._otherID);
            ++it;
        }
    }

    {
        std::vector<EdgeRecord> compareSet = {
            {0, 1, 2, 0},
            {3, 4, 3, 0},
            {2, 3, 4, 0},
            {4, 6, 7, 0},
            {1, 1, 8, 0},
        };

        auto it = compareSet.begin();
        for (const EdgeRecord& in : edges->getIns()) {
            spdlog::info("[{}: {}->{}]", in._edgeID, in._nodeID, in._otherID);
            ASSERT_EQ(it->_edgeID, in._edgeID);
            ASSERT_EQ(it->_nodeID, in._otherID);
            ASSERT_EQ(it->_otherID, in._nodeID);
            ++it;
        }
    }
}

TEST_F(EdgeContainerTest, IDShift) {
    const EntityID firstNodeID = 10;
    const EntityID firstEdgeID = 10;

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

    {
        std::vector<EdgeRecord> compareSet = {
            {10, 1, 2, 0},
            {11, 1, 8, 0},
            {12, 3, 4, 0},
            {13, 4, 3, 0},
            {14, 6, 7, 0},
        };

        auto it = compareSet.begin();
        for (const EdgeRecord& out : edges->getOuts()) {
            spdlog::info("[{}: {}->{}]", out._edgeID, out._nodeID, out._otherID);
            ASSERT_EQ(it->_edgeID, out._edgeID);
            ASSERT_EQ(it->_nodeID, out._nodeID);
            ASSERT_EQ(it->_otherID, out._otherID);
            ++it;
        }
    }

    {
        std::vector<EdgeRecord> compareSet = {
            {10, 1, 2, 0},
            {13, 4, 3, 0},
            {12, 3, 4, 0},
            {14, 6, 7, 0},
            {11, 1, 8, 0},
        };

        auto it = compareSet.begin();
        for (const EdgeRecord& in : edges->getIns()) {
            spdlog::info("[{}: {}->{}]", in._edgeID, in._nodeID, in._otherID);
            ASSERT_EQ(it->_edgeID, in._edgeID);
            ASSERT_EQ(it->_nodeID, in._otherID);
            ASSERT_EQ(it->_otherID, in._nodeID);
            ++it;
        }
    }
}
