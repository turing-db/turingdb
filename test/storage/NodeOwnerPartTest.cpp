#include <gtest/gtest.h>

#include <vector>

#include "TuringTest.h"

#include "datapart/NodeOwnerPart.h"
#include "ID.h"

using namespace db;
using namespace turing::test;

class NodeOwnerPartTest : public TuringTest {
protected:
    static size_t ownerOf(const std::vector<NodeID>& firstNodeIDs, NodeID::Type node) {
        return findNodeOwnerPart(firstNodeIDs, NodeID {node});
    }
};

TEST_F(NodeOwnerPartTest, findsThePartWhoseRangeHoldsTheNode) {
    const std::vector<NodeID> firstNodeIDs {NodeID {0}, NodeID {10}, NodeID {25}};

    EXPECT_EQ(ownerOf(firstNodeIDs, 0), 0u);
    EXPECT_EQ(ownerOf(firstNodeIDs, 9), 0u);
    EXPECT_EQ(ownerOf(firstNodeIDs, 10), 1u);
    EXPECT_EQ(ownerOf(firstNodeIDs, 24), 1u);
    EXPECT_EQ(ownerOf(firstNodeIDs, 25), 2u);
    EXPECT_EQ(ownerOf(firstNodeIDs, 1000), 2u);
}

TEST_F(NodeOwnerPartTest, findsNoPartBeforeTheFirst) {
    const std::vector<NodeID> firstNodeIDs {NodeID {5}, NodeID {10}};

    EXPECT_EQ(ownerOf(firstNodeIDs, 4), firstNodeIDs.size());
    EXPECT_EQ(ownerOf({}, 0), 0u);
}

TEST_F(NodeOwnerPartTest, skipsAPartWithNoNodes) {
    const std::vector<NodeID> firstNodeIDs {NodeID {0}, NodeID {10}, NodeID {10}, NodeID {20}};

    EXPECT_EQ(ownerOf(firstNodeIDs, 10), 2u);
    EXPECT_EQ(ownerOf(firstNodeIDs, 19), 2u);
}
