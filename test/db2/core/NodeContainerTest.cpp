#include <gtest/gtest.h>

#include "GraphMetadata.h"
#include "EntityID.h"
#include "NodeContainer.h"

using namespace db;

class NodeContainerTest : public ::testing::Test {
protected:
    void SetUp() override {
    }

    void TearDown() override {
    }

    GraphMetadata _metadata;
};

TEST_F(NodeContainerTest, UnsortedFail) {
    std::vector<LabelSetID> nodeLabelSets = {0, 4, 2, 4, 0, 2, 2, 4};

    auto container = NodeContainer::create(0, _metadata, nodeLabelSets);
    ASSERT_TRUE(container == nullptr);
}

TEST_F(NodeContainerTest, SortedSuccess) {
    std::vector<LabelSetID> nodeLabelSets = {0, 0, 1, 1, 1, 2, 3};

    auto container = NodeContainer::create(0, _metadata, nodeLabelSets);
    ASSERT_TRUE(container != nullptr);
}

TEST_F(NodeContainerTest, IterateAll) {
    std::vector<LabelSetID> nodeLabelSets = {0, 0, 1, 1, 1, 2, 3};
    std::vector<EntityID> theoNodeIDs= {0, 1, 2, 3, 4, 5, 6};

    auto container = NodeContainer::create(0, _metadata, nodeLabelSets);
    ASSERT_TRUE(container != nullptr);

    auto theoIt = theoNodeIDs.cbegin();
    for (EntityID nodeID : container->getAll()) {
        ASSERT_EQ(*theoIt, nodeID);
        theoIt++;
    }
}

TEST_F(NodeContainerTest, IterateByLabels) {
    std::vector<LabelSetID> nodeLabelSets = {0, 0, 1, 1, 1, 2, 3};
    std::vector<EntityID> theoNodeIDs0 = {0, 1};
    std::vector<EntityID> theoNodeIDs1 = {2, 3, 4};
    std::vector<EntityID> theoNodeIDs2 = {5};
    std::vector<EntityID> theoNodeIDs3 = {6};

    auto container = NodeContainer::create(0, _metadata, nodeLabelSets);
    ASSERT_TRUE(container != nullptr);

    auto theoIt = theoNodeIDs0.cbegin();
    for (EntityID nodeID : container->getRange(0)) {
        ASSERT_EQ(theoIt->getValue(), nodeID.getValue());
        theoIt++;
    }

    theoIt = theoNodeIDs1.cbegin();
    for (EntityID nodeID : container->getRange(1)) {
        ASSERT_EQ(theoIt->getValue(), nodeID.getValue());
        theoIt++;
    }

    theoIt = theoNodeIDs2.cbegin();
    for (EntityID nodeID : container->getRange(2)) {
        ASSERT_EQ(theoIt->getValue(), nodeID.getValue());
        theoIt++;
    }

    theoIt = theoNodeIDs3.cbegin();
    for (EntityID nodeID : container->getRange(3)) {
        ASSERT_EQ(theoIt->getValue(), nodeID.getValue());
        theoIt++;
    }
}

TEST_F(NodeContainerTest, GetNodeLabelSet) {
    std::vector<LabelSetID> nodeLabelSets = {0, 0, 1, 1, 1, 2, 3};

    auto container = NodeContainer::create(0, _metadata, nodeLabelSets);
    ASSERT_TRUE(container != nullptr);

    LabelSetID id0 = container->getNodeLabelSet(0);
    ASSERT_TRUE(id0.isValid());
    ASSERT_EQ(id0.getValue(), 0);

    LabelSetID id1 = container->getNodeLabelSet(1);
    ASSERT_TRUE(id1.isValid());
    ASSERT_EQ(id1.getValue(), 0);

    LabelSetID id2 = container->getNodeLabelSet(2);
    ASSERT_TRUE(id2.isValid());
    ASSERT_EQ(id2.getValue(), 1);

    LabelSetID id3 = container->getNodeLabelSet(3);
    ASSERT_TRUE(id3.isValid());
    ASSERT_EQ(id3.getValue(), 1);

    LabelSetID id5 = container->getNodeLabelSet(5);
    ASSERT_TRUE(id5.isValid());
    ASSERT_EQ(id5.getValue(), 2);

    LabelSetID id9 = container->getNodeLabelSet(9);
    ASSERT_FALSE(id9.isValid());
}
