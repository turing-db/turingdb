#include "FlatEdgeContainer.h"
#include "NodeData.h"

#include <gtest/gtest.h>

using namespace db;

EdgeRecord newEdge(EntityID edgeID, EntityID nodeID, EntityID otherID) {
    return EdgeRecord {
        ._edgeID = edgeID,
        ._nodeID = nodeID,
        ._otherID = otherID,
    };
}

TEST(FlatEdgeContainerTest, Create) {
    std::map<EntityID, TempNodeData> nodes {
        {0, {._labelset = {0}, ._edges = {
                                   newEdge(0, 0, 1),
                                   newEdge(1, 0, 2),
                                   newEdge(2, 0, 3),
                               }}},
        {1, {._labelset = {1}, ._edges = {
                                   newEdge(3, 1, 2),
                                   newEdge(4, 1, 3),
                               }}},
        {2, {._labelset = {0}, ._edges = {
                                   newEdge(5, 2, 4),
                                   newEdge(6, 2, 3),
                               }}},
        {3, {._labelset = {2}, ._edges = {
                                   newEdge(7, 3, 1),
                               }}},
        {4, {._labelset = {1}, ._edges = {
                                   newEdge(8, 4, 5),
                               }}},
        {5, {._labelset = {0}, ._edges = {
                                   newEdge(9, 5, 2),
                                   newEdge(10, 5, 3),
                               }}},
    };

    std::vector<EntityID> nodeEdgeOffsets;
    std::vector<size_t> nodeEdgeCounts;

    // Edges should be stored as:
    // labelset {0}  first
    // edges: [0:0->1, 1:0->2, 2:0->3, 5:2->4, 6:2->3, 9:5->2, 10:5->3]
    // nodeOffsets: 0:0, 2:3, 5:5

    // labelset {1} second
    // edges: [3:1->2, 4:1->3, 8:4->5]
    // nodeOffsets: 1:7, 4:9

    // labelset {2} last
    // edges: [7:3->1]
    // nodeOffsets : 3:10

    // nodeIDs: 0 0 0 2 2 5 5 1 1 4 3
    // edgeIDs: 0 1 2 5 6 9 10 3 4 8 7
    // nodeEdgeOffsets: 0 7 3 10 9 5
    // nodeEdgeCounts: 3 2 2 1 1 2

    constexpr size_t edgeCount = 11;
    FlatEdgeContainer::Builder containerBuilder;
    containerBuilder.startBuilding(edgeCount);
    containerBuilder.addNodeLabelSet({0}, 7);
    containerBuilder.addNodeLabelSet({1}, 3);
    containerBuilder.addNodeLabelSet({2}, 1);

    for (const auto& [nodeID, nodeData] : nodes) {
        nodeEdgeOffsets.emplace_back(containerBuilder.nextNode(nodeData._labelset));

        for (const auto& tmpEdge : nodeData._edges) {
            containerBuilder.setNextEdgeRecord(tmpEdge);
        }

        nodeEdgeCounts.emplace_back(containerBuilder.getCurrentNodeEdgeCount());
    }

    FlatEdgeContainer edges = containerBuilder.build();

    std::string output;
    for (const auto& edge : edges.get()) {
        output += std::to_string(edge._edgeID.getID());
        output += std::to_string(edge._nodeID.getID());
        output += std::to_string(edge._otherID.getID());
        output += " ";
    }
    ASSERT_STREQ(output.c_str(), "001 102 203 524 623 952 1053 312 413 845 731 ");

    output.clear();
    for (const auto& nodeID : nodeEdgeOffsets) {
        output += std::to_string(nodeID.getID()) + " ";
    }
    ASSERT_STREQ(output.c_str(), "0 7 3 10 9 5 ");

    output.clear();
    for (const auto& count : nodeEdgeCounts) {
        output += std::to_string(count) + " ";
    }
    ASSERT_STREQ(output.c_str(), "3 2 2 1 1 2 ");
}
