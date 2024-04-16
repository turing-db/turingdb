#include "EdgeContainer.h"
#include "NodeContainer.h"
#include "NodeData.h"
#include "NodeEdgeView.h"
#include "TemporaryDataBuffer.h"

#include <gtest/gtest.h>

using namespace db;

TEST(EdgeContainerTest, Create) {
    constexpr size_t edgeCount = 11;

    std::map<LabelSet, size_t> nodeCountsPerLabel = {
        {{0}, 3},
        {{1}, 2},
        {{2}, 1},
    };

    std::map<LabelSet, std::pair<size_t, size_t>> edgeCountsPerLabel = {
        {{0}, {7, 4}},
        {{1}, {3, 3}},
        {{2}, {1, 4}},
    };

    TemporaryDataBuffer buffer(0, 0);
    buffer.addNode({0});     // Temp node id: 0
    buffer.addNode({1});     // Temp node id: 1
    buffer.addNode({0});     // Temp node id: 2
    buffer.addNode({2});     // Temp node id: 3
    buffer.addNode({1});     // Temp node id: 4
    buffer.addNode({0});     // Temp node id: 5
    buffer.addEdge(0, 0, 1); // Temp edge id: 0
    buffer.addEdge(0, 0, 2); // Temp edge id: 1
    buffer.addEdge(0, 0, 3); // Temp edge id: 2
    buffer.addEdge(0, 1, 2); // Temp edge id: 3
    buffer.addEdge(0, 1, 3); // Temp edge id: 4
    buffer.addEdge(0, 2, 4); // Temp edge id: 5
    buffer.addEdge(0, 2, 3); // Temp edge id: 6
    buffer.addEdge(0, 3, 1); // Temp edge id: 7
    buffer.addEdge(0, 4, 5); // Temp edge id: 8
    buffer.addEdge(0, 5, 2); // Temp edge id: 9
    buffer.addEdge(0, 5, 3); // Temp edge id: 10

    // Out edges should be stored as:
    // Temp ids: 001 102 203 524 623 952 1053 312 413 845 731
    // Final ids: 003 101 205 314 415 521 625 731 835 942 1053
    // labelset {0}  first
    // edges: [0:0->1, 1:0->2, 2:0->3, 5:2->4, 6:2->3, 9:5->2, 10:5->3]
    // nodeOffsets: 0:0, 2:3, 5:5

    // labelset {1} second
    // edges: [3:1->2, 4:1->3, 8:4->5]
    // nodeOffsets: 1:7, 4:9

    // labelset {2} last
    // edges: [7:3->1]
    // nodeOffsets : 3:10

    // temp nodeIDs:  0 0 0 2 2 5 5 1 1 4 3
    // final nodeIDs: 0 0 0 1 1 2 2 3 3 4 5
    // outEdgeIDs: 0 1 2 3 4 5 6 7 8 9 10
    // inEdgeIDs: 1 2 5 7 9 0 10 3 4 6 8

    // ---
    // In Edges should be stored as:
    // Temp ids: 120 321 925 854 010 713 542 230 431 632 1035
    // Final ids: 110 713 512 924 030 1035 341 250 853 451 652
    // labelset {0}  first
    // edges: [1:0->2, 9:5->2, 3:1->2, 8:4->5]
    // nodeOffsets: 0:0, 1:3, 2:5

    // labelset {1} second
    // edges: [0:0->1, 5:2->4, 7:3->1]
    // nodeOffsets: 3:7, 4:9

    // labelset {2} last
    // edges: [2:0->3, 6:2->3, 10:5->3, 4:1->3]
    // nodeOffsets : 5:10

    EdgeContainer::Builder edgesBuilder;
    NodeContainer::Builder nodesBuilder;

    nodesBuilder.initialize(0,
                            buffer.getCoreNodeCount(),
                            nodeCountsPerLabel);

    edgesBuilder.initialize(0, edgeCount, edgeCount, buffer.getCoreEdgeCounts());
    struct NodeIDMap {
        EntityID tmpNodeID;
        EntityID finalNodeID;
    };

    // - Convert all node IDs
    // - Fill the node container
    // - Prepare the edge container
    for (const auto& [tmpNodeID, tmpNodeData] : buffer.getCoreNodeData()) {
        auto& node = nodesBuilder.addNode(tmpNodeData, tmpNodeID);
        auto offsets = edgesBuilder.addNode(tmpNodeData);

        node._coreOutEdgeOffset = offsets.first;
        node._coreInEdgeOffset = offsets.second;
    }

    // Push edges
    for (const auto& [tmpNodeID, tmpNodeData] : buffer.getCoreNodeData()) {
        NodeData& source = nodesBuilder.getMutableNodeFromTempID(tmpNodeID);

        for (const auto& edge : tmpNodeData._outEdges) {
            NodeData& target = nodesBuilder.getMutableNodeFromTempID(edge._otherID);
            edgesBuilder.addCoreEdgeToCore(edge._edgeTypeID,
                                           source,
                                           target);
        }
    }

    std::unique_ptr<EdgeContainer> edges = edgesBuilder.build();
    std::unique_ptr<NodeContainer> nodes = nodesBuilder.build();

    // Loop other all edges
    std::string output;
    for (const auto& edge : edges->getAllOutEdges()) {
        output += std::to_string(edge._edgeID);
        output += std::to_string(edge._nodeID);
        output += std::to_string(edge._otherID);
        output += " ";
    }
    std::cout << "All Out edges: " << output << std::endl;
    ASSERT_STREQ(output.c_str(), "003 101 205 314 415 521 625 731 835 942 1053 ");

    output.clear();
    for (const auto& edge : edges->getAllInEdges()) {
        output += std::to_string(edge._edgeID);
        output += std::to_string(edge._nodeID);
        output += std::to_string(edge._otherID);
        output += " ";
    }
    std::cout << "All In edges: " << output << std::endl;
    ASSERT_STREQ(output.c_str(), "110 713 512 924 030 1035 341 250 853 451 652 ");

    // Loop other out edges by labelset
    output.clear();
    for (const auto& edge : edges->getOutEdgesSpanFromLabelSet({0})) {
        output += std::to_string(edge._edgeID);
        output += std::to_string(edge._nodeID);
        output += std::to_string(edge._otherID);
        output += " ";
    }
    ASSERT_STREQ(output.c_str(), "003 101 205 314 415 521 625 ");

    output.clear();
    for (const auto& edge : edges->getOutEdgesSpanFromLabelSet({1})) {
        output += std::to_string(edge._edgeID);
        output += std::to_string(edge._nodeID);
        output += std::to_string(edge._otherID);
        output += " ";
    }
    ASSERT_STREQ(output.c_str(), "731 835 942 ");

    output.clear();
    for (const auto& edge : edges->getOutEdgesSpanFromLabelSet({2})) {
        output += std::to_string(edge._edgeID);
        output += std::to_string(edge._nodeID);
        output += std::to_string(edge._otherID);
        output += " ";
    }
    ASSERT_STREQ(output.c_str(), "1053 ");

    // Loop other in edges by labelset
    output.clear();
    for (const auto& edge : edges->getInEdgesSpanFromLabelSet({0})) {
        output += std::to_string(edge._edgeID);
        output += std::to_string(edge._nodeID);
        output += std::to_string(edge._otherID);
        output += " ";
    }
    ASSERT_STREQ(output.c_str(), "110 713 512 924 ");

    output.clear();
    for (const auto& edge : edges->getInEdgesSpanFromLabelSet({1})) {
        output += std::to_string(edge._edgeID);
        output += std::to_string(edge._nodeID);
        output += std::to_string(edge._otherID);
        output += " ";
    }
    ASSERT_STREQ(output.c_str(), "030 1035 341 ");

    output.clear();
    for (const auto& edge : edges->getInEdgesSpanFromLabelSet({2})) {
        output += std::to_string(edge._edgeID);
        output += std::to_string(edge._nodeID);
        output += std::to_string(edge._otherID);
        output += " ";
    }
    ASSERT_STREQ(output.c_str(), "250 853 451 652 ");

    // Node edge view
    const NodeData* node = nodes->tryGetNodeData(2);
    ASSERT_TRUE(node != nullptr);
    NodeEdgeView view = edges->getNodeEdgeView(*node);

    output.clear();
    for (const auto& edge : view.outEdges()) {
        output += std::to_string(edge._edgeID);
        output += std::to_string(edge._nodeID);
        output += std::to_string(edge._otherID);
        output += " ";
    }
    ASSERT_STREQ(output.c_str(), "521 625 ");
    std::cout << "Node out edges: " << output << std::endl;

    output.clear();
    for (const auto& edge : view.inEdges()) {
        output += std::to_string(edge._edgeID);
        output += std::to_string(edge._nodeID);
        output += std::to_string(edge._otherID);
        output += " ";
    }
    ASSERT_STREQ(output.c_str(), "924 ");
    std::cout << "Node in edges: " << output << std::endl;

    output.clear();
    for (const auto& edges : view.patchEdgeSpans()) {
        for (const auto& edge : edges) {
            output += std::to_string(edge._edgeID);
            output += std::to_string(edge._nodeID);
            output += std::to_string(edge._otherID);
            output += " ";
        }
    }

    ASSERT_STREQ(output.c_str(), "");
    ASSERT_EQ(view.getOutEdgeCount(), 2);
    ASSERT_EQ(view.getInEdgeCount(), 1);
}
