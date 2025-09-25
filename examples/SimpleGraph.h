#pragma once

#include <string_view>
#include <vector>

#include "ID.h"

namespace db {

class Graph;

class SimpleGraph {
public:
    static void createSimpleGraph(Graph* graph);

    static NodeID findNodeID(Graph* graph, std::string_view nodeName);
    static void findNodesByLabel(Graph* graph,
                                 std::string_view labelName,
                                 std::vector<NodeID>& nodeIDs);

    static void findOutEdges(Graph* graph,
                             const std::vector<NodeID>& nodeIDs,
                             std::vector<EdgeID>& edgeIDs,
                             std::vector<EdgeTypeID>& edgeTypes,
                             std::vector<NodeID>& targetNodeIDs);

    SimpleGraph() = delete;
};

}
