// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include <vector>

#include "DB.h"
#include "Writeback.h"
#include "Node.h"
#include "Edge.h"
#include "NodeType.h"

using namespace db;

TEST(EdgeTest, createComplete) {
    DB* db = DB::create();
    Writeback wb(db);

    NodeType* nodeType = wb.createNodeType(db->getString("node"));
    ASSERT_TRUE(nodeType);

    Network* net = wb.createNetwork(db->getString("My net"));

    // Create nodes
    const size_t nodeCount = 1000;
    std::vector<Node*> nodes;
    for (size_t i = 0; i < nodeCount; i++) {
        Node* node = wb.createNode(net, nodeType);
        nodes.push_back(node);
    }

    // Create edges between each pair of nodes
    auto nodeComp = nodeType->getBaseComponent();
    EdgeType* edgeType = wb.createEdgeType(db->getString("edge"), nodeComp, nodeComp);
    for (Node* n1 : nodes) {
        for (Node* n2 : nodes) {
            if (n1 != n2) {
                wb.createEdge(edgeType, n1, n2);
            }
        }
    }

    delete db;
}
