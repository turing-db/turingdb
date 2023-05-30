// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include <algorithm>

#include "DB.h"
#include "Network.h"
#include "Node.h"
#include "Edge.h"
#include "Writeback.h"

using namespace db;

void fillNextNodes(Node::Set& nodeSet, const Node* node) {
    const auto getTarget = [](const Edge* e) { return e->getTarget(); };
    MappedRange outNodes(node->outEdges(), getTarget);
    nodeSet.insert(outNodes.begin(), outNodes.end());
}

TEST(NetworkTest, createEmpty) {
    DB* db = DB::create();

    Writeback wb(db);
    Network* myNet = wb.createNetwork(db->getString("my net"));
    ASSERT_TRUE(myNet);
    EXPECT_EQ(myNet->getName(), db->getString("my net"));

    EXPECT_EQ(db->networks().size(), 1);

    std::vector<Network*> nets(db->networks().begin(),
                               db->networks().end());
    ASSERT_EQ(nets, std::vector{myNet});

    delete db;
}

TEST(NetworkTest, createEmptyNameCollision) {
    DB* db = DB::create();

    Writeback wb(db);
    Network* net = wb.createNetwork(db->getString("my net"));
    ASSERT_TRUE(net);
    Network* net2 = wb.createNetwork(db->getString("my net"));
    ASSERT_FALSE(net2);

    EXPECT_EQ(db->networks().size(), 1);

    delete db;
}

TEST(NetworkTest, ppiCXCL14) {
    DB* db = DB::create();
    Writeback wb(db);

    // Protein
    NodeType* protein = wb.createNodeType(db->getString("Protein"));
    EdgeType* interactWith = wb.createEdgeType(db->getString("interactWith"),
                                               protein,
                                               protein);

    Network* ppi = wb.createNetwork(db->getString("ppi"));

    Node* CXCL14 = wb.createNode(ppi, protein, db->getString("CXCL14"));
    Node* CXCL12 = wb.createNode(ppi, protein, db->getString("CXCL12"));
    Node* CCR7 = wb.createNode(ppi, protein, db->getString("CCR7"));
    Node* CXCR2 = wb.createNode(ppi, protein, db->getString("CXCR2"));
    Node* CXCR1 = wb.createNode(ppi, protein, db->getString("CXCR1"));
    Node* CXCR5 = wb.createNode(ppi, protein, db->getString("CXCR5"));
    Node* CCR2 = wb.createNode(ppi, protein, db->getString("CCR2"));
    Node* CCR1 = wb.createNode(ppi, protein, db->getString("CCR1"));
    Node* CCR5 = wb.createNode(ppi, protein, db->getString("CCR5"));
    Node* CXCR4 = wb.createNode(ppi, protein, db->getString("CXCR4"));
    Node* CXCR3 = wb.createNode(ppi, protein, db->getString("CXCR3"));

    wb.createEdge(interactWith, CXCL14, CXCL12);
    wb.createEdge(interactWith, CXCR3, CXCL14);
    wb.createEdge(interactWith, CXCL12, CXCR5);
    wb.createEdge(interactWith, CXCL12, CXCR1);
    wb.createEdge(interactWith, CXCL12, CXCR2);
    wb.createEdge(interactWith, CXCR1, CXCR2);
    wb.createEdge(interactWith, CXCL14, CXCR1);
    wb.createEdge(interactWith, CXCL14, CXCR2);
    wb.createEdge(interactWith, CCR7, CXCL14);
    wb.createEdge(interactWith, CCR7, CXCL12);
    wb.createEdge(interactWith, CXCR3, CXCR4);
    wb.createEdge(interactWith, CXCR4, CCR5);
    wb.createEdge(interactWith, CXCL14, CXCR4);
    wb.createEdge(interactWith, CXCL12, CXCR4);
    wb.createEdge(interactWith, CCR5, CCR1);
    wb.createEdge(interactWith, CCR5, CCR2);
    wb.createEdge(interactWith, CCR1, CCR2);
    wb.createEdge(interactWith, CXCL14, CCR5);
    wb.createEdge(interactWith, CXCL12, CCR5);
    wb.createEdge(interactWith, CXCL14, CCR1);
    wb.createEdge(interactWith, CXCL12, CCR1);
    wb.createEdge(interactWith, CXCL14, CCR2);
    wb.createEdge(interactWith, CXCL12, CCR2);

    // CXCL14
    Node::Set cxcl14NextGold({CXCL12, CXCR1, CXCR2, CXCR4, CCR5, CCR1, CCR2});
    Node::Set cxcl14Next;
    fillNextNodes(cxcl14Next, CXCL14);
    ASSERT_EQ(cxcl14Next, cxcl14NextGold);

    // CCR2
    Node::Set ccr2Next;
    fillNextNodes(ccr2Next, CCR2);
    ASSERT_TRUE(ccr2Next.empty());

    // CCR5
    Node::Set ccr5NextGold({CCR2, CCR1});
    Node::Set ccr5Next;
    fillNextNodes(ccr5Next, CCR5);
    ASSERT_EQ(ccr5Next, ccr5NextGold);

    // Search an edge from CCR7 in CXCL14 in edges
    {
        const auto inEdges = CXCL14->inEdges();
        const auto isFromCCR7 = [CCR7](const Edge* e){ return e->getSource() == CCR7; };
        const auto findIt = std::find_if(inEdges.begin(), inEdges.end(), isFromCCR7);
        EXPECT_NE(findIt, inEdges.end());
    }

    delete db;
}
