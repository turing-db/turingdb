// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include "DB.h"
#include "Network.h"
#include "NodeType.h"
#include "Node.h"
#include "EdgeType.h"
#include "Edge.h"

using namespace db;

TEST(NetworkTest, createEmpty) {
    DB* db = DB::create();

    Network* net = Network::create(db, db->getString("my net"));
    ASSERT_TRUE(net);
    EXPECT_EQ(net->getDisplayName(), db->getString("my net"));
    EXPECT_TRUE(net->nodes().empty());
    EXPECT_TRUE(net->subNetworks().empty());
    EXPECT_FALSE(net->getParent());

    delete db;
}

TEST(NetworkTest, ppiCXCL14) {
    DB* db = DB::create();

    // Protein
    NodeType* protein = NodeType::create(db, db->getString("Protein"));
    EdgeType* interactWith = EdgeType::create(db,
                                              db->getString("interactWith"),
                                              protein,
                                              protein);

    Network* ppi = Network::create(db, db->getString("ppi"));

    Node* CXCL14 = Node::create(ppi, protein, db->getString("CXCL14"));
    Node* CXCL12 = Node::create(ppi, protein, db->getString("CXCL12"));
    Node* CCR7 = Node::create(ppi, protein, db->getString("CCR7"));
    Node* CXCR2 = Node::create(ppi, protein, db->getString("CXCR2"));
    Node* CXCR1 = Node::create(ppi, protein, db->getString("CXCR1"));
    Node* CXCR5 = Node::create(ppi, protein, db->getString("CXCR5"));
    Node* CCR2 = Node::create(ppi, protein, db->getString("CCR2"));
    Node* CCR1 = Node::create(ppi, protein, db->getString("CCR1"));
    Node* CCR5 = Node::create(ppi, protein, db->getString("CCR5"));
    Node* CXCR4 = Node::create(ppi, protein, db->getString("CXCR4"));
    Node* CXCR3 = Node::create(ppi, protein, db->getString("CXCR3"));

    Edge::create(db, interactWith, CXCL14, CXCL12);
    Edge::create(db, interactWith, CXCR3, CXCL14);
    Edge::create(db, interactWith, CXCL12, CXCR5);
    Edge::create(db, interactWith, CXCL12, CXCR1);
    Edge::create(db, interactWith, CXCL12, CXCR2);
    Edge::create(db, interactWith, CXCR1, CXCR2);
    Edge::create(db, interactWith, CXCL14, CXCR1);
    Edge::create(db, interactWith, CXCL14, CXCR2);
    Edge::create(db, interactWith, CCR7, CXCL14);
    Edge::create(db, interactWith, CCR7, CXCL12);
    Edge::create(db, interactWith, CXCR3, CXCR4);
    Edge::create(db, interactWith, CXCR4, CCR5);
    Edge::create(db, interactWith, CXCL14, CXCR4);
    Edge::create(db, interactWith, CXCL12, CXCR4);
    Edge::create(db, interactWith, CCR5, CCR1);
    Edge::create(db, interactWith, CCR5, CCR2);
    Edge::create(db, interactWith, CCR1, CCR2);
    Edge::create(db, interactWith, CXCL14, CCR5);
    Edge::create(db, interactWith, CXCL12, CCR5);
    Edge::create(db, interactWith, CXCL14, CCR1);
    Edge::create(db, interactWith, CXCL12, CCR1);
    Edge::create(db, interactWith, CXCL14, CCR2);
    Edge::create(db, interactWith, CXCL12, CCR2);

    delete db;
}
