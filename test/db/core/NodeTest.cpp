// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include "DB.h"
#include "Writeback.h"
#include "Node.h"

using namespace db;

TEST(NodeTest, create1) {
    DB* db = DB::create();
    Writeback wb(db);

    NodeType* metaboliteType = wb.createNodeType(db->getString("Metabolite"));
    ASSERT_TRUE(metaboliteType);

    Network* net = wb.createNetwork(db->getString("My net"));

    Node* node = wb.createNode(net, metaboliteType);
    ASSERT_TRUE(node);

    delete db;
}
