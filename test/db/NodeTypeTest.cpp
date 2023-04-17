// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include "DB.h"
#include "Writeback.h"
#include "NodeType.h"

using namespace db;

TEST(NodeTypeTest, createEmpty) {
    DB* db = DB::create();
    Writeback wb(db);

    NodeType* nodeType = wb.createNodeType(db->getString("Metabolite"));
    ASSERT_TRUE(nodeType);
    ASSERT_EQ(nodeType->getName(), db->getString("Metabolite"));

    delete db;
}

TEST(NodeTypeTest, createEmptyNameCollision) {
    DB* db = DB::create();
    Writeback wb(db);

    NodeType* type1 = wb.createNodeType(db->getString("Metabolite"));
    ASSERT_TRUE(type1);
    NodeType* type2 = wb.createNodeType(db->getString("Metabolite"));
    ASSERT_FALSE(type2);

    delete db;
}
