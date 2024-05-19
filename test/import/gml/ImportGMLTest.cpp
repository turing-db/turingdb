// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include "DB.h"
#include "Network.h"
#include "Writeback.h"

#include "GMLImport.h"
#include "StringBuffer.h"

using namespace db;
TEST(ImportGMLTest, createEmpty) {
    DB* db = DB::create();

    Writeback wb(db);
    Network* net = wb.createNetwork(db->getString("my net"));
    ASSERT_TRUE(net);

    const auto lrGraphPath = "LR_graph.gml";
    StringBuffer* strBuffer = StringBuffer::readFromFile(lrGraphPath);
    ASSERT_TRUE(strBuffer);

    GMLImport gmlImport(strBuffer, db, net);
    const bool success = gmlImport.run();
    ASSERT_TRUE(success);

    delete db;
}
