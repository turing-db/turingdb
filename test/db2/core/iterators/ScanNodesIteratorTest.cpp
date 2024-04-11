#include <gtest/gtest.h>
#include <memory>

#include "ScanNodesIterator.h"

#include "DB.h"
#include "DBAccess.h"
#include "Reader.h"
#include "DataPart.h"
#include "DataBuffer.h"
#include "ColumnNodes.h"
#include "ChunkConfig.h"

using namespace db;

class ScanNodesIteratorTest : public ::testing::Test {
protected:
    void SetUp() override {
    }

    void TearDown() override {
    }
};

TEST_F(ScanNodesIteratorTest, emptyDB) {
    auto db = std::make_unique<DB>();
    auto access = db->access();
    auto reader = access.getReader();

    auto it = reader.getScanNodesIterator();
    ASSERT_TRUE(!it.isValid());

    ColumnNodes colNodes;
    it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(colNodes.empty());
}

TEST_F(ScanNodesIteratorTest, oneEmptyPart) {
    auto db = std::make_unique<DB>();
    auto access = db->uniqueAccess();

    {
        const DataBuffer buf = access.newDataBuffer();
        access.pushDataPart(buf);
    } 

    auto reader = access.getReader();
    auto it = reader.getScanNodesIterator();
    ASSERT_TRUE(!it.isValid());

    ColumnNodes colNodes;
    it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(colNodes.empty());
}

TEST_F(ScanNodesIteratorTest, threeEmptyParts) {
    auto db = std::make_unique<DB>();
    auto access = db->uniqueAccess();

    for (auto i = 0; i < 3; i++) {
        const DataBuffer buf = access.newDataBuffer();
        access.pushDataPart(buf);
    } 

    auto reader = access.getReader();
    auto it = reader.getScanNodesIterator();
    ASSERT_TRUE(!it.isValid());

    ColumnNodes colNodes;
    it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(colNodes.empty());
}

TEST_F(ScanNodesIteratorTest, oneChunkSizePart) {
    auto db = std::make_unique<DB>();
    auto access = db->uniqueAccess();

    {
        DataBuffer buf = access.newDataBuffer();
        for (size_t i = 0; i < ChunkConfig::CHUNK_SIZE; i++) {
            buf.addNode({0});
        } 

        ASSERT_EQ(buf.getCoreNodeCount(), ChunkConfig::CHUNK_SIZE);

        access.pushDataPart(buf);
    } 

    auto reader = access.getReader();
    auto it = reader.getScanNodesIterator();
    ASSERT_TRUE(it.isValid());

    // Read node by node
    std::vector<EntityID> nodes;
    EntityID expectedID = 0;
    for (; it.isValid(); ++it) {
        ASSERT_EQ(*it, expectedID);
        expectedID++;
    }
    ASSERT_TRUE(!it.isValid());

    // Read nodes by chunks
    ColumnNodes colNodes;
    it = reader.getScanNodesIterator();
    it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(!colNodes.empty());
    ASSERT_EQ(colNodes.size(), ChunkConfig::CHUNK_SIZE);

    expectedID = 0;
    for (EntityID id : colNodes) {
        ASSERT_EQ(id, expectedID);
        expectedID++;
    }
    ASSERT_TRUE(!it.isValid());
}

TEST_F(ScanNodesIteratorTest, manyChunkSizePart) {
    auto db = std::make_unique<DB>();
    auto access = db->uniqueAccess();

    for (auto i = 0; i < 8; i++) {
        DataBuffer buf = access.newDataBuffer();
        for (size_t i = 0; i < ChunkConfig::CHUNK_SIZE; i++) {
            buf.addNode({0});
        } 

        ASSERT_EQ(buf.getCoreNodeCount(), ChunkConfig::CHUNK_SIZE);

        access.pushDataPart(buf);
    } 

    auto reader = access.getReader();
    auto it = reader.getScanNodesIterator();
    ASSERT_TRUE(it.isValid());

    // Read node by node
    EntityID expectedID = 0;
    for (; it.isValid(); ++it) {
        ASSERT_EQ(*it, expectedID);
        expectedID++;
    }
    ASSERT_TRUE(!it.isValid());

    // Read nodes by chunks
    ColumnNodes colNodes;
    it = reader.getScanNodesIterator();

    expectedID = 0;
    while (it.isValid()) {
        colNodes.clear();
        ASSERT_TRUE(colNodes.empty());
        it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
        ASSERT_TRUE(!colNodes.empty());
        ASSERT_LE(colNodes.size(), ChunkConfig::CHUNK_SIZE);

        for (EntityID id : colNodes) {
            ASSERT_EQ(id, expectedID);
            expectedID++;
        }
    }
}

TEST_F(ScanNodesIteratorTest, chunkAndALeftover) {
    const size_t nodeCount = 1.35*ChunkConfig::CHUNK_SIZE;

    auto db = std::make_unique<DB>();
    auto access = db->uniqueAccess();

    {
        DataBuffer buf = access.newDataBuffer();
        for (size_t i = 0; i < nodeCount; i++) {
            buf.addNode({0});
        } 

        access.pushDataPart(buf);
    } 

    auto reader = access.getReader();
    auto it = reader.getScanNodesIterator();

    ColumnNodes colNodes;
    colNodes.reserve(ChunkConfig::CHUNK_SIZE);

    // Read nodes by chunks
    it = reader.getScanNodesIterator();
    EntityID expectedID = 0;
    while (it.isValid()) {
        it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);

        for (EntityID id : colNodes) {
            ASSERT_EQ(id, expectedID);
            expectedID++;
        }
    }
}
