#include <gtest/gtest.h>
#include <memory>

#include "JobSystem.h"
#include "ScanNodesIterator.h"

#include "ChunkConfig.h"
#include "ColumnIDs.h"
#include "Graph.h"
#include "GraphView.h"
#include "GraphReader.h"
#include "GraphMetadata.h"
#include "DataPartBuilder.h"
#include "FileUtils.h"
#include "TuringTest.h"

using namespace db;
using namespace js;
using namespace turing::test;

class ScanNodesIteratorTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->initialize();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(ScanNodesIteratorTest, emptyDB) {
    auto graph = std::make_unique<Graph>();
    const auto view = graph->view();
    const auto reader = view.read();

    auto it = reader.scanNodes().begin();
    ASSERT_TRUE(!it.isValid());

    ColumnIDs colNodes;
    it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(colNodes.empty());
}

TEST_F(ScanNodesIteratorTest, oneEmptyPart) {
    auto graph = std::make_unique<Graph>();
    auto builder = graph->newPartWriter();
    builder->commit(*_jobSystem);

    const auto view = graph->view();
    const auto reader = view.read();
    auto it = reader.scanNodes().begin();
    ASSERT_TRUE(!it.isValid());

    ColumnIDs colNodes;
    it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(colNodes.empty());
}

TEST_F(ScanNodesIteratorTest, threeEmptyParts) {
    auto graph = std::make_unique<Graph>();

    for (auto i = 0; i < 3; i++) {
        auto builder = graph->newPartWriter();
        builder->commit(*_jobSystem);
    }

    const auto view = graph->view();
    const auto reader = view.read();
    auto it = reader.scanNodes().begin();
    ASSERT_TRUE(!it.isValid());

    ColumnIDs colNodes;
    it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(colNodes.empty());
}

TEST_F(ScanNodesIteratorTest, oneChunkSizePart) {
    auto graph = std::make_unique<Graph>();

    auto& labelsets = graph->getMetadata()->labelsets();
    LabelSet labelset = LabelSet::fromList({0});
    LabelSetID labelsetID = labelsets.getOrCreate(labelset);

    {
        auto builder = graph->newPartWriter();
        for (size_t i = 0; i < ChunkConfig::CHUNK_SIZE; i++) {
            builder->addNode(labelsetID);
        }

        ASSERT_EQ(builder->nodeCount(), ChunkConfig::CHUNK_SIZE);
        builder->commit(*_jobSystem);
    }

    const auto view = graph->view();
    const auto reader = view.read();
    auto it = reader.scanNodes().begin();
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
    ColumnIDs colNodes;
    it = reader.scanNodes().begin();
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
    auto graph = std::make_unique<Graph>();

    auto& labelsets = graph->getMetadata()->labelsets();
    LabelSet labelset = LabelSet::fromList({0});
    LabelSetID labelsetID = labelsets.getOrCreate(labelset);

    for (auto i = 0; i < 8; i++) {
        auto builder = graph->newPartWriter();
        for (size_t j = 0; j < ChunkConfig::CHUNK_SIZE; j++) {
            builder->addNode(labelsetID);
        }

        ASSERT_EQ(builder->nodeCount(), ChunkConfig::CHUNK_SIZE);
        builder->commit(*_jobSystem);
    }

    const auto view = graph->view();
    const auto reader = view.read();
    auto it = reader.scanNodes().begin();
    ASSERT_TRUE(it.isValid());

    // Read node by node
    EntityID expectedID = 0;
    for (; it.isValid(); ++it) {
        ASSERT_EQ(*it, expectedID);
        expectedID++;
    }
    ASSERT_TRUE(!it.isValid());

    // Read nodes by chunks
    ColumnIDs colNodes;
    it = reader.scanNodes().begin();

    expectedID = 0;
    while (it.isValid()) {
        // colNodes.clear();
        // ASSERT_TRUE(colNodes.empty());
        it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
        ASSERT_TRUE(!colNodes.empty());
        // ASSERT_LE(colNodes.size(), ChunkConfig::CHUNK_SIZE);
    }

    for (EntityID id : colNodes) {
        ASSERT_EQ(id, expectedID);
        expectedID++;
    }
}

TEST_F(ScanNodesIteratorTest, chunkAndALeftover) {
    const size_t nodeCount = 1.35 * ChunkConfig::CHUNK_SIZE;

    auto graph = std::make_unique<Graph>();

    auto& labelsets = graph->getMetadata()->labelsets();
    LabelSet labelset = LabelSet::fromList({0});
    LabelSetID labelsetID = labelsets.getOrCreate(labelset);

    {
        auto builder = graph->newPartWriter();
        for (size_t i = 0; i < nodeCount; i++) {
            builder->addNode(labelsetID);
        }
        builder->commit(*_jobSystem);
    }

    const auto view = graph->view();
    const auto reader = view.read();
    auto it = reader.scanNodes().begin();

    ColumnIDs colNodes;
    colNodes.reserve(ChunkConfig::CHUNK_SIZE);

    // Read nodes by chunks
    it = reader.scanNodes().begin();
    EntityID expectedID = 0;
    while (it.isValid()) {
        it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    }

    for (EntityID id : colNodes) {
        ASSERT_EQ(id, expectedID);
        expectedID++;
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 4;
    });
}
