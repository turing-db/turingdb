#include <gtest/gtest.h>
#include <memory>

#include "Graph.h"
#include "iterators/ChunkConfig.h"
#include "columns/ColumnIDs.h"
#include "iterators/ScanNodesIterator.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "versioning/CommitBuilder.h"
#include "reader/GraphReader.h"
#include "writers/DataPartBuilder.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

class ScanNodesIteratorTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = JobSystem::create();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<db::JobSystem> _jobSystem;
    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(ScanNodesIteratorTest, emptyGraph) {
    auto graph = Graph::create();
    const Transaction transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    auto it = reader.scanNodes().begin();
    ASSERT_TRUE(!it.isValid());

    ColumnIDs colNodes;
    it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(colNodes.empty());
}

TEST_F(ScanNodesIteratorTest, oneEmptyCommit) {
    auto graph = Graph::create();
    const auto tx = graph->openWriteTransaction();
    auto commitBuilder = tx.prepareCommit();
    [[maybe_unused]] auto& builder = commitBuilder->newBuilder();
    const auto res = graph->rebaseAndCommit(std::move(commitBuilder), *_jobSystem);
    if (!res) {
        spdlog::info(res.error().fmtMessage());
    }
    ASSERT_TRUE(res);

    const Transaction transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    auto it = reader.scanNodes().begin();
    ASSERT_TRUE(!it.isValid());

    ColumnIDs colNodes;
    it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(colNodes.empty());
}

TEST_F(ScanNodesIteratorTest, threeEmptyCommits) {
    auto graph = Graph::create();

    const auto tx = graph->openWriteTransaction();
    for (auto i = 0; i < 3; i++) {
        auto commitBuilder = tx.prepareCommit();
        [[maybe_unused]] auto& builder = commitBuilder->newBuilder();
        const auto res = graph->rebaseAndCommit(std::move(commitBuilder), *_jobSystem);
        if (!res) {
            spdlog::info(res.error().fmtMessage());
        }
        ASSERT_TRUE(res);
    }

    const Transaction transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    auto it = reader.scanNodes().begin();
    ASSERT_TRUE(!it.isValid());

    ColumnIDs colNodes;
    it.fill(&colNodes, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(colNodes.empty());
}

TEST_F(ScanNodesIteratorTest, oneChunkSizePart) {
    auto graph = Graph::create();

    LabelSet labelset = LabelSet::fromList({0});

    {
        const auto tx = graph->openWriteTransaction();
        auto commitBuilder = tx.prepareCommit();
        auto& builder = commitBuilder->newBuilder();
        for (size_t i = 0; i < ChunkConfig::CHUNK_SIZE; i++) {
            builder.addNode(labelset);
        }

        ASSERT_EQ(builder.nodeCount(), ChunkConfig::CHUNK_SIZE);
        const auto res = graph->rebaseAndCommit(std::move(commitBuilder), *_jobSystem);
        if (!res) {
            spdlog::info(res.error().fmtMessage());
        }
        ASSERT_TRUE(res);
    }

    const Transaction transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
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
    auto graph = Graph::create();

    LabelSet labelset = LabelSet::fromList({0});

    const auto tx = graph->openWriteTransaction();
    for (auto i = 0; i < 8; i++) {
        auto commitBuilder = tx.prepareCommit();
        auto& builder = commitBuilder->newBuilder();
        for (size_t j = 0; j < ChunkConfig::CHUNK_SIZE; j++) {
            builder.addNode(labelset);
        }

        ASSERT_EQ(builder.nodeCount(), ChunkConfig::CHUNK_SIZE);
        const auto res = graph->rebaseAndCommit(std::move(commitBuilder), *_jobSystem);
        if (!res) {
            spdlog::info(res.error().fmtMessage());
        }
        ASSERT_TRUE(res);
    }

    const Transaction transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
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

    auto graph = Graph::create();

    LabelSet labelset = LabelSet::fromList({0});

    {
        const auto tx = graph->openWriteTransaction();
        auto commitBuilder = tx.prepareCommit();
        auto& builder = commitBuilder->newBuilder();
        for (size_t i = 0; i < nodeCount; i++) {
            builder.addNode(labelset);
        }
        const auto res = graph->rebaseAndCommit(std::move(commitBuilder), *_jobSystem);
        if (!res) {
            spdlog::info(res.error().fmtMessage());
        }
        ASSERT_TRUE(res);
    }

    const Transaction transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
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
