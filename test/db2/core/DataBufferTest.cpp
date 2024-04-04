#include "DataBuffer.h"
#include "BioLog.h"
#include "FileUtils.h"

#include <gtest/gtest.h>

using namespace db;

class DataBufferTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        srand(0);
        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
        _logPath = FileUtils::Path(_outDir) / "log";

        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);

        Log::BioLog::init();
        Log::BioLog::openFile(_logPath.string());
    }

    void TearDown() override {
        Log::BioLog::destroy();
    }

    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(DataBufferTest, CreateTest) {
    DataBuffer tempBuffer(0, 0);
    std::vector<EntityID> nodeIDs(100);

    // Nodes
    for (auto& nodeID : nodeIDs) {
        nodeID = tempBuffer.addNode({0});
    }

    // Edges
    size_t edgeCount = 100;
    for (size_t i = 0; i < edgeCount; i++) {
        EntityID source = rand() % nodeIDs.size();
        EntityID target = rand() % nodeIDs.size();
        tempBuffer.addEdge(0, source, target);
    }

    size_t optionalPropertyCount = 70;
    for (size_t i = 0; i < optionalPropertyCount; i++) {
        tempBuffer.addProperty<Int64PropertyType>(i, 0, i);
    }

    const auto& nodeData = tempBuffer.getCoreNodeData();
    ASSERT_EQ(nodeData.size(), nodeIDs.size());
    ASSERT_EQ(nodeIDs.size(), tempBuffer.getCoreNodeCount());
    ASSERT_EQ(edgeCount, tempBuffer.getCoreEdgeCount() / 2);
}
