#include "DataPart.h"
#include "BioLog.h"
#include "FileUtils.h"
#include "TemporaryDataBuffer.h"

#include <gtest/gtest.h>

using namespace db;

class DataPartTest : public ::testing::Test {
protected:
    DataPartTest()
        : _tempBuffer(0, 0) {
    }

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

        std::vector<EntityID> nodeIDs(100);

        // Nodes
        for (auto& nodeID : nodeIDs) {
            nodeID = _tempBuffer.addNode({0});
        }

        // Edges
        size_t edgeCount = 100;
        for (size_t i = 0; i < edgeCount; i++) {
            EntityID source = rand() % nodeIDs.size();
            EntityID target = rand() % nodeIDs.size();
            _tempBuffer.addEdge(0, source, target);
        }

        size_t mandatoryPropertyCount = nodeIDs.size();
        for (size_t i = 0; i < mandatoryPropertyCount; i++) {
            _tempBuffer.addProperty<StringPropertyType>(i, 0, std::to_string(i));
        }

        size_t optionalPropertyCount = 70;
        for (size_t i = 0; i < optionalPropertyCount; i++) {
            _tempBuffer.addProperty<Int64PropertyType>(i, 1, i);
        }
    }

    void TearDown() override {
        Log::BioLog::destroy();
    }

    std::string _outDir;
    FileUtils::Path _logPath;
    TemporaryDataBuffer _tempBuffer;
};

TEST_F(DataPartTest, CreateTest) {
    DataPart dp;
    dp.load(0, 0, _tempBuffer);

    ASSERT_EQ(dp.getNodeCount(), _tempBuffer.getCoreNodeCount());
    ASSERT_EQ(dp.getCoreEdgeCount(), _tempBuffer.getCoreEdgeCount() / 2);
}
