#include <gtest/gtest.h>
#include <memory>

#include "DebugDumpProcessor.h"
#include "DiscardProcessor.h"
#include "JobSystem.h"
#include "Pipeline.h"
#include "PipelineExecutor.h"
#include "ScanNodesProcessor.h"
#include "FileUtils.h"
#include "PerfStat.h"

#include "ColumnNodes.h"
#include "DB.h"
#include "DBAccess.h"
#include "Stream.h"
#include "TestUtils.h"
#include "LogSetup.h"

using namespace db;

class ScanNodesProcessorTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
        _logPath = FileUtils::Path(_outDir) / "log";
        _perfPath = FileUtils::Path(_outDir) / "perf";

        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);

        LogSetup::setupLogFileBacked(_logPath.string());
        PerfStat::init(_perfPath);
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->initialize();
    }

    void TearDown() override {
        _jobSystem->terminate();
        PerfStat::destroy();
    }

    std::string _outDir;
    std::unique_ptr<JobSystem> _jobSystem;
    FileUtils::Path _logPath;
    FileUtils::Path _perfPath;
};

TEST_F(ScanNodesProcessorTest, emptyDB) {
    auto db = std::make_unique<DB>();
    auto access = db->access();

    Pipeline pipeline;

    auto scanNodes = pipeline.addProcessor<ScanNodesProcessor>(db.get());
    auto scanNodesOut = scanNodes->createOut(&scanNodes->writeNodeID);

    auto debugDump = pipeline.addProcessor<DebugDumpProcessor>();
    debugDump->setInput(&debugDump->inputDump, scanNodesOut);

    PipelineExecutor pipeExec(&pipeline);
    pipeExec.run();
}

TEST_F(ScanNodesProcessorTest, millionNodes) {
    auto db = std::make_unique<DB>();

    TestUtils::generateMillionTestDB(*db, *_jobSystem);
    auto access = db->access();

    Pipeline pipeline;

    auto scanNodes = pipeline.addProcessor<ScanNodesProcessor>(db.get());
    auto scanNodesOut = scanNodes->createOut(&scanNodes->writeNodeID);

    auto discard = pipeline.addProcessor<DiscardProcessor>();
    discard->setInput(&discard->inputData, scanNodesOut);

    PipelineExecutor pipeExec(&pipeline);
    pipeExec.init();

    const ColumnNodes& nodes = *static_cast<ColumnNodes*>(scanNodesOut->getColumn());

    EntityID expectedID = 0;
    while (!pipeExec.isFinished()) {
        pipeExec.executeStep();

        for (EntityID id : nodes) {
            ASSERT_EQ(id, expectedID);
            expectedID++;
        }
    }
}
