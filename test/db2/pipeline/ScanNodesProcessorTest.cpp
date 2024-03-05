#include <gtest/gtest.h>
#include <memory>

#include "ScanNodesProcessor.h"
#include "Pipeline.h"
#include "PipelineExecutor.h"
#include "DebugDumpProcessor.h"
#include "DiscardProcessor.h"

#include "DB.h"
#include "DBAccess.h"
#include "TestUtils.h"
#include "Stream.h"
#include "ColumnNodes.h"

using namespace db;

class ScanNodesProcessorTest : public ::testing::Test {
protected:
    void SetUp() override {
    }

    void TearDown() override {
    }
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
    auto access = db->uniqueAccess();

    TestUtils::generateMillionTestDB(access);

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
