#include <memory>

#include "gtest/gtest.h"

#include "DB.h"
#include "Pipeline.h"
#include "ScanNodesProcessor.h"
#include "DiscardProcessor.h"
#include "ColumnNodes.h"
#include "Stream.h"

using namespace db;

class PipelineTest : public ::testing::Test {
};

TEST_F(PipelineTest, createSmallPipeline) {
    auto db = std::make_unique<DB>();
    auto access = db->access();
    Pipeline pipe;

    auto scanNodes = pipe.addProcessor<ScanNodesProcessor>(db.get());
    auto scanNodeOut = scanNodes->createOut(&scanNodes->writeNodeID);
    ASSERT_TRUE(scanNodeOut);

    auto discard = pipe.addProcessor<DiscardProcessor>();
    discard->setInput(&discard->inputData, scanNodeOut);

    pipe.setup();

    ASSERT_EQ(pipe.processors().size(), 2);
    ASSERT_EQ(pipe.sources().size(), 1);

    // Check scan nodes
    ASSERT_TRUE(scanNodes->inputs().empty());
    ASSERT_EQ(scanNodes->outputs().size(), 1);
    for (const auto& [writeFunc, setupFunc, stream] : scanNodes->outputs()) {
        ASSERT_EQ(writeFunc, &scanNodes->writeNodeID);
        ASSERT_TRUE(stream != nullptr);
        ASSERT_EQ(stream, scanNodeOut);

        ASSERT_EQ(stream->getDriver(), scanNodes);
        ASSERT_EQ(stream->getReceiver(), discard);
        ASSERT_TRUE(stream->getColumn() != nullptr);
        ASSERT_TRUE(dynamic_cast<ColumnNodes*>(stream->getColumn()));
    }

    // Check discard
    ASSERT_TRUE(discard->outputs().empty());
    ASSERT_EQ(discard->inputs().size(), 1);
    for (const auto& [tag, stream] : discard->inputs()) {
        ASSERT_EQ(tag, &discard->inputData);
        ASSERT_EQ(stream, scanNodeOut);
    }
}
