#include "gtest/gtest.h"

#include "PipelineManager.h"
#include "PipelineBuilder.h"
#include "ScanNodesProcessor.h"
#include "ColumnNodes.h"
#include "FilterNodesProcessor.h"

using namespace db;

class PipelineTest : public ::testing::Test {
};

TEST_F(PipelineTest, emptyPipeline) {
    PipelineManager pipeMan;
    PipelineBuilder builder(&pipeMan);
    builder.setPipeline(pipeMan.createPipeline());

    ASSERT_TRUE(builder.getPipeline());
}

TEST_F(PipelineTest, createPipelineWithSource) {
    PipelineManager pipeMan;
    PipelineBuilder builder(&pipeMan);

    builder.setPipeline(pipeMan.createPipeline());
    ASSERT_TRUE(builder.getPipeline());

    builder.addSource<ScanNodesProcessor>();

    Pipeline* pipeline = builder.getPipeline();
    PipelineState* state = pipeline->getState();

    // We have only one source node so we should have only 
    // one output column
    ASSERT_EQ(state->size(), 1);

    const auto& processors = pipeline->processors();
    ASSERT_EQ(processors.size(), 1);

    // We check that the only processor that we have in the pipeline
    // is indeed a ScanNodesProcessor
    const auto firstProc = processors.front();
    ASSERT_TRUE(dynamic_cast<const ScanNodesProcessor*>(firstProc));

    // Check that the processor has no inputs as it is a source
    // and a single output column which is a ColumnNodes
    ASSERT_TRUE(firstProc->getInputs().empty());
    const auto& outputs = firstProc->getOutputs();
    ASSERT_EQ(outputs.size(), 1);
    ASSERT_TRUE(dynamic_cast<ColumnNodes*>(outputs.columns().front()));
}

TEST_F(PipelineTest, createPipelineWithSourceAndFilter) {
    PipelineManager pipeMan;
    PipelineBuilder builder(&pipeMan);

    builder.setPipeline(pipeMan.createPipeline());
    ASSERT_TRUE(builder.getPipeline());

    builder.addSource<ScanNodesProcessor>();
    builder.addTransform<FilterNodesProcessor>();

    Pipeline* pipeline = builder.getPipeline();
    PipelineState* state = pipeline->getState();

    // We should have 2 columns
    ASSERT_EQ(state->size(), 2);

    // We should have 2 processors
    const auto& processors = pipeline->processors();
    ASSERT_EQ(processors.size(), 2);

    // Check that the processors are correct
    auto scanProc = dynamic_cast<ScanNodesProcessor*>(processors[0]);
    auto filterProc = dynamic_cast<FilterNodesProcessor*>(processors[1]);
    ASSERT_TRUE(scanProc);
    ASSERT_TRUE(filterProc);

    // ScanNodes must have no inputs
    ASSERT_TRUE(scanProc->getInputs().empty());

    // ScanNodes has one output
    ASSERT_EQ(scanProc->getOutputs().size(), 1);
    ASSERT_TRUE(dynamic_cast<ColumnNodes*>(scanProc->getOutputs().columns().front()));

    // The output of ScanNodes is the input of FilterNodes
    ASSERT_EQ(scanProc->getOutputs().columns(), filterProc->getInputs().columns());

    // FilterNodes has one output
    ASSERT_EQ(filterProc->getOutputs().size(), 1);
    ASSERT_TRUE(dynamic_cast<ColumnNodes*>(filterProc->getOutputs().columns().front()));
}
