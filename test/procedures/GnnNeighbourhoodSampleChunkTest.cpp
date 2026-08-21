#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureManager.h"
#include "ProcedureState.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// A drive that never ends would hang the suite rather than fail it
constexpr size_t stepLimit = 1000;

struct SampleRun {
    std::vector<NodeID> sampled;
    std::vector<size_t> stepSizes;
};

}

// What one step of gnn.neighbourhoodSample is allowed to emit. The engine hands the
// procedure the chunk size it budgeted for, so a step that fills to the engine's maximum
// instead answers with a chunk nobody asked for - and takes one step where the caller
// asked for several.
class GnnNeighbourhoodSampleChunkTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        _procedures.init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Drives the procedure over the Person nodes of simpledb until it declares itself
    // finished, recording every step's rows and how many of them there were.
    void runSample(const GraphView& view, int64_t sampleSize, size_t chunkSize, SampleRun& run) {
        const Procedure* procedure = _procedures.getProcedure("gnn.neighbourhoodSample");
        ASSERT_NE(procedure, nullptr);

        ProcedureContext context;
        context.setGraph(_graph.get());
        context.setGraphView(&view);
        context.setChunkSize(chunkSize);

        const ColumnNodeIDs input = {0, 1, 7, 9, 11, 12, 13, 16};
        const ColumnConst<int64_t> sampleSizeArg(int64_t {sampleSize});
        const ColumnConst<int64_t> seedArg(int64_t {42});

        ColumnNodeIDs srcIDs;
        ColumnEdgeIDs edgeIDs;
        ColumnEdgeTypes edgeTypes;
        ColumnNodeIDs tgtIDs;
        ColumnIndices indices;

        ProcedureData* data = procedure->getAllocCallback()();

        data->resizeInputColumns(3);
        data->setInputColumn(0, &input);
        data->setInputColumn(1, &sampleSizeArg);
        data->setInputColumn(2, &seedArg);

        data->resizeReturnColumns(4);
        data->setReturnColumn(0, &srcIDs);
        data->setReturnColumn(1, &edgeIDs);
        data->setReturnColumn(2, &edgeTypes);
        data->setReturnColumn(3, &tgtIDs);

        IndexedProcedureData* indexedData = static_cast<IndexedProcedureData*>(data);
        indexedData->setIndices(&indices);

        ProcedureState state;
        state.setContext(&context);
        state.setData(data);

        state.setStep(ProcedureState::Step::PREPARE);
        procedure->getExecCallback()(&state);

        while (!state.isFinished() && run.stepSizes.size() < stepLimit) {
            state.setStep(ProcedureState::Step::EXECUTE);
            procedure->getExecCallback()(&state);

            run.stepSizes.push_back(tgtIDs.size());
            run.sampled.insert(run.sampled.end(), tgtIDs.begin(), tgtIDs.end());
        }

        const bool finished = state.isFinished();
        procedure->getDeallocCallback()(data);

        ASSERT_TRUE(finished);
    }

    ProcedureManager _procedures;
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<JobSystem> _jobSystem;
};

// The caller budgeted two rows a step, so no step may answer with more.
TEST_F(GnnNeighbourhoodSampleChunkTest, stepsHonourTheContextChunkSize) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    constexpr size_t chunkSize = 2;

    SampleRun run;
    runSample(reader.getView(), /*sampleSize=*/2, chunkSize, run);

    ASSERT_FALSE(run.stepSizes.empty());

    for (const size_t stepSize : run.stepSizes) {
        EXPECT_LE(stepSize, chunkSize);
    }
}

// Chunking is how many steps the sample takes, not what it samples: the same seed over
// the same nodes reaches the same rows whether they come two at a time or all at once.
TEST_F(GnnNeighbourhoodSampleChunkTest, chunkingLeavesTheSampleUnchanged) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const GraphView& view = reader.getView();

    SampleRun chunked;
    runSample(view, /*sampleSize=*/2, /*chunkSize=*/2, chunked);

    SampleRun whole;
    runSample(view, /*sampleSize=*/2, ChunkConfig::CHUNK_SIZE, whole);

    EXPECT_EQ(chunked.sampled, whole.sampled);
    EXPECT_GT(chunked.stepSizes.size(), whole.stepSizes.size());
}

// A node's sample is emitted whole, so a sample larger than the chunk grows the step to
// hold it - the one case where the budget cannot be met, and still bounded by the sample.
TEST_F(GnnNeighbourhoodSampleChunkTest, aSampleLargerThanTheChunkIsEmittedWhole) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    constexpr size_t sampleSize = 3;

    SampleRun run;
    runSample(reader.getView(), sampleSize, /*chunkSize=*/2, run);

    ASSERT_FALSE(run.stepSizes.empty());

    for (const size_t stepSize : run.stepSizes) {
        EXPECT_LE(stepSize, sampleSize);
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
