#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureManager.h"
#include "ProcedureState.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIndices.h"
#include "columns/ColumnVector.h"
#include "versioning/Transaction.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// A drive that never ends would hang the suite rather than fail it
constexpr size_t stepLimit = 100;

struct DescribeRun {
    std::vector<uint64_t> nodeCounts;
    std::vector<size_t> inputRows;
    std::vector<size_t> stepSizes;
};

}

// db.describeCommit answers one chunk of its input hashes a step, so a drive over more
// hashes than the chunk holds has to resume where the last step stopped and stop at the
// last hash - not read the same count of rows from every cursor it reaches.
class DescribeCommitChunkTest : public TuringTest {
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

    // Drives the procedure over the given commit hashes until it declares itself finished,
    // recording every step's rows and how many of them there were.
    void runDescribe(const ColumnVector<std::string>& commits, size_t chunkSize, DescribeRun& run) {
        const Procedure* procedure = _procedures.getProcedure("db.describeCommit");
        ASSERT_NE(procedure, nullptr);

        Transaction transaction(_graph->openTransaction());

        ProcedureContext context;
        context.setGraph(_graph.get());
        context.setTransaction(&transaction);
        context.setChunkSize(chunkSize);

        ColumnVector<uint64_t> nodeCounts;
        ColumnVector<uint64_t> edgeCounts;
        ColumnVector<uint64_t> partCounts;
        ColumnIndices indices;

        ProcedureData* data = procedure->getAllocCallback()();

        data->resizeInputColumns(1);
        data->setInputColumn(0, &commits);

        data->resizeReturnColumns(3);
        data->setReturnColumn(0, &nodeCounts);
        data->setReturnColumn(1, &edgeCounts);
        data->setReturnColumn(2, &partCounts);

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

            run.stepSizes.push_back(nodeCounts.size());
            run.nodeCounts.insert(run.nodeCounts.end(), nodeCounts.begin(), nodeCounts.end());
            run.inputRows.insert(run.inputRows.end(), indices.begin(), indices.end());
        }

        const bool finished = state.isFinished();
        procedure->getDeallocCallback()(data);

        ASSERT_TRUE(finished);
    }

    void headCommitHash(std::string& hash) const {
        const Transaction transaction(_graph->openTransaction());
        hash = fmt::format("{:x}", transaction.getCommitHash().get());
    }

    ProcedureManager _procedures;
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<JobSystem> _jobSystem;
};

// Ten hashes four at a time is three steps of 4, 4 and 2 - the last step reads what is
// left, and every hash is described exactly once.
TEST_F(DescribeCommitChunkTest, describesEveryHashOfAnInputLongerThanTheChunk) {
    std::string hash;
    headCommitHash(hash);

    constexpr size_t rowCount = 10;
    constexpr size_t chunkSize = 4;

    ColumnVector<std::string> commits;
    for (size_t row = 0; row < rowCount; row++) {
        commits.push_back(hash);
    }

    DescribeRun run;
    runDescribe(commits, chunkSize, run);

    const std::vector<size_t> expectedSteps {4, 4, 2};
    EXPECT_EQ(run.stepSizes, expectedSteps);

    ASSERT_EQ(run.nodeCounts.size(), rowCount);
    ASSERT_EQ(run.inputRows.size(), rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        EXPECT_EQ(run.inputRows[row], row);
        EXPECT_GT(run.nodeCounts[row], 0u);
        EXPECT_EQ(run.nodeCounts[row], run.nodeCounts[0]);
    }
}

// An input the chunk holds whole is one step, and an input the chunk divides exactly
// ends on its last full step rather than on an empty one.
TEST_F(DescribeCommitChunkTest, endsOnTheLastFullStep) {
    std::string hash;
    headCommitHash(hash);

    ColumnVector<std::string> commits;
    for (size_t row = 0; row < 4; row++) {
        commits.push_back(hash);
    }

    DescribeRun exact;
    runDescribe(commits, /*chunkSize=*/2, exact);

    const std::vector<size_t> expectedExact {2, 2};
    EXPECT_EQ(exact.stepSizes, expectedExact);

    DescribeRun whole;
    runDescribe(commits, /*chunkSize=*/16, whole);

    const std::vector<size_t> expectedWhole {4};
    EXPECT_EQ(whole.stepSizes, expectedWhole);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
