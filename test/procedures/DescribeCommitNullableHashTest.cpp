#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>
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
#include "columns/ColumnConst.h"
#include "columns/ColumnIndices.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "versioning/Transaction.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t STEP_LIMIT = 100;

struct DescribeRun {
    std::vector<uint64_t> nodeCounts;
    std::vector<size_t> inputRows;
    std::vector<size_t> stepSizes;
};

}

class DescribeCommitNullableHashTest : public TuringTest {
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

    void runDescribe(const Column* commits, size_t chunkSize, DescribeRun& run) {
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
        data->setInputColumn(0, commits);

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

        while (!state.isFinished() && run.stepSizes.size() < STEP_LIMIT) {
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
        hash = fmt::format("{}", transaction.getCommitHash());
    }

    ProcedureManager _procedures;
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(DescribeCommitNullableHashTest, answersANullHashRowWithZeroCounts) {
    std::string hash;
    headCommitHash(hash);

    ColumnOptVector<std::string_view> commits;
    commits.push_back(std::string_view {hash});
    commits.pushNull();
    commits.push_back(std::string_view {hash});
    commits.pushNull();

    DescribeRun run;
    runDescribe(&commits, 3, run);

    const std::vector<size_t> expectedSteps {3, 1};
    EXPECT_EQ(run.stepSizes, expectedSteps);

    const std::vector<size_t> expectedRows {0, 1, 2, 3};
    EXPECT_EQ(run.inputRows, expectedRows);

    ASSERT_EQ(run.nodeCounts.size(), 4u);
    EXPECT_GT(run.nodeCounts[0], 0u);
    EXPECT_EQ(run.nodeCounts[1], 0u);
    EXPECT_EQ(run.nodeCounts[2], run.nodeCounts[0]);
    EXPECT_EQ(run.nodeCounts[3], 0u);
}

TEST_F(DescribeCommitNullableHashTest, describesANullableConstantHash) {
    std::string hash;
    headCommitHash(hash);

    const ColumnConst<std::optional<std::string_view>> commit(std::optional<std::string_view> {hash});

    DescribeRun run;
    runDescribe(&commit, 4, run);

    ASSERT_EQ(run.nodeCounts.size(), 1u);
    EXPECT_GT(run.nodeCounts[0], 0u);

    const ColumnConst<std::optional<std::string_view>> noCommit(std::optional<std::string_view> {});

    DescribeRun nullRun;
    runDescribe(&noCommit, 4, nullRun);

    const std::vector<uint64_t> expectedCounts {0};
    EXPECT_EQ(nullRun.nodeCounts, expectedCounts);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
