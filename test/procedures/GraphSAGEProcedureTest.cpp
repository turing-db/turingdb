#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureManager.h"
#include "ProcedureState.h"
#include "ProcedureTypeVector.h"

#include "Graph.h"
#include "JobSystem.h"
#include "TuringException.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "reader/GraphReader.h"
#include "samplers/GraphSAGESampler.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using NodeCol = ColumnOptVector<NodeID>;

constexpr size_t hops = GraphSAGESampler::hops;
constexpr size_t returnValuesPerHop = 3;
constexpr size_t returnValueCount = hops * returnValuesPerHop;

// A drive that never ends would hang the suite rather than fail it
constexpr size_t stepLimit = 1000;

// The nine return columns of one drive, with the null padding kept: a step hands the
// engine nine columns of one length, and that length is what the row count is read from.
struct SAGERun {
    std::vector<size_t> _stepSizes;
    std::array<std::vector<uint64_t>, returnValueCount> _rows;
};

}

// Drives the registered gnn.graphSAGE against the shared SimpleGraph fixture, through
// the same PREPARE / EXECUTE protocol the engine uses.
class GraphSAGEProcedureTest : public TuringTest {
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

    const Procedure* graphSAGE() const { return _procedures.getProcedure("gnn.graphSAGE"); }

    // A constant list argument, as the engine materialises a list literal: one view over
    // the query-scoped buffer, held by a ColumnConst.
    ListView intList(const std::vector<int64_t>& values) {
        std::vector<ListBuffer<>::ListItemVariant> elements;
        elements.reserve(values.size());

        for (const int64_t value : values) {
            elements.emplace_back(value);
        }

        return _listBuffer.insert(elements);
    }

    // Run the procedure to completion, recording each step's row count and every column's
    // contents. Steps are checked for rectangularity as they are read, since the engine
    // takes the whole step's row count from one column alone.
    void runSAGE(const GraphView& view,
                 const std::vector<int64_t>& seeds,
                 const std::vector<int64_t>& fanouts,
                 std::optional<int64_t> rngSeed,
                 size_t chunkSize,
                 SAGERun& run) {
        const Procedure* procedure = graphSAGE();
        ASSERT_NE(procedure, nullptr);

        ProcedureContext context;
        context.setGraph(_graph.get());
        context.setGraphView(&view);
        context.setChunkSize(chunkSize);

        const ColumnConst<ListView> seedsArg(intList(seeds));
        const ColumnConst<ListView> fanoutsArg(intList(fanouts));
        const ColumnConst<int64_t> rngSeedArg(int64_t {rngSeed.value_or(0)});

        std::array<NodeCol, returnValueCount> columns {};

        ProcedureData* data = procedure->getAllocCallback()();

        data->resizeInputColumns(3);
        data->setInputColumn(0, &seedsArg);
        data->setInputColumn(1, &fanoutsArg);
        data->setInputColumn(2, rngSeed.has_value() ? &rngSeedArg : nullptr);

        data->resizeReturnColumns(returnValueCount);
        for (size_t column = 0; column < returnValueCount; column++) {
            data->setReturnColumn(column, &columns[column]);
        }

        ProcedureState state;
        state.setContext(&context);
        state.setData(data);

        state.setStep(ProcedureState::Step::PREPARE);
        procedure->getExecCallback()(&state);

        while (!state.isFinished() && run._stepSizes.size() < stepLimit) {
            state.setStep(ProcedureState::Step::EXECUTE);
            procedure->getExecCallback()(&state);

            collectStep(columns, run);
        }

        const bool finished = state.isFinished();
        procedure->getDeallocCallback()(data);

        ASSERT_TRUE(finished);
    }

    // Append one step, after checking that all nine columns carry the same rows
    static void collectStep(const std::array<NodeCol, returnValueCount>& columns, SAGERun& run) {
        const size_t stepRows = columns.front().size();

        for (size_t column = 0; column < returnValueCount; column++) {
            ASSERT_EQ(columns[column].size(), stepRows) << "column " << column << " is ragged";
        }

        run._stepSizes.push_back(stepRows);

        for (size_t column = 0; column < returnValueCount; column++) {
            for (const std::optional<NodeID>& node : columns[column]) {
                if (!node.has_value()) {
                    continue;
                }

                run._rows[column].push_back(node->getValue());
            }
        }
    }

    // Everything a run emitted in one column, in the order the steps produced it
    static std::vector<uint64_t> sortedColumn(const SAGERun& run, size_t column) {
        std::vector<uint64_t> values = run._rows[column];
        std::ranges::sort(values);

        return values;
    }

    // PREPARE is where the arguments are validated, so a rejection surfaces there
    void prepareOnly(const GraphView& view,
                     const std::vector<int64_t>& seeds,
                     const std::vector<int64_t>& fanouts) {
        const Procedure* procedure = graphSAGE();
        ASSERT_NE(procedure, nullptr);

        ProcedureContext context;
        context.setGraph(_graph.get());
        context.setGraphView(&view);
        context.setChunkSize(ChunkConfig::CHUNK_SIZE);

        const ColumnConst<ListView> seedsArg(intList(seeds));
        const ColumnConst<ListView> fanoutsArg(intList(fanouts));

        std::array<NodeCol, returnValueCount> columns {};

        ProcedureData* data = procedure->getAllocCallback()();

        data->resizeInputColumns(3);
        data->setInputColumn(0, &seedsArg);
        data->setInputColumn(1, &fanoutsArg);
        data->setInputColumn(2, nullptr);

        data->resizeReturnColumns(returnValueCount);
        for (size_t column = 0; column < returnValueCount; column++) {
            data->setReturnColumn(column, &columns[column]);
        }

        ProcedureState state;
        state.setContext(&context);
        state.setData(data);
        state.setStep(ProcedureState::Step::PREPARE);

        // The callback throws through this, so the data is released before it leaves
        try {
            procedure->getExecCallback()(&state);
        } catch (...) {
            procedure->getDeallocCallback()(data);
            throw;
        }

        procedure->getDeallocCallback()(data);
    }

    ProcedureManager _procedures;
    ListBuffer<> _listBuffer;
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<JobSystem> _jobSystem;
};

// The call takes two constant lists and an optional seed, and answers with three columns
// per hop. Every argument is constant, so the call is not driven per row.
TEST_F(GraphSAGEProcedureTest, registersTheExpectedSignature) {
    const Procedure* procedure = graphSAGE();
    ASSERT_NE(procedure, nullptr);

    const ProcedureTypeVector& arguments = procedure->argumentTypes();
    ASSERT_EQ(arguments.size(), 3U);
    EXPECT_EQ(procedure->getRequiredArgumentCount(), 2U);

    for (const NamedProcedureType& argument : arguments) {
        EXPECT_TRUE(argument._constant) << argument._name << " is not a constant argument";
    }

    EXPECT_FALSE(procedure->hasRowAlignedArgument());

    const ProcedureTypeVector& returnValues = procedure->returnValues();
    ASSERT_EQ(returnValues.size(), returnValueCount);

    for (size_t hop = 0; hop < hops; hop++) {
        const std::string suffix = std::to_string(hop);
        const size_t base = hop * returnValuesPerHop;

        EXPECT_EQ(returnValues[base]._name, "dst_nodes" + suffix);
        EXPECT_EQ(returnValues[base + 1]._name, "src_nodes" + suffix);
        EXPECT_EQ(returnValues[base + 2]._name, "tgt_nodes" + suffix);

        EXPECT_EQ(returnValues[base]._type, ProcedureType::NODE);
        EXPECT_EQ(returnValues[base + 1]._type, ProcedureType::NODE);
        EXPECT_EQ(returnValues[base + 2]._type, ProcedureType::NODE);
    }
}

// Every return value is named, so a yield of one of them resolves to its own index
TEST_F(GraphSAGEProcedureTest, everyReturnValueResolvesByName) {
    const Procedure* procedure = graphSAGE();
    ASSERT_NE(procedure, nullptr);

    std::set<size_t> indices;
    for (size_t hop = 0; hop < hops; hop++) {
        const std::string suffix = std::to_string(hop);

        indices.insert(procedure->getReturnValueIndex("dst_nodes" + suffix));
        indices.insert(procedure->getReturnValueIndex("src_nodes" + suffix));
        indices.insert(procedure->getReturnValueIndex("tgt_nodes" + suffix));
    }

    EXPECT_EQ(indices.size(), returnValueCount) << "two return values share an index";
}

// The caller budgeted two rows a step, so no step may answer with more.
TEST_F(GraphSAGEProcedureTest, stepsHonourTheContextChunkSize) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    constexpr size_t chunkSize = 2;

    SAGERun run;
    runSAGE(reader.getView(), {0, 1, 8, 9, 11}, {2, 2, 2}, 42, chunkSize, run);

    ASSERT_FALSE(run._stepSizes.empty());

    for (const size_t stepSize : run._stepSizes) {
        EXPECT_LE(stepSize, chunkSize);
    }
}

// The budget is honoured at every size, not only the one the engine happens to pass
TEST_F(GraphSAGEProcedureTest, everyChunkSizeIsHonoured) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    for (const size_t chunkSize : {2, 3, 4, 8, 16}) {
        SAGERun run;
        runSAGE(reader.getView(), {0, 1, 8, 9, 11, 12, 15, 17}, {2, 2, 2}, 7, chunkSize, run);

        ASSERT_FALSE(run._stepSizes.empty()) << "chunk size " << chunkSize << " ran no step";

        for (const size_t stepSize : run._stepSizes) {
            EXPECT_LE(stepSize, chunkSize) << "chunk size " << chunkSize << " overran";
        }
    }
}

// A small budget takes more steps to hand over the same sample than a large one
TEST_F(GraphSAGEProcedureTest, aSmallerChunkTakesMoreSteps) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const GraphView& view = reader.getView();

    SAGERun chunked;
    runSAGE(view, {0, 1, 8, 9, 11}, {2, 2, 2}, 42, 2, chunked);

    SAGERun whole;
    runSAGE(view, {0, 1, 8, 9, 11}, {2, 2, 2}, 42, ChunkConfig::CHUNK_SIZE, whole);

    EXPECT_GT(chunked._stepSizes.size(), whole._stepSizes.size());
}

// Chunking is how the rows are handed over, not which rows they are
TEST_F(GraphSAGEProcedureTest, chunkingLeavesTheSampleUnchanged) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const GraphView& view = reader.getView();

    SAGERun whole;
    runSAGE(view, {0, 1, 8, 9, 11}, {3, 3, 3}, 99, ChunkConfig::CHUNK_SIZE, whole);

    for (const size_t chunkSize : {3, 4, 8}) {
        SAGERun chunked;
        runSAGE(view, {0, 1, 8, 9, 11}, {3, 3, 3}, 99, chunkSize, chunked);

        for (size_t column = 0; column < returnValueCount; column++) {
            EXPECT_EQ(sortedColumn(whole, column), sortedColumn(chunked, column))
                << "column " << column << " differs at chunk size " << chunkSize;
        }
    }
}

// A seeded call is reproducible, step for step
TEST_F(GraphSAGEProcedureTest, sameSeedRepeatsTheSample) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const GraphView& view = reader.getView();

    SAGERun first;
    runSAGE(view, {0, 8, 9, 11, 15}, {2, 3, 2}, 1234, 8, first);

    SAGERun second;
    runSAGE(view, {0, 8, 9, 11, 15}, {2, 3, 2}, 1234, 8, second);

    EXPECT_EQ(first._stepSizes, second._stepSizes);

    for (size_t column = 0; column < returnValueCount; column++) {
        EXPECT_EQ(first._rows[column], second._rows[column]) << "column " << column << " differs";
    }
}

// The seed argument reaches the sampling. Gym (13) is the only node with more in-edges
// than the fanout, so it is the only seed whose sample the RNG has a say in.
TEST_F(GraphSAGEProcedureTest, differentSeedsDrawDifferently) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const GraphView& view = reader.getView();

    SAGERun first;
    runSAGE(view, {13}, {2, 2, 2}, 101, ChunkConfig::CHUNK_SIZE, first);

    SAGERun second;
    runSAGE(view, {13}, {2, 2, 2}, 202, ChunkConfig::CHUNK_SIZE, second);

    bool anyDifference = false;
    for (size_t column = 0; column < returnValueCount; column++) {
        if (first._rows[column] != second._rows[column]) {
            anyDifference = true;
        }
    }

    EXPECT_TRUE(anyDifference) << "two seeds drew the identical sample";
}

// Omitting the optional seed leaves the call unseeded, which must still run
TEST_F(GraphSAGEProcedureTest, theSeedArgumentIsOptional) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    SAGERun run;
    runSAGE(reader.getView(), {0, 8}, {2, 2, 2}, std::nullopt, ChunkConfig::CHUNK_SIZE, run);

    EXPECT_FALSE(run._stepSizes.empty());
}

// The first hop generates embeddings for exactly the seeds it was called with
TEST_F(GraphSAGEProcedureTest, firstHopDstNodesAreTheSeeds) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    SAGERun run;
    runSAGE(reader.getView(), {9, 0, 8}, {2, 2, 2}, 3, ChunkConfig::CHUNK_SIZE, run);

    const std::vector<uint64_t> expected {0, 8, 9};
    EXPECT_EQ(sortedColumn(run, 0), expected);
}

// A seed the graph does not hold reaches nothing, and the call still finishes
TEST_F(GraphSAGEProcedureTest, anUnknownSeedFinishesWithNoEdgeRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    SAGERun run;
    runSAGE(reader.getView(), {9999}, {2, 2, 2}, 5, ChunkConfig::CHUNK_SIZE, run);

    for (size_t hop = 0; hop < hops; hop++) {
        const size_t base = hop * returnValuesPerHop;

        EXPECT_TRUE(run._rows[base + 1].empty()) << "hop " << hop << " emitted a source";
        EXPECT_TRUE(run._rows[base + 2].empty()) << "hop " << hop << " emitted a target";
    }
}

// The fanout list names one sample size per hop, so a list of any other length names
// something the call cannot act on
TEST_F(GraphSAGEProcedureTest, rejectsAFanoutListOfTheWrongSize) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const GraphView& view = reader.getView();

    EXPECT_THROW(prepareOnly(view, {0}, {2, 2}), TuringException);
    EXPECT_THROW(prepareOnly(view, {0}, {2, 2, 2, 2}), TuringException);
    EXPECT_THROW(prepareOnly(view, {0}, {}), TuringException);
}

// One node's sample is emitted whole, so a fanout wider than a chunk could not be
// returned without a step running over the row budget it promises
TEST_F(GraphSAGEProcedureTest, rejectsAFanoutWiderThanAChunk) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const GraphView& view = reader.getView();

    const int64_t overWide = static_cast<int64_t>(ChunkConfig::CHUNK_SIZE) + 1;

    EXPECT_THROW(prepareOnly(view, {0}, {overWide, 2, 2}), TuringException);
    EXPECT_THROW(prepareOnly(view, {0}, {2, overWide, 2}), TuringException);
    EXPECT_THROW(prepareOnly(view, {0}, {2, 2, overWide}), TuringException);

    EXPECT_NO_THROW(prepareOnly(view, {0}, {static_cast<int64_t>(ChunkConfig::CHUNK_SIZE), 1, 1}));
}

// A fanout of zero samples nothing for that hop, which is a call that returns the seeds
// rather than one the procedure turns away
TEST_F(GraphSAGEProcedureTest, acceptsAZeroFanout) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    SAGERun run;
    runSAGE(reader.getView(), {0, 8}, {0, 2, 2}, 11, ChunkConfig::CHUNK_SIZE, run);

    const std::vector<uint64_t> expected {0, 8};
    EXPECT_EQ(sortedColumn(run, 0), expected);

    EXPECT_TRUE(run._rows[1].empty()) << "a zero fanout emitted a source";
    EXPECT_TRUE(run._rows[2].empty()) << "a zero fanout emitted a target";
}

// RESET rewinds a prepared call so it can be driven again, which is how a call nested
// under a loop is re-run. The second drive must be the first one over.
TEST_F(GraphSAGEProcedureTest, resetRewindsThePreparedCall) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const GraphView& view = reader.getView();

    const Procedure* procedure = graphSAGE();
    ASSERT_NE(procedure, nullptr);

    ProcedureContext context;
    context.setGraph(_graph.get());
    context.setGraphView(&view);
    context.setChunkSize(4);

    const ColumnConst<ListView> seedsArg(intList({0, 8, 9}));
    const ColumnConst<ListView> fanoutsArg(intList({2, 2, 2}));
    const ColumnConst<int64_t> rngSeedArg(int64_t {77});

    std::array<NodeCol, returnValueCount> columns {};

    ProcedureData* data = procedure->getAllocCallback()();

    data->resizeInputColumns(3);
    data->setInputColumn(0, &seedsArg);
    data->setInputColumn(1, &fanoutsArg);
    data->setInputColumn(2, &rngSeedArg);

    data->resizeReturnColumns(returnValueCount);
    for (size_t column = 0; column < returnValueCount; column++) {
        data->setReturnColumn(column, &columns[column]);
    }

    ProcedureState state;
    state.setContext(&context);
    state.setData(data);

    state.setStep(ProcedureState::Step::PREPARE);
    procedure->getExecCallback()(&state);

    const auto drive = [&](SAGERun& run) {
        while (!state.isFinished() && run._stepSizes.size() < stepLimit) {
            state.setStep(ProcedureState::Step::EXECUTE);
            procedure->getExecCallback()(&state);

            collectStep(columns, run);
        }
    };

    SAGERun first;
    drive(first);
    ASSERT_TRUE(state.isFinished());

    state.clearFinished();
    state.setStep(ProcedureState::Step::RESET);
    procedure->getExecCallback()(&state);

    SAGERun second;
    drive(second);
    const bool finishedAgain = state.isFinished();

    procedure->getDeallocCallback()(data);

    ASSERT_TRUE(finishedAgain) << "the call did not finish after a reset";
    EXPECT_EQ(sortedColumn(first, 0), sortedColumn(second, 0))
        << "the seeds were not rebuilt by the reset";
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
