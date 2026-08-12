#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <tuple>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"

#include "Graph.h"
#include "JobSystem.h"
#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureManager.h"
#include "ProcedureNamespace.h"
#include "ProcedureState.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListBuffer.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "DBDialect.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the (n, m, value) rows the two carried match columns and the yielded value form.
class NodePairValueSink : public NLOutputSink {
public:
    using Row = std::tuple<uint64_t, uint64_t, types::Int64::Primitive>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* leftNodes = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* rightNodes = dynamic_cast<const ColumnVector<NodeID>*>(chunks[1]);
        const auto* values = dynamic_cast<const ColumnVector<types::Int64::Primitive>*>(chunks[2]);
        ASSERT_NE(leftNodes, nullptr);
        ASSERT_NE(rightNodes, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(leftNodes->size(), values->size());
        ASSERT_EQ(rightNodes->size(), values->size());

        const auto& leftRaw = leftNodes->getRaw();
        const auto& rightRaw = rightNodes->getRaw();
        const auto& valuesRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(leftRaw[rowIndex].getValue(), rightRaw[rowIndex].getValue(), valuesRaw[rowIndex]);
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

struct PairData : public IndexedProcedureData {};

// test.pairNodeIDs(leftNodeIDs, rightNodeIDs) YIELD value: one row per input row, holding
// left * 100 + right - so a value names the exact (n, m) pair it was computed from and a
// mispaired product row cannot produce a correct one.
void pairExecuteImpl(ProcedureState* procedureState) {
    PairData& data = procedureState->data<PairData>();

    const auto* leftNodeIDs = static_cast<const ColumnVector<NodeID>*>(data.getInputColumn(0));
    const auto* rightNodeIDs = static_cast<const ColumnVector<NodeID>*>(data.getInputColumn(1));
    auto* values = static_cast<ColumnVector<types::Int64::Primitive>*>(data.getReturnColumn(0));
    ColumnIndices* indices = data.indices();

    values->clear();

    const auto& leftRaw = leftNodeIDs->getRaw();
    const auto& rightRaw = rightNodeIDs->getRaw();
    for (size_t inputRow = 0; inputRow < leftRaw.size(); inputRow++) {
        const types::Int64::Primitive left = static_cast<types::Int64::Primitive>(leftRaw[inputRow].getValue());
        const types::Int64::Primitive right = static_cast<types::Int64::Primitive>(rightRaw[inputRow].getValue());

        values->push_back(left * 100 + right);

        if (indices) {
            indices->push_back(inputRow);
        }
    }

    procedureState->finish();
}

void pairExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        pairExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* pairAlloc() {
    return new PairData();
}

void pairDealloc(ProcedureData* data) {
    delete data;
}

}

class CallCrossProductArgumentsTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _procedures.init();

        // No registered procedure takes two per-row node columns, so the call whose
        // arguments come from both factors of a MATCH's cross product needs a
        // test-local one.
        ProcedureNamespace* testNamespace = _procedures.createNamespace("test");

        Procedure* pairProcedure = new Procedure("pairNodeIDs");
        pairProcedure->setAllocCallback(&pairAlloc);
        pairProcedure->setDeallocCallback(&pairDealloc);
        pairProcedure->setExecuteCallback(&pairExecute);
        pairProcedure->setHasIndices(true);
        pairProcedure->addArgument("leftNodeIDs", ProcedureType::NODE);
        pairProcedure->addArgument("rightNodeIDs", ProcedureType::NODE);
        pairProcedure->addReturnValue("value", ProcedureType::INT64);
        testNamespace->addProcedure(pairProcedure);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Three Person nodes (0..2) and two Device nodes (3, 4), so the two labelled scans
    // select disjoint sets and their product is six pairs.
    std::unique_ptr<Graph> buildTwoLabelGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("Person");
        metadata.getOrCreateLabel("Device");

        const LabelSet personLabelSet = LabelSet::fromList({0});
        const LabelSet deviceLabelSet = LabelSet::fromList({1});
        for (size_t nodeIndex = 0; nodeIndex < 3; nodeIndex++) {
            builder.addNode(personLabelSet);
        }

        for (size_t nodeIndex = 0; nodeIndex < 2; nodeIndex++) {
            builder.addNode(deviceLabelSet);
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Runs a Cypher query the whole way down: parsed and analyzed against the test's own
    // procedure registry, generated into db dialect by DBProgramGenerator, lowered and
    // executed - the path a CALL takes from the query text. The chunk size is exposed so
    // a test can force the product to span chunk boundaries.
    void runQuery(const char* queryText,
                  const GraphView& view,
                  NLOutputSink& sink,
                  size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        CypherAST ast(&_procedures, queryText);

        CypherParser parser(&ast);
        parser.parse(queryText);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = dbModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view, &_procedures);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;

        ProcedureContext procedureContext;
        procedureContext.setGraphView(&view);
        procedureContext.setProcedures(&_procedures);
        procedureContext.setChunkSize(chunkSize);
        procedureContext.setListBuffer(&memory.listBuffer());

        NLInterpreter interpreter(*nlModule,
                                  &view,
                                  &sink,
                                  &memory,
                                  chunkSize,
                                  /*writeBuffer=*/nullptr,
                                  /*metadataBuilder=*/nullptr,
                                  &procedureContext);
        interpreter.run();
    }

    ProcedureManager _procedures;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(CallCrossProductArgumentsTest, callReadsBothColumnsOfTheMatchCrossProduct) {
    auto graph = buildTwoLabelGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // MATCH (n:Person), (m:Device) CALL test.pairNodeIDs(n, m) YIELD value RETURN n, m,
    // value: the two disconnected scans cross into six (n, m) pairs before the call, and
    // the call reads both product columns - so each value proves its row paired the
    // factors correctly, and six rows prove the product was complete.
    NodePairValueSink sink;
    runQuery("MATCH (n:Person), (m:Device) CALL test.pairNodeIDs(n, m) YIELD value RETURN n, m, value",
             reader.getView(),
             sink);

    std::vector<NodePairValueSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<NodePairValueSink::Row> expected {{0, 3, 3},
                                                        {0, 4, 4},
                                                        {1, 3, 103},
                                                        {1, 4, 104},
                                                        {2, 3, 203},
                                                        {2, 4, 204}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallCrossProductArgumentsTest, callReadsBothColumnsAcrossChunkBoundaries) {
    auto graph = buildTwoLabelGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // At chunk size two the three-node Person scan spans two chunks, so the product
    // re-runs its inner factor per outer chunk and the call is driven once per product
    // chunk - the six pairs must still come out exactly once each.
    NodePairValueSink sink;
    runQuery("MATCH (n:Person), (m:Device) CALL test.pairNodeIDs(n, m) YIELD value RETURN n, m, value",
             reader.getView(),
             sink,
             /*chunkSize=*/2);

    std::vector<NodePairValueSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<NodePairValueSink::Row> expected {{0, 3, 3},
                                                        {0, 4, 4},
                                                        {1, 3, 103},
                                                        {1, 4, 104},
                                                        {2, 3, 203},
                                                        {2, 4, 204}};
    EXPECT_EQ(rows, expected);
}
