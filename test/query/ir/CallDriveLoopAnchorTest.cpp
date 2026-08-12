#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"

#include "Graph.h"
#include "JobSystem.h"
#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcUtils.h"
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

// Collects the (node, value) rows the carried match column and the yielded value form.
class NodeValueSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, types::Int64::Primitive>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodes = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnVector<types::Int64::Primitive>*>(chunks[1]);
        ASSERT_NE(nodes, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(nodes->size(), values->size());

        const auto& nodesRaw = nodes->getRaw();
        const auto& valuesRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(nodesRaw[rowIndex].getValue(), valuesRaw[rowIndex]);
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

struct ScaleData : public IndexedProcedureData {};

// test.scaleNodeID(factor, nodeIDs) YIELD value: one row per input node, holding the
// node ID scaled by the constant factor. The scalar comes first and the per-row column
// second - an argument order no registered procedure declares - putting the loop-bound
// chunk at a position the drive-loop anchor must still find.
void scaleExecuteImpl(ProcedureState* procedureState) {
    ScaleData& data = procedureState->data<ScaleData>();

    const types::Int64::Primitive factor =
        ProcUtils::constArg<types::Int64::Primitive>(data.getInputColumn(0),
                                                     "test.scaleNodeID: factor must be a constant int");

    const auto* nodeIDs = static_cast<const ColumnVector<NodeID>*>(data.getInputColumn(1));
    auto* values = static_cast<ColumnVector<types::Int64::Primitive>*>(data.getReturnColumn(0));
    ColumnIndices* indices = data.indices();

    values->clear();

    const auto& nodeIDsRaw = nodeIDs->getRaw();
    for (size_t inputRow = 0; inputRow < nodeIDsRaw.size(); inputRow++) {
        values->push_back(factor * static_cast<types::Int64::Primitive>(nodeIDsRaw[inputRow].getValue()));

        if (indices) {
            indices->push_back(inputRow);
        }
    }

    procedureState->finish();
}

void scaleExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        scaleExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* scaleAlloc() {
    return new ScaleData();
}

void scaleDealloc(ProcedureData* data) {
    delete data;
}

}

class CallDriveLoopAnchorTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _procedures.init();

        // No registered procedure declares a scalar argument ahead of a per-row one, so
        // the shape that leaves the call's first operand loop-invariant comes from a
        // test-local procedure.
        ProcedureNamespace* testNamespace = _procedures.createNamespace("test");

        Procedure* scaleProcedure = new Procedure("scaleNodeID");
        scaleProcedure->setAllocCallback(&scaleAlloc);
        scaleProcedure->setDeallocCallback(&scaleDealloc);
        scaleProcedure->setExecuteCallback(&scaleExecute);
        scaleProcedure->setHasIndices(true);
        scaleProcedure->addArgument("factor", ProcedureType::INT64);
        scaleProcedure->addArgument("nodeIDs", ProcedureType::NODE);
        scaleProcedure->addReturnValue("value", ProcedureType::INT64);
        testNamespace->addProcedure(scaleProcedure);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<Graph> buildFiveNodeGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("Node");

        const LabelSet labelset = LabelSet::fromList({0});
        for (size_t nodeIndex = 0; nodeIndex < 5; nodeIndex++) {
            builder.addNode(labelset);
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Runs a Cypher query the whole way down: parsed and analyzed against the test's own
    // procedure registry, generated into db dialect by DBProgramGenerator, lowered and
    // executed - the path a CALL takes from the query text.
    void runQuery(const char* queryText, const GraphView& view, NLOutputSink& sink) {
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
        procedureContext.setChunkSize(ChunkConfig::CHUNK_SIZE);
        procedureContext.setListBuffer(&memory.listBuffer());

        NLInterpreter interpreter(*nlModule,
                                  &view,
                                  &sink,
                                  &memory,
                                  ChunkConfig::CHUNK_SIZE,
                                  /*writeBuffer=*/nullptr,
                                  /*metadataBuilder=*/nullptr,
                                  &procedureContext);
        interpreter.run();
    }

    ProcedureManager _procedures;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(CallDriveLoopAnchorTest, anchorsTheDriveLoopOnALaterLoopBoundArgument) {
    auto graph = buildFiveNodeGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // MATCH (n) CALL test.scaleNodeID(100, n) YIELD value RETURN n, value: the call's
    // first argument is a constant, hoisted to the entry block, and only its second is
    // bound by the scan loop - so an anchor computed from the first argument alone would
    // root the drive loop at function scope, where the node chunk does not dominate it.
    NodeValueSink sink;
    runQuery("MATCH (n) CALL test.scaleNodeID(100, n) YIELD value RETURN n, value",
             reader.getView(),
             sink);

    std::vector<NodeValueSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<NodeValueSink::Row> expected {{0, 0}, {1, 100}, {2, 200}, {3, 300}, {4, 400}};
    EXPECT_EQ(rows, expected);
}
