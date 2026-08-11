#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "ProcedureContext.h"
#include "ProcedureManager.h"
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

#include "DBDialect.h"
#include "DBLowering.h"
#include "IRException.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the (node, node) rows the sampler's carried source and yielded target emit.
class NodePairSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* left = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* right = dynamic_cast<const ColumnVector<NodeID>*>(chunks[1]);
        ASSERT_NE(left, nullptr);
        ASSERT_NE(right, nullptr);
        ASSERT_EQ(left->size(), right->size());

        const auto& leftRaw = left->getRaw();
        const auto& rightRaw = right->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(leftRaw[rowIndex].getValue(), rightRaw[rowIndex].getValue());
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// MATCH (n) CALL gnn.neighbourhoodSample(n, 2) YIELD tgt RETURN n, tgt: two of the
// procedure's three declared arguments, leaving the optional seed unwritten. The
// declared count is 3 but the required count is 2, so both the lowering's and the
// translator's argument checks must accept the call.
constexpr const char* omittedSeedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %size = db.constant(2 : i64)
  %tgt, %n2 = db.call_procedure("gnn.neighbourhoodSample", {%n, %size}, {%n}) yields ["tgt"] : (!db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  db.output(%n2, %tgt) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// A call stopping short of the required sampleSize: below the floor of the accepted
// window, so it must still be rejected.
constexpr const char* missingRequiredArgumentProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %tgt = db.call_procedure("gnn.neighbourhoodSample", {%n}, {}) yields ["tgt"] : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%tgt) : !db.column<none>
  return
}
)mlir";

// A call passing one argument beyond the declared three: above the ceiling of the
// accepted window, so it must still be rejected.
constexpr const char* extraArgumentProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %size = db.constant(2 : i64)
  %seed = db.constant(42 : i64)
  %extra = db.constant(7 : i64)
  %tgt = db.call_procedure("gnn.neighbourhoodSample", {%n, %size, %seed, %extra}, {}) yields ["tgt"] : (!db.column<!storage.node_id>, !db.column<i64>, !db.column<i64>, !db.column<i64>) -> !db.column<none>
  db.output(%tgt) : !db.column<none>
  return
}
)mlir";

}

class CallOptionalArgumentTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _procedures.init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Five nodes and four edges - 0->1, 0->2, 1->4, 2->3. The sample size of 2 is at
    // least every node's out-degree, so the sample is all four edges whatever seed the
    // procedure picks for itself.
    std::unique_ptr<Graph> buildHopGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID node0 = builder.addNode(labelset);
        const NodeID node1 = builder.addNode(labelset);
        const NodeID node2 = builder.addNode(labelset);
        const NodeID node3 = builder.addNode(labelset);
        const NodeID node4 = builder.addNode(labelset);

        builder.addEdge(0, node0, node1);
        builder.addEdge(0, node0, node2);
        builder.addEdge(0, node1, node4);
        builder.addEdge(0, node2, node3);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    void runLoweredProgram(const char* programText, const GraphView& view, NLOutputSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule);

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
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

    void lowerProgram(const char* programText, const GraphView& view) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule);

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view, &_procedures);
        lowering.lower(dbFunction, *nlModule);
    }

    ProcedureManager _procedures;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(CallOptionalArgumentTest, runsWithTheOptionalSeedOmitted) {
    auto graph = buildHopGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    NodePairSink sink;
    runLoweredProgram(omittedSeedProgram, reader.getView(), sink);

    std::vector<NodePairSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<NodePairSink::Row> expected {{0, 1}, {0, 2}, {1, 4}, {2, 3}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallOptionalArgumentTest, rejectsACallBelowTheRequiredArgumentCount) {
    auto graph = buildHopGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    EXPECT_THROW(lowerProgram(missingRequiredArgumentProgram, reader.getView()), IRException);
}

TEST_F(CallOptionalArgumentTest, rejectsACallBeyondTheDeclaredArgumentCount) {
    auto graph = buildHopGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    EXPECT_THROW(lowerProgram(extraArgumentProgram, reader.getView()), IRException);
}
