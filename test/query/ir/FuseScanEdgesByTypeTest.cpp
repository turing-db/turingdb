#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "LocalMemory.h"
#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

template <typename OpType>
llvm::SmallVector<OpType> collect(mlir::ModuleOp module) {
    llvm::SmallVector<OpType> ops;
    module.walk([&](OpType op) {
        ops.push_back(op);
    });

    return ops;
}

template <typename OpType>
size_t countOps(mlir::ModuleOp module) {
    return collect<OpType>(module).size();
}

// Accumulates the two node-ID columns of an emitted (source, target) pair.
class CollectingPairSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const ColumnNodeIDs* sources = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const ColumnNodeIDs* targets = dynamic_cast<const ColumnNodeIDs*>(chunks[1]);
        ASSERT_NE(sources, nullptr);
        ASSERT_NE(targets, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _pairs.emplace_back((*sources)[rowIndex].getValue(), (*targets)[rowIndex].getValue());
        }
    }

    void sortedPairs(std::vector<std::pair<uint64_t, uint64_t>>& pairs) const {
        pairs = _pairs;
        std::sort(pairs.begin(), pairs.end());
    }

private:
    std::vector<std::pair<uint64_t, uint64_t>> _pairs;
};

// MATCH (a)-[:KNOWS_WELL]->(b) RETURN a, b, once the whole-graph scan and its hop
// have fused into an edge scan.
const char* const typedEdgeScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %knows = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %ef, %etf, %tf = db.filter(%knows, {%s, %e, %et, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A check over two required types: an edge carries one type, so the pair is a set
// match no single by-type scan walks.
const char* const twoRequiredTypes = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %either = db.check_edge_type_constraint(%et, ["KNOWS_WELL", "INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %ef, %etf, %tf = db.filter(%either, {%s, %e, %et, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A property read off the scan's own edge column: it is neither a column the fused
// scan produces nor one the filter cuts, so its rows would stop matching the ones
// beside it.
const char* const edgeScanReadPastItsFilter = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %name = db.get_edge_properties(%e, "name") : (!db.column<!storage.edge_id>) -> !db.column<none>
  %knows = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %ef, %etf, %tf, %namef = db.filter(%knows, {%s, %e, %et, %t, %name}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<none>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The by-type scan a hand-written program can already ask for, so the lowering and
// the fused rewrite are checked against the same shape.
const char* const byTypeEdgeScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type("KNOWS_WELL") : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  db.output(%s, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A type name absent from the schema matches no edge, so the scan yields no row.
const char* const byTypeEdgeScanUnknownType = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type("ROBOTS") : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  db.output(%s, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class FuseScanEdgesByTypeTest : public TuringTest {
protected:
    void initialize() override {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runFuse(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseScanEdgesByType());

        return mlir::succeeded(passManager.run(module));
    }

    // The module still holds its plain edge scan, its type check and the filter.
    void expectUntouched(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::ScanEdgesByType>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::ScanEdges>(module), 1u);
        EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(module), 1u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(module), 1u);
    }

    // Lowers the db program as it stands and interprets it over the graph, filling
    // the (source, target) pairs it emits, sorted.
    void runPairs(mlir::ModuleOp module,
                  const GraphView& view,
                  std::vector<std::pair<uint64_t, uint64_t>>& pairs) {
        const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        CollectingPairSink sink;
        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();

        sink.sortedPairs(pairs);
    }

    // Runs one program over simpledb as it stands, with no fusion.
    void runProgram(const char* programText,
                    std::vector<std::pair<uint64_t, uint64_t>>& pairs,
                    size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        auto graph = Graph::create();
        SimpleGraph::createSimpleGraph(graph.get());

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);

        const mlir::func::FuncOp dbFunction = module->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        CollectingPairSink sink;
        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();

        sink.sortedPairs(pairs);
    }

    // The pairs the program emits before the fusion and after it, over simpledb.
    void runPairsBeforeAndAfterFusion(const char* programText,
                                      std::vector<std::pair<uint64_t, uint64_t>>& unfused,
                                      std::vector<std::pair<uint64_t, uint64_t>>& fused) {
        auto graph = Graph::create();
        SimpleGraph::createSimpleGraph(graph.get());

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        const mlir::OwningOpRef<mlir::ModuleOp> unfusedModule = parse(programText);
        ASSERT_TRUE(unfusedModule);
        runPairs(*unfusedModule, view, unfused);

        const mlir::OwningOpRef<mlir::ModuleOp> fusedModule = parse(programText);
        ASSERT_TRUE(fusedModule);
        ASSERT_TRUE(runFuse(*fusedModule));
        ASSERT_EQ(countOps<mlir::db::ScanEdgesByType>(*fusedModule), 1u);
        runPairs(*fusedModule, view, fused);
    }

    mlir::MLIRContext _context;
};

TEST_F(FuseScanEdgesByTypeTest, fusesEdgeScanAndItsTypeCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedEdgeScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(*module);
    ASSERT_EQ(scans.size(), 1u);
    mlir::db::ScanEdgesByType scan = scans.front();
    EXPECT_EQ(scan.getEdgeType(), "KNOWS_WELL");

    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);

    // The output reads the fused scan's source and target columns directly.
    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], scan.getSrcids());
    EXPECT_EQ(columns[1], scan.getTgtids());
}

TEST_F(FuseScanEdgesByTypeTest, leavesACheckOverTwoTypesAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoRequiredTypes);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    expectUntouched(*module);
}

TEST_F(FuseScanEdgesByTypeTest, leavesAScanReadPastItsFilterAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgeScanReadPastItsFilter);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    expectUntouched(*module);
}

TEST_F(FuseScanEdgesByTypeTest, emitsTheSameEdgesFused) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion(typedEdgeScan, unfused, fused);

    // simpledb's KNOWS_WELL edges: Remy (0) <-> Adam (1), and Ghosts (6) -> Remy (0).
    const std::vector<std::pair<uint64_t, uint64_t>> expected {{0, 1}, {1, 0}, {6, 0}};
    EXPECT_EQ(unfused, expected);
    EXPECT_EQ(fused, expected);
}

TEST_F(FuseScanEdgesByTypeTest, byTypeScanExecutes) {
    std::vector<std::pair<uint64_t, uint64_t>> pairs;
    runProgram(byTypeEdgeScan, pairs);

    const std::vector<std::pair<uint64_t, uint64_t>> expected {{0, 1}, {1, 0}, {6, 0}};
    EXPECT_EQ(pairs, expected);
}

// A chunk smaller than the match count makes each step stop mid-span, so the scan
// has to resume from the edge it left off at.
TEST_F(FuseScanEdgesByTypeTest, byTypeScanExecutesAcrossChunks) {
    std::vector<std::pair<uint64_t, uint64_t>> pairs;
    runProgram(byTypeEdgeScan, pairs, /*chunkSize=*/1);

    const std::vector<std::pair<uint64_t, uint64_t>> expected {{0, 1}, {1, 0}, {6, 0}};
    EXPECT_EQ(pairs, expected);
}

TEST_F(FuseScanEdgesByTypeTest, byTypeScanOfAnUnknownTypeIsEmpty) {
    std::vector<std::pair<uint64_t, uint64_t>> pairs;
    runProgram(byTypeEdgeScanUnknownType, pairs);

    EXPECT_TRUE(pairs.empty());
}
