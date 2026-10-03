#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <utility>
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

#include "IRTestOps.h"

using namespace db;
using namespace turing::test;

namespace {

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

// MATCH (a)-[e:KNOWS_WELL]->(b) WHERE e:KNOWS_WELL RETURN a, b: the check reads the scan's
// own type column, so it keeps every row the scan emits.
const char* const impliedOverScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The check names more types than the scan can emit, so it still keeps every row.
const char* const broaderCheckOverScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL", "INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The hop sibling of impliedOverScan.
const char* const impliedOverHop = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges_by_type(%a, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The in-edge sibling.
const char* const impliedOverInHop = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_in_edges_by_type(%a, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The scan emits a type the check turns away, so the filter is doing real work.
const char* const narrowerCheckOverScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL", "INTERESTED_IN"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Nothing the scan emits passes the check, so the intersection is empty: the scan is left
// with no type at all, which walks nothing, and the filter goes with the check.
const char* const disjointCheckOverScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, ["INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The checked column is the scan's source nodes, not its edge types. Only the type column
// carries the guarantee, so the check stands.
const char* const checkOverAnotherColumn = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %et2 = db.get_edge_types(%e) : (!db.column<!storage.edge_id>) -> !db.column<!storage.edge_type_id>
  %ok = db.check_edge_type_constraint(%et2, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Both sides name several types and neither contains the other, so the read is left with the
// two they share - the case a set against a single type never exercises.
const char* const overlappingSetsOverScan = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL", "INTERESTED_IN", "LIKES"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ok = db.check_edge_type_constraint(%et, ["INTERESTED_IN", "LIKES", "ROBOTS"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The scan's target column is read by something other than the check and the filter, so
// narrowing the scan would quietly take rows away from that reader too.
const char* const narrowerCheckWithAnotherReader = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges_by_type(["KNOWS_WELL", "INTERESTED_IN"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %name = db.get_node_properties(%t, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %ok = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class NarrowEdgeTypeReadsTest : public TuringTest {
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

    bool runDrop(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createNarrowEdgeTypeReads());

        return mlir::succeeded(passManager.run(module));
    }

    // The check and the filter over it are gone, and the read that guaranteed the type stays.
    void expectCheckDropped(const char* programText) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);
        ASSERT_TRUE(runDrop(*module));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

        EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 0u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    }

    // The module still holds its check and the filter over it.
    void expectUntouched(const char* programText) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);
        ASSERT_TRUE(runDrop(*module));

        EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 1u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
    }

    // The check and filter are gone and the read now carries the intersection.
    void expectNarrowedTo(const char* programText, const std::vector<std::string>& edgeTypes) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);
        ASSERT_TRUE(runDrop(*module));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

        EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 0u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);

        llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(module.get());
        ASSERT_EQ(scans.size(), 1u);

        const mlir::ArrayAttr narrowed = scans.front().getEdgeTypes();
        ASSERT_EQ(narrowed.size(), edgeTypes.size());
        for (size_t index = 0; index < edgeTypes.size(); index++) {
            EXPECT_EQ(mlir::cast<mlir::StringAttr>(narrowed[index]).getValue(), edgeTypes[index]);
        }
    }

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

    // The rows the program emits before the pass and after it, over simpledb. Dropping a
    // check that could reject a row would show up here and nowhere else.
    void expectSameRowsAfterPass(const char* programText) {
        auto graph = Graph::create();
        SimpleGraph::createSimpleGraph(graph.get());

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        const mlir::OwningOpRef<mlir::ModuleOp> before = parse(programText);
        ASSERT_TRUE(before);
        std::vector<std::pair<uint64_t, uint64_t>> beforeRows;
        runPairs(*before, view, beforeRows);

        const mlir::OwningOpRef<mlir::ModuleOp> after = parse(programText);
        ASSERT_TRUE(after);
        ASSERT_TRUE(runDrop(*after));
        std::vector<std::pair<uint64_t, uint64_t>> afterRows;
        runPairs(*after, view, afterRows);

        EXPECT_EQ(afterRows, beforeRows);
    }

    mlir::MLIRContext _context;
};

TEST_F(NarrowEdgeTypeReadsTest, dropsACheckRepeatingTheScansType) {
    expectCheckDropped(impliedOverScan);
}

TEST_F(NarrowEdgeTypeReadsTest, dropsACheckBroaderThanTheScansType) {
    expectCheckDropped(broaderCheckOverScan);
}

TEST_F(NarrowEdgeTypeReadsTest, dropsACheckOverAnOutHopsType) {
    expectCheckDropped(impliedOverHop);
}

TEST_F(NarrowEdgeTypeReadsTest, dropsACheckOverAnInHopsType) {
    expectCheckDropped(impliedOverInHop);
}

// Neither set contains the other, so the read keeps only their overlap, in its own order.
TEST_F(NarrowEdgeTypeReadsTest, narrowsAReadToTheOverlapOfTwoSets) {
    expectNarrowedTo(overlappingSetsOverScan, {"INTERESTED_IN", "LIKES"});
}

TEST_F(NarrowEdgeTypeReadsTest, overlappingSetsEmitTheSameRows) {
    expectSameRowsAfterPass(overlappingSetsOverScan);
}

TEST_F(NarrowEdgeTypeReadsTest, narrowsAReadToTheIntersectionWithTheCheck) {
    expectNarrowedTo(narrowerCheckOverScan, {"KNOWS_WELL"});
}

// Narrowing is only safe when nothing else reads the rows it would take away.
TEST_F(NarrowEdgeTypeReadsTest, leavesAReadAnotherOpAlsoReadsAlone) {
    expectUntouched(narrowerCheckWithAnotherReader);
}

TEST_F(NarrowEdgeTypeReadsTest, narrowedReadEmitsTheSameRows) {
    expectSameRowsAfterPass(narrowerCheckOverScan);
}

// An edge carries one type, so a read of one type checked against another passes no row.
// The read says so with an empty type set and walks nothing.
TEST_F(NarrowEdgeTypeReadsTest, emptiesAReadNoRowOfWhichPassesTheCheck) {
    expectNarrowedTo(disjointCheckOverScan, {});
}

TEST_F(NarrowEdgeTypeReadsTest, keepsACheckOverAColumnTheReadDoesNotGuarantee) {
    expectUntouched(checkOverAnotherColumn);
}

TEST_F(NarrowEdgeTypeReadsTest, impliedCheckEmitsTheSameRows) {
    expectSameRowsAfterPass(impliedOverScan);
}

TEST_F(NarrowEdgeTypeReadsTest, broaderCheckEmitsTheSameRows) {
    expectSameRowsAfterPass(broaderCheckOverScan);
}

TEST_F(NarrowEdgeTypeReadsTest, hopCheckEmitsTheSameRows) {
    expectSameRowsAfterPass(impliedOverHop);
}

TEST_F(NarrowEdgeTypeReadsTest, narrowerCheckEmitsTheSameRows) {
    expectSameRowsAfterPass(narrowerCheckOverScan);
}

TEST_F(NarrowEdgeTypeReadsTest, disjointCheckEmitsTheSameRows) {
    expectSameRowsAfterPass(disjointCheckOverScan);
}
