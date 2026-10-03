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


// MATCH (a)-[e]->(b) WHERE e:KNOWS_WELL OR e:INTERESTED_IN RETURN a, b - one check per type,
// OR-ed, which is what a WHERE disjunction reaches the passes as.
const char* const orOfTwoTypes = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %k = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %i = db.check_edge_type_constraint(%et, ["INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %ok = db.or %k, %i : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The three-way chain, left-associated the way codegen emits it.
const char* const orOfThreeTypes = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %k = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %i = db.check_edge_type_constraint(%et, ["INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %ki = db.or %k, %i : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %l = db.check_edge_type_constraint(%et, ["LIKES"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %ok = db.or %ki, %l : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// An AND of two overlapping sets is the one type they share.
const char* const andOfOverlappingTypes = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %a = db.check_edge_type_constraint(%et, ["KNOWS_WELL", "INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %b = db.check_edge_type_constraint(%et, ["INTERESTED_IN", "LIKES"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %ok = db.and %a, %b : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// An edge carries one type, so an AND over disjoint sets passes no edge. There is no way to
// spell that on the check op, so the pair is left as it stands.
const char* const andOfDisjointTypes = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %a = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %b = db.check_edge_type_constraint(%et, ["INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %ok = db.and %a, %b : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The two checks read different columns, so they are about different edges and do not combine.
const char* const orOverDifferentColumns = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %et2 = db.get_edge_types(%e) : (!db.column<!storage.edge_id>) -> !db.column<!storage.edge_type_id>
  %k = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %i = db.check_edge_type_constraint(%et2, ["INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %ok = db.or %k, %i : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Only one side is a type check, so there is nothing to combine it with.
const char* const orWithANonTypeCheck = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %k = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %ls = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.or %k, %p : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%ok, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class FuseEdgeTypePredicatesTest : public TuringTest {
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
        passManager.addPass(mlir::db::createFuseEdgeTypePredicates());

        return mlir::succeeded(passManager.run(module));
    }

    // One check is left, carrying the combined set, and the boolean op that joined them is gone.
    void expectFusedTo(const char* programText, const std::vector<std::string>& edgeTypes) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);
        ASSERT_TRUE(runFuse(*module));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

        llvm::SmallVector<mlir::db::CheckEdgeTypeConstraint> checks = collect<mlir::db::CheckEdgeTypeConstraint>(module.get());
        ASSERT_EQ(checks.size(), 1u);

        const mlir::ArrayAttr fused = checks.front().getEdgeTypes();
        ASSERT_EQ(fused.size(), edgeTypes.size());
        for (size_t index = 0; index < edgeTypes.size(); index++) {
            EXPECT_EQ(mlir::cast<mlir::StringAttr>(fused[index]).getValue(), edgeTypes[index]);
        }

        EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 0u);
        EXPECT_EQ(countOps<mlir::db::AndOp>(*module), 0u);
    }

    // Both checks and the op joining them are still there.
    void expectUntouched(const char* programText) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);
        ASSERT_TRUE(runFuse(*module));

        const size_t joins = countOps<mlir::db::OrOp>(*module) + countOps<mlir::db::AndOp>(*module);
        EXPECT_EQ(joins, 1u);
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

    // Combining two checks into one must not change a row. Nothing else here proves that.
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
        ASSERT_TRUE(runFuse(*after));
        std::vector<std::pair<uint64_t, uint64_t>> afterRows;
        runPairs(*after, view, afterRows);

        EXPECT_EQ(afterRows, beforeRows);
    }

    mlir::MLIRContext _context;
};

TEST_F(FuseEdgeTypePredicatesTest, foldsAnOrIntoTheUnionOfItsTypes) {
    expectFusedTo(orOfTwoTypes, {"KNOWS_WELL", "INTERESTED_IN"});
}

TEST_F(FuseEdgeTypePredicatesTest, foldsAChainOfOrsInOneRun) {
    expectFusedTo(orOfThreeTypes, {"KNOWS_WELL", "INTERESTED_IN", "LIKES"});
}

TEST_F(FuseEdgeTypePredicatesTest, foldsAnAndIntoTheIntersectionOfItsTypes) {
    expectFusedTo(andOfOverlappingTypes, {"INTERESTED_IN"});
}

TEST_F(FuseEdgeTypePredicatesTest, leavesAnAndOverDisjointTypesAlone) {
    expectUntouched(andOfDisjointTypes);
}

TEST_F(FuseEdgeTypePredicatesTest, leavesChecksOverDifferentColumnsAlone) {
    expectUntouched(orOverDifferentColumns);
}

TEST_F(FuseEdgeTypePredicatesTest, leavesAnOrWithANonTypeCheckAlone) {
    expectUntouched(orWithANonTypeCheck);
}

TEST_F(FuseEdgeTypePredicatesTest, orEmitsTheSameRows) {
    expectSameRowsAfterPass(orOfTwoTypes);
}

TEST_F(FuseEdgeTypePredicatesTest, orChainEmitsTheSameRows) {
    expectSameRowsAfterPass(orOfThreeTypes);
}

TEST_F(FuseEdgeTypePredicatesTest, andEmitsTheSameRows) {
    expectSameRowsAfterPass(andOfOverlappingTypes);
}
