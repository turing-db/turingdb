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

#include "IRTestOps.h"

using namespace db;
using namespace turing::test;

namespace {

using NodePairs = std::vector<std::pair<uint64_t, uint64_t>>;

class CollectingPairSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const ColumnNodeIDs* first = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const ColumnNodeIDs* second = dynamic_cast<const ColumnNodeIDs*>(chunks[1]);
        ASSERT_NE(first, nullptr);
        ASSERT_NE(second, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _pairs.emplace_back((*first)[rowIndex].getValue(), (*second)[rowIndex].getValue());
        }
    }

    void sortedPairs(NodePairs& pairs) const {
        pairs = _pairs;
        std::sort(pairs.begin(), pairs.end());
    }

private:
    NodePairs _pairs;
};

const char* const checkOverLabelScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %af = db.filter(%matches, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%af, %af) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const checkWiderThanLabelScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person", "Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %af = db.filter(%matches, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%af, %af) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The second check is the conjunction of the scan's label and the first check's.
const char* const checkCoveredByTwoEarlierConstraints = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %founder = db.check_label_constraint(%labelsets, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %af = db.filter(%founder, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %labelsets2 = db.get_node_label_set(%af) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %both = db.check_label_constraint(%labelsets2, ["Person", "Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %aff = db.filter(%both, {%af}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%aff, %aff) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-->(b:Person), (a)-->(b) RETURN a, b: b is the first hop's labelled target,
// carried through the second hop and the equality that rebinds it, then checked again.
const char* const checkOverACarriedLabelledTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %b = db.get_out_edges_by_label(%a, ["Person"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %c, %bc = db.get_out_edges_by_type_and_label(%s, ["KNOWS_WELL"], ["Person"], {%b}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %same = db.eq %bc, %c : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %bf, %sf = db.filter(%same, {%bc, %s2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%bf) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %bff, %sff = db.filter(%matches, {%bf, %sf}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sff, %bff) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The by-type-and-label in-hop reaches the edge's source, so srcids carries its labels and
// tgtids is the input column.
const char* const checkOverAnInHopSource = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_in_edges_by_type_and_label(%a, ["INTERESTED_IN"], ["Person"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %sourceLabelsets = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %sourceMatches = db.check_label_constraint(%sourceLabelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%sourceMatches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %targetLabelsets = db.get_node_label_set(%tf) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %targetMatches = db.check_label_constraint(%targetLabelsets, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sff, %tff = db.filter(%targetMatches, {%sf, %tf}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sff, %tff) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A plain hop's target carries no label, so its check is the only thing keeping Persons.
const char* const checkOverAPlainHopTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The check is read by the output too, so only the filter goes.
const char* const checkReadPastItsFilter = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %af, %mf = db.filter(%matches, {%a, %matches}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.bool>) -> (!db.column<!storage.node_id>, !db.column<!storage.bool>)
  db.output(%af, %mf) : !db.column<!storage.node_id>, !db.column<!storage.bool>
  return
}
)mlir";

}

class RemoveRedundantLabelChecksTest : public TuringTest {
protected:
    void initialize() override {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    mlir::OwningOpRef<mlir::ModuleOp> parseAndRemove(const char* programText) {
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module);
        if (!module) {
            return module;
        }

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createRemoveRedundantLabelChecks());
        EXPECT_TRUE(mlir::succeeded(passManager.run(*module)));
        EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

        return module;
    }

    void runPairs(const char* programText, bool removeChecks, NodePairs& pairs) {
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module);

        if (removeChecks) {
            mlir::PassManager passManager(&_context);
            passManager.addPass(mlir::db::createRemoveRedundantLabelChecks());
            ASSERT_TRUE(mlir::succeeded(passManager.run(*module)));
        }

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        const mlir::func::FuncOp dbFunction = module->lookupSymbol<mlir::func::FuncOp>("main");
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

    mlir::MLIRContext _context;
    std::unique_ptr<Graph> _graph;
};

TEST_F(RemoveRedundantLabelChecksTest, removesACheckTheLabelScanGuarantees) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(checkOverLabelScan);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 0u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    llvm::SmallVector<mlir::db::ScanNodesByLabel> scans = collect<mlir::db::ScanNodesByLabel>(*module);
    ASSERT_EQ(scans.size(), 1u);
    EXPECT_EQ(outputs.front().getColumns()[0], scans.front().getResult());
}

TEST_F(RemoveRedundantLabelChecksTest, keepsACheckAskingForMoreLabels) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(checkWiderThanLabelScan);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
}

TEST_F(RemoveRedundantLabelChecksTest, removesACheckTwoEarlierConstraintsGuarantee) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(checkCoveredByTwoEarlierConstraints);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);

    llvm::SmallVector<mlir::db::CheckLabelConstraint> checks = collect<mlir::db::CheckLabelConstraint>(*module);
    ASSERT_EQ(checks.size(), 1u);
    EXPECT_EQ(checks.front().getLabels().size(), 1u);
}

TEST_F(RemoveRedundantLabelChecksTest, removesACheckOverACarriedLabelledTarget) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(checkOverACarriedLabelledTarget);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(RemoveRedundantLabelChecksTest, removesChecksOnBothEndsOfAnInHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(checkOverAnInHopSource);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(RemoveRedundantLabelChecksTest, keepsACheckOverAPlainHopTarget) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(checkOverAPlainHopTarget);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
}

TEST_F(RemoveRedundantLabelChecksTest, keepsACheckReadPastItsFilter) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseAndRemove(checkReadPastItsFilter);
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
}

// Remy -> Adam, Adam -> Remy and Ghosts -> Remy are the KNOWS_WELL edges arriving at a Person.
TEST_F(RemoveRedundantLabelChecksTest, emitsTheSameRowsOverACarriedLabelledTarget) {
    NodePairs kept;
    runPairs(checkOverACarriedLabelledTarget, false, kept);

    NodePairs removed;
    runPairs(checkOverACarriedLabelledTarget, true, removed);

    const NodePairs expected {{0, 1}, {1, 0}, {6, 0}};
    EXPECT_EQ(kept, expected);
    EXPECT_EQ(removed, expected);
}

TEST_F(RemoveRedundantLabelChecksTest, emitsTheSameRowsOverAnInHop) {
    NodePairs kept;
    runPairs(checkOverAnInHopSource, false, kept);

    NodePairs removed;
    runPairs(checkOverAnInHopSource, true, removed);

    EXPECT_FALSE(kept.empty());
    EXPECT_EQ(removed, kept);
}
