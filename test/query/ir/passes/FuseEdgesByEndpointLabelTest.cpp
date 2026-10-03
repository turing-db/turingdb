#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <string>
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

// MATCH (a)-->(b:Person) RETURN a, b, walked from the node scan rather than read off the
// whole edge set: the labels sit on the end the hop arrives at.
const char* const outHopWithLabelledTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)<--(b:Person) RETURN a, b: the in-hop reaches the edge's source, so that is the
// end the labels land on.
const char* const inHopWithLabelledSource = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-->(b:Interest:Exotic) RETURN a, b
const char* const outHopWithTwoLabelledTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Interest", "Exotic"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const outHopWithAbsentLabelledTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Wizard"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a:Person)-->(b)-->(c:Person) RETURN a, c: the first hop's node column rides the
// second as a carried column, and the filter cuts it beside the edge columns.
const char* const hopCarryingAColumn = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %c, %ac = db.get_out_edges(%b, {%s}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%c) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %cf, %af = db.filter(%matches, {%c, %ac}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%af, %cf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The label check sits on the end the hop leaves, which is the input column a by-label node
// scan constrains instead - there is nothing here for the hop to carry.
const char* const outHopWithLabelledSource = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const inHopWithLabelledTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)--(b:Person) RETURN a, b: an undirected hop reaches both ends, so neither is the
// one the labels could ride on.
const char* const undirectedHopWithLabelledTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A property read off the hop's own target column: it is neither a column the fused hop
// produces nor one the filter cuts, so its rows would stop matching the ones beside it.
const char* const hopReadPastItsFilter = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %name = db.get_node_properties(%t, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const labelledHopInCrossProductFactor = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
    db.yield %sf, %tf : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %m : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1, %0#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The fused forms spelled by hand, which is what the pass prints.
const char* const outHopByLabel = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges_by_label(%a, ["Person"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%s, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const inHopByLabel = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_in_edges_by_label(%a, ["Person"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%s, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class FuseEdgesByEndpointLabelTest : public TuringTest {
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
        passManager.addPass(mlir::db::createFuseEdgesByEndpointLabel());

        return mlir::succeeded(passManager.run(module));
    }

    void expectUntouched(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::GetOutEdgesByLabel>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::GetInEdgesByLabel>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(module), 1u);
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

    template <typename HopOp>
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
        ASSERT_EQ(countOps<HopOp>(*fusedModule), 1u);
        runPairs(*fusedModule, view, fused);
    }

    mlir::MLIRContext _context;
};

TEST_F(FuseEdgesByEndpointLabelTest, fusesOutHopAndTargetLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outHopWithLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByLabel> hops = collect<mlir::db::GetOutEdgesByLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdgesByLabel hop = hops.front();

    ASSERT_EQ(hop.getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(hop.getLabels()[0]).getValue(), "Person");

    llvm::SmallVector<mlir::db::ScanNodes> scans = collect<mlir::db::ScanNodes>(*module);
    ASSERT_EQ(scans.size(), 1u);
    EXPECT_EQ(hop.getInputNodes(), scans.front().getResult());

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], hop.getSrcids());
    EXPECT_EQ(columns[1], hop.getTgtids());

    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseEdgesByEndpointLabelTest, fusesInHopAndSourceLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(inHopWithLabelledSource);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetInEdgesByLabel> hops = collect<mlir::db::GetInEdgesByLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);

    ASSERT_EQ(hops.front().getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(hops.front().getLabels()[0]).getValue(), "Person");

    EXPECT_EQ(countOps<mlir::db::GetInEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

// The fused hop prints the label list the way it parses it, so a program that went through
// the pass reads back as the same one.
TEST_F(FuseEdgesByEndpointLabelTest, printsBackAsItParses) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(outHopWithLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));

    llvm::SmallVector<mlir::db::GetOutEdgesByLabel> hops = collect<mlir::db::GetOutEdgesByLabel>(*reparsed);
    ASSERT_EQ(hops.size(), 1u);
    ASSERT_EQ(hops.front().getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(hops.front().getLabels()[0]).getValue(), "Person");
}

TEST_F(FuseEdgesByEndpointLabelTest, carriesTheWholeLabelConjunction) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outHopWithTwoLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByLabel> hops = collect<mlir::db::GetOutEdgesByLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);

    const mlir::ArrayAttr labels = hops.front().getLabels();
    ASSERT_EQ(labels.size(), 2u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[0]).getValue(), "Interest");
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[1]).getValue(), "Exotic");
}

// The carry set rides onto the fused hop, and the columns the filter handed on are the ones
// it was given.
TEST_F(FuseEdgesByEndpointLabelTest, keepsTheCarrySet) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(hopCarryingAColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByLabel> hops = collect<mlir::db::GetOutEdgesByLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdgesByLabel hop = hops.front();

    ASSERT_EQ(hop.getColumnsToFilter().size(), 1u);
    ASSERT_EQ(hop.getFilteredColumns().size(), 1u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], hop.getFilteredColumns()[0]);
    EXPECT_EQ(columns[1], hop.getTgtids());

    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseEdgesByEndpointLabelTest, leavesTheEndTheOutHopLeavesAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outHopWithLabelledSource);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
}

TEST_F(FuseEdgesByEndpointLabelTest, leavesTheEndTheInHopLeavesAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(inHopWithLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::GetInEdges>(*module), 1u);
}

TEST_F(FuseEdgesByEndpointLabelTest, leavesUndirectedHopAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(undirectedHopWithLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::GetEdges>(*module), 1u);
}

TEST_F(FuseEdgesByEndpointLabelTest, leavesHopWithASecondReaderAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(hopReadPastItsFilter);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
}

TEST_F(FuseEdgesByEndpointLabelTest, fusesInsideCrossProductFactor) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelledHopInCrossProductFactor);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByLabel> hops = collect<mlir::db::GetOutEdgesByLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdgesByLabel hop = hops.front();

    llvm::SmallVector<mlir::db::Yield> yields = collect<mlir::db::Yield>(*module);
    ASSERT_EQ(yields.size(), 2u);
    const mlir::Operation::operand_range yielded = yields.front().getColumns();
    ASSERT_EQ(yielded.size(), 2u);
    EXPECT_EQ(yielded[0], hop.getSrcids());
    EXPECT_EQ(yielded[1], hop.getTgtids());

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

// Remy -> Adam, Adam -> Remy and Ghosts -> Remy are the three edges of simpledb arriving at
// a Person.
TEST_F(FuseEdgesByEndpointLabelTest, walksTheEdgesArrivingAtTheLabels) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetOutEdgesByLabel>(outHopWithLabelledTarget, unfused, fused);

    const std::vector<std::pair<uint64_t, uint64_t>> expected {{0, 1}, {1, 0}, {6, 0}};
    EXPECT_EQ(unfused, expected);
    EXPECT_EQ(fused, expected);
}

TEST_F(FuseEdgesByEndpointLabelTest, emitsTheSameEdgesAsTheSourceLabelCheck) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetInEdgesByLabel>(inHopWithLabelledSource, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseEdgesByEndpointLabelTest, emitsTheSameEdgesAsTheTwoLabelCheck) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetOutEdgesByLabel>(outHopWithTwoLabelledTarget, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseEdgesByEndpointLabelTest, emitsTheSameRowsWithACarriedColumn) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetOutEdgesByLabel>(hopCarryingAColumn, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseEdgesByEndpointLabelTest, emitsNothingForAnAbsentLabel) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetOutEdgesByLabel>(outHopWithAbsentLabelledTarget, unfused, fused);

    EXPECT_TRUE(unfused.empty());
    EXPECT_TRUE(fused.empty());
}

// The hop reads the same edges the by-label edge scan does, walked from a node chunk rather
// than off the edge index.
TEST_F(FuseEdgesByEndpointLabelTest, emitsTheSameEdgesAsTheLabelCheckedHops) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const mlir::OwningOpRef<mlir::ModuleOp> outModule = parse(outHopByLabel);
    ASSERT_TRUE(outModule);
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*outModule)));
    std::vector<std::pair<uint64_t, uint64_t>> outPairs;
    runPairs(*outModule, view, outPairs);

    const mlir::OwningOpRef<mlir::ModuleOp> checkedOutModule = parse(outHopWithLabelledTarget);
    ASSERT_TRUE(checkedOutModule);
    std::vector<std::pair<uint64_t, uint64_t>> checkedOutPairs;
    runPairs(*checkedOutModule, view, checkedOutPairs);

    EXPECT_FALSE(outPairs.empty());
    EXPECT_EQ(outPairs, checkedOutPairs);

    const mlir::OwningOpRef<mlir::ModuleOp> inModule = parse(inHopByLabel);
    ASSERT_TRUE(inModule);
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*inModule)));
    std::vector<std::pair<uint64_t, uint64_t>> inPairs;
    runPairs(*inModule, view, inPairs);

    const mlir::OwningOpRef<mlir::ModuleOp> checkedInModule = parse(inHopWithLabelledSource);
    ASSERT_TRUE(checkedInModule);
    std::vector<std::pair<uint64_t, uint64_t>> checkedInPairs;
    runPairs(*checkedInModule, view, checkedInPairs);

    EXPECT_FALSE(inPairs.empty());
    EXPECT_EQ(inPairs, checkedInPairs);
}
