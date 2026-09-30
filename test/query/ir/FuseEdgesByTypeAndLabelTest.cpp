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

using NodePairs = std::vector<std::pair<uint64_t, uint64_t>>;

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

    void sortedPairs(NodePairs& pairs) const {
        pairs = _pairs;
        std::sort(pairs.begin(), pairs.end());
    }

private:
    NodePairs _pairs;
};

// MATCH (a)-[:INTERESTED_IN]->(b:Exotic) RETURN a, b
const char* const outTypedHopWithLabelledTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges_by_type(%a, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Exotic"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)<-[:KNOWS_WELL]-(b:Founder) RETURN b, a
const char* const inTypedHopWithLabelledSource = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_in_edges_by_type(%a, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]->(b:SoftwareEngineering) RETURN a, b
const char* const outTwoTypeHopWithLabelledTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges_by_type(%a, ["KNOWS_WELL", "INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["SoftwareEngineering"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-->(b)-[:KNOWS_WELL]->(c:Bioinformatics) RETURN a, c
const char* const typedHopCarryingAColumn = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %c, %ac = db.get_out_edges_by_type(%b, ["KNOWS_WELL"], {%s}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%c) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Bioinformatics"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %cf, %af = db.filter(%matches, {%c, %ac}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%af, %cf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const typedHopWithAbsentLabel = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges_by_type(%a, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Wizard"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const absentTypeHopWithLabelledTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges_by_type(%a, ["HATES"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The label check is on the end the typed out-hop leaves, which is not the hop's to carry.
const char* const outTypedHopWithLabelledSource = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges_by_type(%a, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %labelsets = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf, %tf = db.filter(%matches, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const outHopByTypeAndLabel = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges_by_type_and_label(%a, ["INTERESTED_IN"], ["Exotic"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%s, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const inHopByTypeAndLabel = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_in_edges_by_type_and_label(%a, ["KNOWS_WELL"], ["Founder"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%s, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// An empty type set is what NarrowEdgeTypeReads writes for a read no type can match.
const char* const outHopByNoTypeAndLabel = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges_by_type_and_label(%a, [], ["Person"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%s, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const outHopByTypeAndNoLabel = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges_by_type_and_label(%a, ["KNOWS_WELL"], [], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%s, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class FuseEdgesByTypeAndLabelTest : public TuringTest {
protected:
    void initialize() override {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runFuse(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseEdgesByEndpointLabel());

        return mlir::succeeded(passManager.run(module));
    }

    void runPairs(mlir::ModuleOp module, NodePairs& pairs) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

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
    void runPairsBeforeAndAfterFusion(const char* programText, NodePairs& unfused, NodePairs& fused) {
        const mlir::OwningOpRef<mlir::ModuleOp> unfusedModule = parse(programText);
        ASSERT_TRUE(unfusedModule);
        runPairs(*unfusedModule, unfused);

        const mlir::OwningOpRef<mlir::ModuleOp> fusedModule = parse(programText);
        ASSERT_TRUE(fusedModule);
        ASSERT_TRUE(runFuse(*fusedModule));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*fusedModule)));
        ASSERT_EQ(countOps<HopOp>(*fusedModule), 1u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(*fusedModule), 0u);
        runPairs(*fusedModule, fused);
    }

    void runHandWritten(const char* programText, NodePairs& pairs) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));
        runPairs(*module, pairs);
    }

    mlir::MLIRContext _context;
    std::unique_ptr<Graph> _graph;
};

TEST_F(FuseEdgesByTypeAndLabelTest, fusesTypedOutHopAndTargetLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outTypedHopWithLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByTypeAndLabel> hops = collect<mlir::db::GetOutEdgesByTypeAndLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdgesByTypeAndLabel hop = hops.front();

    const mlir::ArrayAttr edgeTypes = hop.getEdgeTypes();
    ASSERT_EQ(edgeTypes.size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeTypes[0]).getValue(), "INTERESTED_IN");

    const mlir::ArrayAttr labels = hop.getLabels();
    ASSERT_EQ(labels.size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[0]).getValue(), "Exotic");

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], hop.getSrcids());
    EXPECT_EQ(columns[1], hop.getTgtids());

    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByType>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseEdgesByTypeAndLabelTest, fusesTypedInHopAndSourceLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(inTypedHopWithLabelledSource);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetInEdgesByTypeAndLabel> hops = collect<mlir::db::GetInEdgesByTypeAndLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);

    ASSERT_EQ(hops.front().getEdgeTypes().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(hops.front().getEdgeTypes()[0]).getValue(), "KNOWS_WELL");
    ASSERT_EQ(hops.front().getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(hops.front().getLabels()[0]).getValue(), "Founder");

    EXPECT_EQ(countOps<mlir::db::GetInEdgesByType>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByTypeAndLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseEdgesByTypeAndLabelTest, keepsEveryType) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outTwoTypeHopWithLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    llvm::SmallVector<mlir::db::GetOutEdgesByTypeAndLabel> hops = collect<mlir::db::GetOutEdgesByTypeAndLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);

    const mlir::ArrayAttr edgeTypes = hops.front().getEdgeTypes();
    ASSERT_EQ(edgeTypes.size(), 2u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeTypes[0]).getValue(), "KNOWS_WELL");
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeTypes[1]).getValue(), "INTERESTED_IN");
}

TEST_F(FuseEdgesByTypeAndLabelTest, keepsTheCarrySet) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedHopCarryingAColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByTypeAndLabel> hops = collect<mlir::db::GetOutEdgesByTypeAndLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdgesByTypeAndLabel hop = hops.front();

    ASSERT_EQ(hop.getColumnsToFilter().size(), 1u);
    ASSERT_EQ(hop.getFilteredColumns().size(), 1u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], hop.getFilteredColumns()[0]);
    EXPECT_EQ(columns[1], hop.getTgtids());
}

TEST_F(FuseEdgesByTypeAndLabelTest, leavesTheEndTheTypedOutHopLeavesAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outTypedHopWithLabelledSource);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByTypeAndLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByType>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(FuseEdgesByTypeAndLabelTest, printsBackAsItParses) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(outTypedHopWithLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed);
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));

    llvm::SmallVector<mlir::db::GetOutEdgesByTypeAndLabel> hops = collect<mlir::db::GetOutEdgesByTypeAndLabel>(*reparsed);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_EQ(hops.front().getEdgeTypes().size(), 1u);
    EXPECT_EQ(hops.front().getLabels().size(), 1u);
}

TEST_F(FuseEdgesByTypeAndLabelTest, rejectsAnEmptyLabelList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outHopByTypeAndNoLabel);
    EXPECT_FALSE(module);
}

// Remy -> Eighties and Remy -> Ghosts are the INTERESTED_IN edges of simpledb arriving at an
// Exotic node; Remy -> Computers is dropped by the label and Remy -> Adam by the type.
TEST_F(FuseEdgesByTypeAndLabelTest, walksTheTypedEdgesArrivingAtTheLabels) {
    NodePairs unfused;
    NodePairs fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetOutEdgesByTypeAndLabel>(outTypedHopWithLabelledTarget, unfused, fused);

    const NodePairs expected {{0, 3}, {0, 6}};
    EXPECT_EQ(unfused, expected);
    EXPECT_EQ(fused, expected);
}

// Ghosts -> Remy is a KNOWS_WELL edge too, but Ghosts is no Founder.
TEST_F(FuseEdgesByTypeAndLabelTest, walksTheTypedEdgesLeavingTheLabels) {
    NodePairs unfused;
    NodePairs fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetInEdgesByTypeAndLabel>(inTypedHopWithLabelledSource, unfused, fused);

    const NodePairs expected {{0, 1}, {1, 0}};
    EXPECT_EQ(unfused, expected);
    EXPECT_EQ(fused, expected);
}

// Adam -> Remy and Ghosts -> Remy over KNOWS_WELL, Remy -> Computers and Luc -> Computers over
// INTERESTED_IN; Remy -> Adam is dropped by the label.
TEST_F(FuseEdgesByTypeAndLabelTest, walksEitherTypeArrivingAtTheLabels) {
    NodePairs unfused;
    NodePairs fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetOutEdgesByTypeAndLabel>(outTwoTypeHopWithLabelledTarget, unfused, fused);

    const NodePairs expected {{0, 2}, {1, 0}, {6, 0}, {9, 2}};
    EXPECT_EQ(unfused, expected);
    EXPECT_EQ(fused, expected);
}

// Remy -> Adam is the one KNOWS_WELL edge arriving at a Bioinformatics node, and Adam and
// Ghosts are the nodes with an edge to Remy.
TEST_F(FuseEdgesByTypeAndLabelTest, emitsTheSameRowsWithACarriedColumn) {
    NodePairs unfused;
    NodePairs fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetOutEdgesByTypeAndLabel>(typedHopCarryingAColumn, unfused, fused);

    const NodePairs expected {{1, 1}, {6, 1}};
    EXPECT_EQ(unfused, expected);
    EXPECT_EQ(fused, expected);
}

TEST_F(FuseEdgesByTypeAndLabelTest, emitsNothingForAnAbsentLabel) {
    NodePairs unfused;
    NodePairs fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetOutEdgesByTypeAndLabel>(typedHopWithAbsentLabel, unfused, fused);

    EXPECT_TRUE(unfused.empty());
    EXPECT_TRUE(fused.empty());
}

TEST_F(FuseEdgesByTypeAndLabelTest, emitsNothingForAnAbsentType) {
    NodePairs unfused;
    NodePairs fused;
    runPairsBeforeAndAfterFusion<mlir::db::GetOutEdgesByTypeAndLabel>(absentTypeHopWithLabelledTarget, unfused, fused);

    EXPECT_TRUE(unfused.empty());
    EXPECT_TRUE(fused.empty());
}

TEST_F(FuseEdgesByTypeAndLabelTest, runsTheHandWrittenHops) {
    NodePairs outPairs;
    runHandWritten(outHopByTypeAndLabel, outPairs);
    EXPECT_EQ(outPairs, (NodePairs {{0, 3}, {0, 6}}));

    NodePairs inPairs;
    runHandWritten(inHopByTypeAndLabel, inPairs);
    EXPECT_EQ(inPairs, (NodePairs {{0, 1}, {1, 0}}));
}

TEST_F(FuseEdgesByTypeAndLabelTest, emitsNothingForAnEmptyTypeSet) {
    NodePairs pairs;
    runHandWritten(outHopByNoTypeAndLabel, pairs);
    EXPECT_TRUE(pairs.empty());
}
