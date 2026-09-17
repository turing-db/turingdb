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

// MATCH (a:Person)<--(b) RETURN a, b
const char* const inHopOverLabelScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a:Interest)<--(b) RETURN a, b: the Interest nodes are created across several
// commits, so the scan reads more than one data part.
const char* const inHopOverInterestScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a:Person:Founder)<--(b) RETURN a, b
const char* const inHopOverTwoLabelScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person", "Founder"]) : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const inHopOverAbsentLabelScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Wizard"]) : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The fused form spelled by hand, which is what the pass prints.
const char* const labelledInEdgeScan = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_in_edges_by_label_tgt(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The out-hop belongs to the out-edge index, which this pass does not touch.
const char* const outHopOverLabelScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const undirectedHopOverLabelScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const typedInHopOverLabelScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_in_edges_by_type(%a, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const inHopOverFullScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const hopCarryingAColumn = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %srcs, %eids, %etypes, %tgts, %agef = db.get_in_edges(%a, {%age}) : (!db.column<!storage.node_id>, !db.column<i64>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<i64>)
  db.output(%srcs, %agef) : !db.column<!storage.node_id>, !db.column<i64>
  return
}
)mlir";

const char* const twoHopsOffOneScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %s1, %e1, %t1, %b = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %t2, %c = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%b, %c) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a:Person)<--(b)<--(c) RETURN a, c: only the first hop expands the label scan.
const char* const twoHopChain = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %a1, %e0, %et0, %b = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %b2, %e1, %et1, %c, %a2 = db.get_in_edges(%a1, {%b}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%a2, %c) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const inHopInCrossProductFactor = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    %srcs, %eids, %etypes, %tgts = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %srcs, %tgts : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %m : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1, %0#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class FuseScanInEdgesByLabelTest : public TuringTest {
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
        passManager.addPass(mlir::db::createFuseScanInEdgesByLabel());

        return mlir::succeeded(passManager.run(module));
    }

    void expectUntouched(mlir::ModuleOp module, size_t hopCount) {
        EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::ScanNodes>(module) + countOps<mlir::db::ScanNodesByLabel>(module), 1u);

        const size_t hops = countOps<mlir::db::GetOutEdges>(module)
                            + countOps<mlir::db::GetInEdges>(module)
                            + countOps<mlir::db::GetEdges>(module)
                            + countOps<mlir::db::GetInEdgesByType>(module);
        EXPECT_EQ(hops, hopCount);
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
        ASSERT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(*fusedModule), 1u);
        runPairs(*fusedModule, view, fused);
    }

    mlir::MLIRContext _context;
};

TEST_F(FuseScanInEdgesByLabelTest, fusesLabelScanAndInHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(inHopOverLabelScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanInEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanInEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanInEdgesByLabelTgt edgeScan = edgeScans.front();

    ASSERT_EQ(edgeScan.getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeScan.getLabels()[0]).getValue(), "Person");

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], edgeScan.getSrcids());
    EXPECT_EQ(columns[1], edgeScan.getTgtids());

    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetInEdges>(*module), 0u);
}

TEST_F(FuseScanInEdgesByLabelTest, carriesTheWholeLabelConjunction) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(inHopOverTwoLabelScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanInEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanInEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);

    const mlir::ArrayAttr labels = edgeScans.front().getLabels();
    ASSERT_EQ(labels.size(), 2u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[0]).getValue(), "Person");
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[1]).getValue(), "Founder");
}

TEST_F(FuseScanInEdgesByLabelTest, leavesOutHopAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outHopOverLabelScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 1u);
}

TEST_F(FuseScanInEdgesByLabelTest, leavesFullScanAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(inHopOverFullScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 1u);
}

TEST_F(FuseScanInEdgesByLabelTest, leavesUndirectedHopAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(undirectedHopOverLabelScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 1u);
}

TEST_F(FuseScanInEdgesByLabelTest, leavesTypedHopAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedInHopOverLabelScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 1u);
}

TEST_F(FuseScanInEdgesByLabelTest, leavesHopCarryingAColumnAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(hopCarryingAColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 1u);
}

TEST_F(FuseScanInEdgesByLabelTest, leavesScanWithASecondReaderAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoHopsOffOneScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 2u);
}

TEST_F(FuseScanInEdgesByLabelTest, fusesOnlyTheFirstHopOfATwoHopChain) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoHopChain);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanInEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanInEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanInEdgesByLabelTgt edgeScan = edgeScans.front();

    llvm::SmallVector<mlir::db::GetInEdges> hops = collect<mlir::db::GetInEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetInEdges secondHop = hops.front();
    EXPECT_EQ(secondHop.getInputNodes(), edgeScan.getSrcids());

    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
}

TEST_F(FuseScanInEdgesByLabelTest, fusesInsideCrossProductFactor) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(inHopInCrossProductFactor);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanInEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanInEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanInEdgesByLabelTgt edgeScan = edgeScans.front();

    llvm::SmallVector<mlir::db::CrossProduct> products = collect<mlir::db::CrossProduct>(*module);
    ASSERT_EQ(products.size(), 1u);
    mlir::db::CrossProduct product = products.front();

    mlir::db::Yield leftYield = mlir::cast<mlir::db::Yield>(product.getLeftFactor().front().getTerminator());
    const mlir::Operation::operand_range yielded = leftYield.getColumns();
    ASSERT_EQ(yielded.size(), 2u);
    EXPECT_EQ(yielded[0], edgeScan.getSrcids());
    EXPECT_EQ(yielded[1], edgeScan.getTgtids());

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetInEdges>(*module), 0u);
}

// The columns hold the edge as the graph stores it, so the fused scan reports the same
// source and target the hop did, not the pair its own walk direction suggests.
TEST_F(FuseScanInEdgesByLabelTest, emitsTheSameEdgesAsThePersonHop) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion(inHopOverLabelScan, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseScanInEdgesByLabelTest, emitsTheSameEdgesAcrossSeveralDataParts) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion(inHopOverInterestScan, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseScanInEdgesByLabelTest, emitsTheSameEdgesAsTheTwoLabelHop) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion(inHopOverTwoLabelScan, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseScanInEdgesByLabelTest, emitsNothingForAnAbsentLabel) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion(inHopOverAbsentLabelScan, unfused, fused);

    EXPECT_TRUE(unfused.empty());
    EXPECT_TRUE(fused.empty());
}

// The op parses back from the form it prints, so a written program and a fused one are
// the same program.
TEST_F(FuseScanInEdgesByLabelTest, emitsTheSameEdgesAsTheWrittenEdgeScan) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const mlir::OwningOpRef<mlir::ModuleOp> hopModule = parse(inHopOverLabelScan);
    ASSERT_TRUE(hopModule);
    std::vector<std::pair<uint64_t, uint64_t>> hopPairs;
    runPairs(*hopModule, view, hopPairs);

    const mlir::OwningOpRef<mlir::ModuleOp> scanModule = parse(labelledInEdgeScan);
    ASSERT_TRUE(scanModule);
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*scanModule)));
    ASSERT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(*scanModule), 1u);

    std::vector<std::pair<uint64_t, uint64_t>> scanPairs;
    runPairs(*scanModule, view, scanPairs);

    EXPECT_FALSE(hopPairs.empty());
    EXPECT_EQ(scanPairs, hopPairs);
}
