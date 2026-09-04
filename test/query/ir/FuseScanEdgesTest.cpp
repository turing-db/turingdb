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

// MATCH (a)-->(b) RETURN a, b
const char* const outHopOverFullScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)<--(b) RETURN a, b, whose four columns hold the same edge set: the hop
// walks predecessors, but srcids is still the edge's own source and tgtids its target.
const char* const inHopOverFullScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a:Person)-->(b) RETURN a, b
const char* const outHopOverLabelScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-->(b) WHERE a = 1 OR a = 3 RETURN a, b, once the ID filter fused into the scan
const char* const outHopOverConstScan = R"mlir(
func.func @main() {
  %a = db.const_scan_nodes([1, 3]) : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)--(b) RETURN a, b: the undirected hop walks both directions, so over
// every node it reports each edge twice - once from each endpoint.
const char* const undirectedHopOverFullScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-[:KNOWS]->(b) RETURN a, b
const char* const typedHopOverFullScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %tgts = db.get_out_edges_by_type(%a, "KNOWS", {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A hop carrying a column read off its own input: the carried column is row-aligned
// with the scan, and a scan_edges has nothing to filter it against.
const char* const hopCarryingAColumn = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %srcs, %eids, %etypes, %tgts, %agef = db.get_out_edges(%a, {%age}) : (!db.column<!storage.node_id>, !db.column<i64>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<i64>)
  db.output(%tgts, %agef) : !db.column<!storage.node_id>, !db.column<i64>
  return
}
)mlir";

// Two hops off the same scan: the raw node column has a second reader, which the
// fused form no longer produces.
const char* const twoHopsOffOneScan = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %t1, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %t2, %c = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%b, %c) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-->(b)-->(c) RETURN a, c: only the first hop expands the scan.
const char* const twoHopChain = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %b2, %e1, %et1, %c, %a2 = db.get_out_edges(%b, {%a1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%a2, %c) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-->(b) MATCH (m) RETURN a, b, m
const char* const outHopInCrossProductFactor = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %srcs, %eids, %etypes, %tgts = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
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

class FuseScanEdgesTest : public TuringTest {
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
        passManager.addPass(mlir::db::createFuseScanEdges());

        return mlir::succeeded(passManager.run(module));
    }

    // The module holds exactly its original scan and hop and no edge scan.
    void expectUntouched(mlir::ModuleOp module, size_t hopCount) {
        EXPECT_EQ(countOps<mlir::db::ScanEdges>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::ScanNodes>(module)
                      + countOps<mlir::db::ScanNodesByLabel>(module)
                      + countOps<mlir::db::ConstScanNodes>(module),
                  1u);

        const size_t hops = countOps<mlir::db::GetOutEdges>(module)
                            + countOps<mlir::db::GetInEdges>(module)
                            + countOps<mlir::db::GetEdges>(module)
                            + countOps<mlir::db::GetOutEdgesByType>(module);
        EXPECT_EQ(hops, hopCount);
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
        ASSERT_EQ(countOps<mlir::db::ScanEdges>(*fusedModule), 1u);
        runPairs(*fusedModule, view, fused);
    }

    mlir::MLIRContext _context;
};

TEST_F(FuseScanEdgesTest, fusesFullScanAndOutHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outHopOverFullScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanEdges> edgeScans = collect<mlir::db::ScanEdges>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanEdges edgeScan = edgeScans.front();

    // The output reads the edge scan's source and target columns directly.
    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], edgeScan.getSrcids());
    EXPECT_EQ(columns[1], edgeScan.getTgtids());

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 0u);
}

TEST_F(FuseScanEdgesTest, fusesFullScanAndInHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(inHopOverFullScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanEdges> edgeScans = collect<mlir::db::ScanEdges>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanEdges edgeScan = edgeScans.front();

    // The in-hop's four results map to the edge scan's in the same order: the
    // direction decided which side was walked from, not which column holds what.
    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], edgeScan.getSrcids());
    EXPECT_EQ(columns[1], edgeScan.getTgtids());

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetInEdges>(*module), 0u);
}

TEST_F(FuseScanEdgesTest, leavesLabelledScanAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outHopOverLabelScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 1u);
}

TEST_F(FuseScanEdgesTest, leavesConstScanAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outHopOverConstScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 1u);
}

TEST_F(FuseScanEdgesTest, leavesUndirectedHopAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(undirectedHopOverFullScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 1u);
}

TEST_F(FuseScanEdgesTest, leavesTypedHopAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedHopOverFullScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 1u);
}

TEST_F(FuseScanEdgesTest, leavesHopCarryingAColumnAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(hopCarryingAColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 1u);
}

TEST_F(FuseScanEdgesTest, leavesScanWithASecondReaderAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoHopsOffOneScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module, 2u);
}

TEST_F(FuseScanEdgesTest, fusesOnlyTheFirstHopOfATwoHopChain) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoHopChain);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanEdges> edgeScans = collect<mlir::db::ScanEdges>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanEdges edgeScan = edgeScans.front();

    // The second hop walks on from the edge scan's targets, carrying its sources.
    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdges secondHop = hops.front();
    EXPECT_EQ(secondHop.getInputNodes(), edgeScan.getTgtids());
    ASSERT_EQ(secondHop.getColumnsToFilter().size(), 1u);
    EXPECT_EQ(secondHop.getColumnsToFilter().front(), edgeScan.getSrcids());

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
}

TEST_F(FuseScanEdgesTest, fusesInsideCrossProductFactor) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outHopInCrossProductFactor);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanEdges> edgeScans = collect<mlir::db::ScanEdges>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanEdges edgeScan = edgeScans.front();

    // The edge scan took the hop's place inside the left factor, which now yields
    // its two node columns; the right factor's node scan is untouched.
    llvm::SmallVector<mlir::db::CrossProduct> products = collect<mlir::db::CrossProduct>(*module);
    ASSERT_EQ(products.size(), 1u);
    mlir::db::CrossProduct product = products.front();

    mlir::db::Yield leftYield = mlir::cast<mlir::db::Yield>(product.getLeftFactor().front().getTerminator());
    const mlir::Operation::operand_range yielded = leftYield.getColumns();
    ASSERT_EQ(yielded.size(), 2u);
    EXPECT_EQ(yielded[0], edgeScan.getSrcids());
    EXPECT_EQ(yielded[1], edgeScan.getTgtids());

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 0u);
}

TEST_F(FuseScanEdgesTest, emitsTheSameEdgesAsTheOutHop) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion(outHopOverFullScan, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseScanEdgesTest, emitsTheSameEdgesAsTheInHop) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion(inHopOverFullScan, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}
