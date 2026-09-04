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

// MATCH (a)-[:KNOWS_WELL]->(b) RETURN a, b
const char* const typedOutHop = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %knows = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %ef, %etf, %tf = db.filter(%knows, {%s, %e, %et, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)<-[:KNOWS_WELL]-(b) RETURN a, b, whose columns hold the same edge set: the hop
// walks predecessors, but srcids is still the edge's own source and tgtids its target.
const char* const typedInHop = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %knows = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %ef, %etf, %tf = db.filter(%knows, {%s, %e, %et, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a:Person)-[:KNOWS_WELL]->(b) RETURN a.age, b: the age read before the hop rides it
// as a carried column and the filter cuts it beside the edge columns.
const char* const typedHopCarryingAColumn = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<i64>
  %s, %e, %et, %t, %agec = db.get_out_edges(%a, {%age}) : (!db.column<!storage.node_id>, !db.column<i64>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<i64>)
  %knows = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %ef, %etf, %tf, %agef = db.filter(%knows, {%s, %e, %et, %t, %agec}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<i64>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<i64>)
  db.output(%agef, %tf) : !db.column<i64>, !db.column<!storage.node_id>
  return
}
)mlir";

// A check over two required types: an edge carries one type, so the pair is a set match no
// single by-type hop walks.
const char* const twoRequiredTypes = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %either = db.check_edge_type_constraint(%et, ["KNOWS_WELL", "INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %ef, %etf, %tf = db.filter(%either, {%s, %e, %et, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-[:KNOWS_WELL]-(b) RETURN a, b: the undirected hop has no by-type sibling.
const char* const typedUndirectedHop = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %knows = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %ef, %etf, %tf = db.filter(%knows, {%s, %e, %et, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A property read off the hop's own target column: it is neither a column the fused hop
// produces nor one the filter cuts, so its rows would stop matching the ones beside it.
const char* const typedHopReadPastItsFilter = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %name = db.get_node_properties(%t, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %knows = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %sf, %ef, %etf, %tf, %namef = db.filter(%knows, {%s, %e, %et, %t, %name}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<none>)
  db.output(%sf, %tf) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-[:KNOWS_WELL]->(b)-[:INTERESTED_IN]->(c) RETURN a, c
const char* const twoTypedHopChain = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s1, %e1, %et1, %t1 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %knows = db.check_edge_type_constraint(%et1, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %s1f, %e1f, %et1f, %t1f = db.filter(%knows, {%s1, %e1, %et1, %t1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %t2, %s1c = db.get_out_edges(%t1f, {%s1f}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %likes = db.check_edge_type_constraint(%et2, ["INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %s2f, %e2f, %et2f, %t2f, %s1cf = db.filter(%likes, {%s2, %e2, %et2, %t2, %s1c}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%s1cf, %t2f) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-[:KNOWS_WELL]->(b) MATCH (m) RETURN a, b, m
const char* const typedHopInCrossProductFactor = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %knows = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
    %sf, %ef, %etf, %tf = db.filter(%knows, {%s, %e, %et, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %sf, %tf : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %m : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1, %0#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class FuseEdgesByTypeTest : public TuringTest {
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
        passManager.addPass(mlir::db::createFuseEdgesByType());

        return mlir::succeeded(passManager.run(module));
    }

    // The module still holds its plain hop, its type check and the filter over them.
    void expectUntouched(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::GetOutEdgesByType>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::GetInEdgesByType>(module), 0u);

        const size_t hops = countOps<mlir::db::GetOutEdges>(module)
                            + countOps<mlir::db::GetInEdges>(module)
                            + countOps<mlir::db::GetEdges>(module);
        EXPECT_EQ(hops, 1u);

        EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(module), 1u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(module), 1u);
    }

    // Neither the check nor the filter it fed is left, and the fused hop reads what the
    // plain one read.
    void expectFusedHop(mlir::ModuleOp module, mlir::Operation* hop, llvm::StringRef edgeType) {
        ASSERT_NE(hop, nullptr);

        EXPECT_EQ(countOps<mlir::db::GetOutEdges>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::GetInEdges>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(module), 0u);

        EXPECT_EQ(hop->getAttrOfType<mlir::StringAttr>("edge_type").getValue(), edgeType);
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
        ASSERT_EQ(countOps<mlir::db::FilterOp>(*fusedModule), 0u);
        runPairs(*fusedModule, view, fused);
    }

    mlir::MLIRContext _context;
};

TEST_F(FuseEdgesByTypeTest, fusesOutHopAndItsTypeCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedOutHop);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByType> hops = collect<mlir::db::GetOutEdgesByType>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdgesByType hop = hops.front();

    expectFusedHop(*module, hop, "KNOWS_WELL");
    EXPECT_TRUE(hop.getInputNodes().getDefiningOp<mlir::db::ScanNodes>());

    // The output reads the fused hop's source and target columns directly.
    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], hop.getSrcids());
    EXPECT_EQ(columns[1], hop.getTgtids());
}

TEST_F(FuseEdgesByTypeTest, fusesInHopAndItsTypeCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedInHop);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetInEdgesByType> hops = collect<mlir::db::GetInEdgesByType>(*module);
    ASSERT_EQ(hops.size(), 1u);

    expectFusedHop(*module, hops.front(), "KNOWS_WELL");
}

TEST_F(FuseEdgesByTypeTest, fusedHopKeepsTheColumnItCarried) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedHopCarryingAColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByType> hops = collect<mlir::db::GetOutEdgesByType>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdgesByType hop = hops.front();

    expectFusedHop(*module, hop, "KNOWS_WELL");

    llvm::SmallVector<mlir::db::GetNodeProperties> properties = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(properties.size(), 1u);

    const mlir::Operation::operand_range carried = hop.getColumnsToFilter();
    ASSERT_EQ(carried.size(), 1u);
    EXPECT_EQ(carried[0], properties.front().getResult());

    const mlir::ResultRange filtered = hop.getFilteredColumns();
    ASSERT_EQ(filtered.size(), 1u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front().getColumns()[0], filtered[0]);
}

TEST_F(FuseEdgesByTypeTest, fusesBothHopsOfATypedChain) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoTypedHopChain);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByType> hops = collect<mlir::db::GetOutEdgesByType>(*module);
    ASSERT_EQ(hops.size(), 2u);
    EXPECT_EQ(hops[0].getEdgeType(), "KNOWS_WELL");
    EXPECT_EQ(hops[1].getEdgeType(), "INTERESTED_IN");

    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);

    // The second hop walks the first hop's targets and carries its sources along.
    EXPECT_EQ(hops[1].getInputNodes(), hops[0].getTgtids());
    const mlir::Operation::operand_range carried = hops[1].getColumnsToFilter();
    ASSERT_EQ(carried.size(), 1u);
    EXPECT_EQ(carried[0], hops[0].getSrcids());
}

TEST_F(FuseEdgesByTypeTest, fusesInsideCrossProductFactor) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedHopInCrossProductFactor);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdgesByType> hops = collect<mlir::db::GetOutEdgesByType>(*module);
    ASSERT_EQ(hops.size(), 1u);

    expectFusedHop(*module, hops.front(), "KNOWS_WELL");
}

TEST_F(FuseEdgesByTypeTest, leavesACheckOverTwoTypesAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoRequiredTypes);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    expectUntouched(*module);
}

TEST_F(FuseEdgesByTypeTest, leavesAnUndirectedHopAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedUndirectedHop);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    expectUntouched(*module);
}

TEST_F(FuseEdgesByTypeTest, leavesAHopReadPastItsFilterAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedHopReadPastItsFilter);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    expectUntouched(*module);
}

TEST_F(FuseEdgesByTypeTest, outHopEmitsTheSameEdgesFused) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion(typedOutHop, unfused, fused);

    // simpledb's KNOWS_WELL edges: Remy (0) <-> Adam (1), and Ghosts (6) -> Remy (0).
    const std::vector<std::pair<uint64_t, uint64_t>> expected {{0, 1}, {1, 0}, {6, 0}};
    EXPECT_EQ(unfused, expected);
    EXPECT_EQ(fused, expected);
}

TEST_F(FuseEdgesByTypeTest, inHopEmitsTheSameEdgesFused) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion(typedInHop, unfused, fused);

    const std::vector<std::pair<uint64_t, uint64_t>> expected {{0, 1}, {1, 0}, {6, 0}};
    EXPECT_EQ(unfused, expected);
    EXPECT_EQ(fused, expected);
}

TEST_F(FuseEdgesByTypeTest, chainEmitsTheSameRowsFused) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion(twoTypedHopChain, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(unfused, fused);
}
