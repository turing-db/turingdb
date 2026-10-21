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

// MATCH (a)-->(b:Person) RETURN a, b
const char* const edgeScanWithLabelledTarget = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)<--(b:Person) RETURN a, b: the in-hop reads the same edges the other way
// round, so the label lands on the source column and the output crosses the two.
const char* const edgeScanWithLabelledSource = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%srcs) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-->(b:Interest:Exotic) RETURN a, b
const char* const edgeScanWithTwoLabelledTarget = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Interest", "Exotic"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const edgeScanWithAbsentLabelledTarget = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Wizard"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a:Person)-->(b:Interest) RETURN a, b
const char* const sourceLabelledScanWithLabelledTarget = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_src(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-->(b:Interest) WHERE a:Bioinformatics RETURN a, b
const char* const targetLabelledScanWithLabelledSource = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_tgt(["Interest"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%srcs) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Bioinformatics"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a:Person)-->(b) WHERE a:Founder RETURN a, b
const char* const sourceLabelledScanWithSourceCheck = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_src(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%srcs) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a:Person)-->(b:Interest) WHERE b:Exotic RETURN a, b
const char* const bothLabelledScanWithTargetCheck = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_src_tgt(["Person"], ["Interest"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Exotic"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const sourceLabelledScanWithAbsentLabelledTarget = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_src(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Wizard"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-->(b) WHERE b:Interest AND a:Bioinformatics RETURN a, b
const char* const edgeScanWithStackedEndpointChecks = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %labelsets2 = db.get_node_label_set(%kept#0) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches2 = db.check_label_constraint(%labelsets2, ["Bioinformatics"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept2:2 = db.filter(%matches2, {%kept#0, %kept#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept2#0, %kept2#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A disjunction is no label set an index holds.
const char* const sourceLabelledScanWithTargetDisjunction = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_src(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, [["Exotic"], ["SleepDisturber"]]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The fused forms spelled by hand, which is what the pass prints.
const char* const labelledTargetEdgeScan = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_tgt(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const labelledSourceEdgeScan = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_in_edges_by_label_src(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  db.output(%srcs, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A label check over a node scan is the by-label node scan's own shape, and that column is
// no endpoint of an edge scan.
const char* const nodeScanWithLabelCheck = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept = db.filter(%matches, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%kept) : !db.column<!storage.node_id>
  return
}
)mlir";

// The by-type scan keeps its own edges; there is no op holding both an edge type and a
// label set.
const char* const typedEdgeScanWithLabelledTarget = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_edges_by_type(["KNOWS_WELL"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// An edge type check is the by-type fusion's shape, not this one's.
const char* const edgeScanWithTypeCheck = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %matches = db.check_edge_type_constraint(%etypes, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %kept#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The unfiltered target column is read a second time, so the rows the filter drops are
// still someone's rows and the scan has to produce them.
const char* const edgeScanReadUnfiltered = R"mlir(
func.func @main() {
  %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%kept#0, %tgts) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const labelledTargetInCrossProductFactor = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
    %labelsets = db.get_node_label_set(%tgts) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %matches = db.check_label_constraint(%labelsets, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    %kept:2 = db.filter(%matches, {%srcs, %tgts}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
    db.yield %kept#0, %kept#1 : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %m : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1, %0#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

class FuseScanEdgesByEndpointLabelTest : public TuringTest {
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
        passManager.addPass(mlir::db::createFuseScanEdgesByEndpointLabel());

        return mlir::succeeded(passManager.run(module));
    }

    void expectUntouched(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelSrc>(module), 0u);
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
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, _chunkSize);
        interpreter.run();

        sink.sortedPairs(pairs);
    }

    template <typename ScanOp>
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
        ASSERT_EQ(countOps<ScanOp>(*fusedModule), 1u);
        runPairs(*fusedModule, view, fused);
    }

    mlir::MLIRContext _context;
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
};

TEST_F(FuseScanEdgesByEndpointLabelTest, fusesEdgeScanAndTargetLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgeScanWithLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanOutEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanOutEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanOutEdgesByLabelTgt edgeScan = edgeScans.front();

    ASSERT_EQ(edgeScan.getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeScan.getLabels()[0]).getValue(), "Person");

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], edgeScan.getSrcids());
    EXPECT_EQ(columns[1], edgeScan.getTgtids());

    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, fusesEdgeScanAndSourceLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgeScanWithLabelledSource);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanInEdgesByLabelSrc> edgeScans = collect<mlir::db::ScanInEdgesByLabelSrc>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanInEdgesByLabelSrc edgeScan = edgeScans.front();

    ASSERT_EQ(edgeScan.getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeScan.getLabels()[0]).getValue(), "Person");

    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, carriesTheWholeLabelConjunction) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgeScanWithTwoLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanOutEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanOutEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);

    const mlir::ArrayAttr labels = edgeScans.front().getLabels();
    ASSERT_EQ(labels.size(), 2u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[0]).getValue(), "Interest");
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[1]).getValue(), "Exotic");
}

TEST_F(FuseScanEdgesByEndpointLabelTest, leavesNodeScanAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nodeScanWithLabelCheck);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, leavesTypedEdgeScanAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(typedEdgeScanWithLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::ScanEdgesByType>(*module), 1u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, leavesEdgeTypeCheckAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgeScanWithTypeCheck);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 1u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, leavesScanWithASecondReaderAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgeScanReadUnfiltered);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 1u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, fusesInsideCrossProductFactor) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelledTargetInCrossProductFactor);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanOutEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanOutEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanOutEdgesByLabelTgt edgeScan = edgeScans.front();

    llvm::SmallVector<mlir::db::Yield> yields = collect<mlir::db::Yield>(*module);
    ASSERT_EQ(yields.size(), 2u);
    const mlir::Operation::operand_range yielded = yields.front().getColumns();
    ASSERT_EQ(yielded.size(), 2u);
    EXPECT_EQ(yielded[0], edgeScan.getSrcids());
    EXPECT_EQ(yielded[1], edgeScan.getTgtids());

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, emitsTheSameEdgesAsTheTargetLabelCheck) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::ScanOutEdgesByLabelTgt>(edgeScanWithLabelledTarget, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, emitsTheSameEdgesAsTheSourceLabelCheck) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::ScanInEdgesByLabelSrc>(edgeScanWithLabelledSource, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, emitsTheSameEdgesAsTheTwoLabelCheck) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::ScanOutEdgesByLabelTgt>(edgeScanWithTwoLabelledTarget, unfused, fused);

    EXPECT_FALSE(unfused.empty());
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, emitsNothingForAnAbsentLabel) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::ScanOutEdgesByLabelTgt>(edgeScanWithAbsentLabelledTarget, unfused, fused);

    EXPECT_TRUE(unfused.empty());
    EXPECT_TRUE(fused.empty());
}

// The rows are the ones the labelled endpoint's own index already holds: a target-labelled
// scan emits what an in-hop over the same labels does, and a source-labelled one what an
// out-hop does.
TEST_F(FuseScanEdgesByEndpointLabelTest, emitsTheSameEdgesAsTheLabelledHops) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    const mlir::OwningOpRef<mlir::ModuleOp> targetModule = parse(labelledTargetEdgeScan);
    ASSERT_TRUE(targetModule);
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*targetModule)));
    std::vector<std::pair<uint64_t, uint64_t>> targetPairs;
    runPairs(*targetModule, view, targetPairs);

    const mlir::OwningOpRef<mlir::ModuleOp> checkedTargetModule = parse(edgeScanWithLabelledTarget);
    ASSERT_TRUE(checkedTargetModule);
    std::vector<std::pair<uint64_t, uint64_t>> checkedTargetPairs;
    runPairs(*checkedTargetModule, view, checkedTargetPairs);

    EXPECT_FALSE(targetPairs.empty());
    EXPECT_EQ(targetPairs, checkedTargetPairs);

    const mlir::OwningOpRef<mlir::ModuleOp> sourceModule = parse(labelledSourceEdgeScan);
    ASSERT_TRUE(sourceModule);
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*sourceModule)));
    std::vector<std::pair<uint64_t, uint64_t>> sourcePairs;
    runPairs(*sourceModule, view, sourcePairs);

    const mlir::OwningOpRef<mlir::ModuleOp> checkedSourceModule = parse(edgeScanWithLabelledSource);
    ASSERT_TRUE(checkedSourceModule);
    std::vector<std::pair<uint64_t, uint64_t>> checkedSourcePairs;
    runPairs(*checkedSourceModule, view, checkedSourcePairs);

    EXPECT_FALSE(sourcePairs.empty());
    EXPECT_EQ(sourcePairs, checkedSourcePairs);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, fusesSourceLabelledScanAndTargetLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(sourceLabelledScanWithLabelledTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanOutEdgesByLabelSrcTgt> edgeScans = collect<mlir::db::ScanOutEdgesByLabelSrcTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanOutEdgesByLabelSrcTgt edgeScan = edgeScans.front();

    ASSERT_EQ(edgeScan.getSrcLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeScan.getSrcLabels()[0]).getValue(), "Person");
    ASSERT_EQ(edgeScan.getTgtLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeScan.getTgtLabels()[0]).getValue(), "Interest");

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], edgeScan.getSrcids());
    EXPECT_EQ(columns[1], edgeScan.getTgtids());

    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelSrc>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

// The scan keeps the index it read, which is keyed by the target's label set.
TEST_F(FuseScanEdgesByEndpointLabelTest, fusesTargetLabelledScanAndSourceLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(targetLabelledScanWithLabelledSource);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanInEdgesByLabelSrcTgt> edgeScans = collect<mlir::db::ScanInEdgesByLabelSrcTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanInEdgesByLabelSrcTgt edgeScan = edgeScans.front();

    ASSERT_EQ(edgeScan.getSrcLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeScan.getSrcLabels()[0]).getValue(), "Bioinformatics");
    ASSERT_EQ(edgeScan.getTgtLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeScan.getTgtLabels()[0]).getValue(), "Interest");

    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelSrcTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, widensTheLabelsOfTheCheckedEnd) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(sourceLabelledScanWithSourceCheck);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanOutEdgesByLabelSrc> edgeScans = collect<mlir::db::ScanOutEdgesByLabelSrc>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);

    const mlir::ArrayAttr labels = edgeScans.front().getLabels();
    ASSERT_EQ(labels.size(), 2u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[0]).getValue(), "Person");
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[1]).getValue(), "Founder");

    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelSrc>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, widensTheTargetLabelsOfABothEndsScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(bothLabelledScanWithTargetCheck);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::ScanOutEdgesByLabelSrcTgt> edgeScans = collect<mlir::db::ScanOutEdgesByLabelSrcTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanOutEdgesByLabelSrcTgt edgeScan = edgeScans.front();

    EXPECT_EQ(edgeScan.getSrcLabels().size(), 1u);

    const mlir::ArrayAttr targetLabels = edgeScan.getTgtLabels();
    ASSERT_EQ(targetLabels.size(), 2u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(targetLabels[0]).getValue(), "Interest");
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(targetLabels[1]).getValue(), "Exotic");

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

// The first check makes a target-labelled scan, and the second then lands on its source.
TEST_F(FuseScanEdgesByEndpointLabelTest, fusesStackedChecksOnBothEnds) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgeScanWithStackedEndpointChecks);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelSrcTgt>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, leavesTargetDisjunctionAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(sourceLabelledScanWithTargetDisjunction);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelSrc>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelSrcTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, emitsTheSameEdgesOffTheSourceIndex) {
    for (const size_t chunkSize : {ChunkConfig::CHUNK_SIZE, size_t {1}, size_t {2}}) {
        _chunkSize = chunkSize;

        std::vector<std::pair<uint64_t, uint64_t>> unfused;
        std::vector<std::pair<uint64_t, uint64_t>> fused;
        runPairsBeforeAndAfterFusion<mlir::db::ScanOutEdgesByLabelSrcTgt>(sourceLabelledScanWithLabelledTarget, unfused, fused);

        EXPECT_EQ(unfused.size(), 15u) << chunkSize;
        EXPECT_EQ(fused, unfused) << chunkSize;
    }
}

TEST_F(FuseScanEdgesByEndpointLabelTest, emitsTheSameEdgesOffTheTargetIndex) {
    for (const size_t chunkSize : {ChunkConfig::CHUNK_SIZE, size_t {1}, size_t {2}}) {
        _chunkSize = chunkSize;

        std::vector<std::pair<uint64_t, uint64_t>> unfused;
        std::vector<std::pair<uint64_t, uint64_t>> fused;
        runPairsBeforeAndAfterFusion<mlir::db::ScanInEdgesByLabelSrcTgt>(targetLabelledScanWithLabelledSource, unfused, fused);

        EXPECT_EQ(unfused.size(), 5u) << chunkSize;
        EXPECT_EQ(fused, unfused) << chunkSize;
    }
}

TEST_F(FuseScanEdgesByEndpointLabelTest, emitsTheSameEdgesForStackedChecks) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::ScanInEdgesByLabelSrcTgt>(edgeScanWithStackedEndpointChecks, unfused, fused);

    EXPECT_EQ(unfused.size(), 5u);
    EXPECT_EQ(fused, unfused);
}

TEST_F(FuseScanEdgesByEndpointLabelTest, emitsNothingForAnAbsentLabelOnTheOtherEnd) {
    std::vector<std::pair<uint64_t, uint64_t>> unfused;
    std::vector<std::pair<uint64_t, uint64_t>> fused;
    runPairsBeforeAndAfterFusion<mlir::db::ScanOutEdgesByLabelSrcTgt>(sourceLabelledScanWithAbsentLabelledTarget, unfused, fused);

    EXPECT_TRUE(unfused.empty());
    EXPECT_TRUE(fused.empty());
}
