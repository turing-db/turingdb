#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

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

}

// Generates the db program a Cypher query compiles to, passes included, so a test reads
// the shape the engine will lower rather than a hand-written approximation of it.
class FuseScanEdgesByEndpointLabelCodegenTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

protected:
    mlir::OwningOpRef<mlir::ModuleOp> generate(std::string_view query) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
        analyzer.analyze();

        mlir::OpBuilder builder(&_context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        return owningModule;
    }

    // The program opens on one by-label edge scan and neither the whole edge scan nor the
    // label check it was cut down by is left.
    void expectFusedToLabelledEdgeScan(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::ScanEdges>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(module), 0u);
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(FuseScanEdgesByEndpointLabelCodegenTest, labelledTargetOfAnOutHopBecomesAnEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b:Person) RETURN a, b");

    expectFusedToLabelledEdgeScan(*module);

    llvm::SmallVector<mlir::db::ScanOutEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanOutEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanOutEdgesByLabelTgt edgeScan = edgeScans.front();

    ASSERT_EQ(edgeScan.getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeScan.getLabels()[0]).getValue(), "Person");

    // The labelled end is the edge's target, so `b` reads off tgtids and `a` off srcids.
    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], edgeScan.getSrcids());
    EXPECT_EQ(columns[1], edgeScan.getTgtids());
}

TEST_F(FuseScanEdgesByEndpointLabelCodegenTest, labelledSourceOfAnInHopBecomesAnEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)<--(b:Person) RETURN a, b");

    expectFusedToLabelledEdgeScan(*module);

    llvm::SmallVector<mlir::db::ScanInEdgesByLabelSrc> edgeScans = collect<mlir::db::ScanInEdgesByLabelSrc>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanInEdgesByLabelSrc edgeScan = edgeScans.front();

    ASSERT_EQ(edgeScan.getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeScan.getLabels()[0]).getValue(), "Person");

    // `b` is the labelled end the in-hop leaves, which is the edge's source, so the two
    // columns cross on the way out.
    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], edgeScan.getTgtids());
    EXPECT_EQ(columns[1], edgeScan.getSrcids());
}

TEST_F(FuseScanEdgesByEndpointLabelCodegenTest, labelledEndpointOnlyProjectionBecomesAnEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b:Person) RETURN b");

    expectFusedToLabelledEdgeScan(*module);
    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(*module), 1u);
}

TEST_F(FuseScanEdgesByEndpointLabelCodegenTest, twoLabelsRideOntoTheEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b:Person:Founder) RETURN a, b");

    expectFusedToLabelledEdgeScan(*module);

    llvm::SmallVector<mlir::db::ScanOutEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanOutEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    EXPECT_EQ(edgeScans.front().getLabels().size(), 2u);
}

// The hop's own end carries the labels, so the by-label hop fusion has already taken it and
// there is no whole edge scan left to cut down.
TEST_F(FuseScanEdgesByEndpointLabelCodegenTest, labelledSourceOfAnOutHopKeepsItsOwnScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)-->(b) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelSrc>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelSrc>(*module), 0u);
}

TEST_F(FuseScanEdgesByEndpointLabelCodegenTest, labelledTargetOfAnInHopKeepsItsOwnScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)<--(b) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelSrc>(*module), 0u);
}

// Both ends carry labels: the hop's own end fuses into the by-label scan and the other
// stays a label check over it, since no op holds a label set per endpoint.
TEST_F(FuseScanEdgesByEndpointLabelCodegenTest, labelledBothEndsKeepsTheCheckOnTheFarOne) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)-->(b:Interest) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelSrc>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
}

// An undirected hop reads both edge directions of its nodes, which is no edge scan.
TEST_F(FuseScanEdgesByEndpointLabelCodegenTest, labelledUndirectedHopKeepsItsNodeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)--(b:Person) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelSrc>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetEdges>(*module), 1u);
}

// The type narrows the scan first and there is no op holding both an edge type and a label
// set, so the label stays a check over the by-type scan.
TEST_F(FuseScanEdgesByEndpointLabelCodegenTest, typedLabelledHopKeepsItsByTypeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-[:KNOWS_WELL]->(b:Person) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanEdgesByType>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
}

// A predicate on the other end anchors the scan on the property index, so the hop never
// becomes the whole edge scan this pass reads - it stays a hop, and the by-label hop fusion
// takes the label instead.
TEST_F(FuseScanEdgesByEndpointLabelCodegenTest, predicateOnTheFarEndKeepsItsPropertyScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b:Person) WHERE a.name = 'Remy' RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByLabel>(*module), 1u);
}
