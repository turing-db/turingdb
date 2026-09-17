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
class FuseScanInEdgesByLabelCodegenTest : public TuringTest {
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

    // The program opens on one by-label in-edge scan and no node scan or hop is left.
    void expectFusedToLabelledEdgeScan(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(module), 1u);
        EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::GetInEdges>(module), 0u);
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(FuseScanInEdgesByLabelCodegenTest, bothEndpointsOfALabelledInHopBecomeAnEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)<--(b) RETURN a, b");

    expectFusedToLabelledEdgeScan(*module);

    llvm::SmallVector<mlir::db::ScanInEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanInEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanInEdgesByLabelTgt edgeScan = edgeScans.front();

    ASSERT_EQ(edgeScan.getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeScan.getLabels()[0]).getValue(), "Person");

    // The labelled end is the edge's target, so `a` reads off tgtids and `b` off srcids.
    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], edgeScan.getTgtids());
    EXPECT_EQ(columns[1], edgeScan.getSrcids());
}

TEST_F(FuseScanInEdgesByLabelCodegenTest, sourceOnlyProjectionBecomesAnEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)<--(b) RETURN b");

    expectFusedToLabelledEdgeScan(*module);
}

TEST_F(FuseScanInEdgesByLabelCodegenTest, twoLabelsRideOntoTheEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person:Founder)<--(b) RETURN a, b");

    expectFusedToLabelledEdgeScan(*module);

    llvm::SmallVector<mlir::db::ScanInEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanInEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    EXPECT_EQ(edgeScans.front().getLabels().size(), 2u);
}

// The labelled variable's property is read off the target column, the end the labels
// constrain.
TEST_F(FuseScanInEdgesByLabelCodegenTest, labelledPropertyIsReadOffTheTargetColumn) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)<--(b) RETURN a.name, b");

    expectFusedToLabelledEdgeScan(*module);

    llvm::SmallVector<mlir::db::ScanInEdgesByLabelTgt> edgeScans = collect<mlir::db::ScanInEdgesByLabelTgt>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);

    llvm::SmallVector<mlir::db::GetNodeProperties> properties = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(properties.size(), 1u);
    EXPECT_EQ(properties.front().getInputNodes(), edgeScans.front().getTgtids());
}

// An out-hop compiles to the out-edge scan instead, so the two passes do not contend.
TEST_F(FuseScanInEdgesByLabelCodegenTest, labelledOutHopBecomesTheOutEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)-->(b) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelSrc>(*module), 1u);
}

TEST_F(FuseScanInEdgesByLabelCodegenTest, labelledUndirectedHopKeepsItsNodeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)--(b) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetEdges>(*module), 1u);
}

// The by-type hop fusion runs first and keeps the edge type on the hop, which the by-label
// edge scan cannot express, so the type wins and the node scan stays.
TEST_F(FuseScanInEdgesByLabelCodegenTest, typedLabelledInHopKeepsItsByTypeHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)<-[:KNOWS_WELL]-(b) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetInEdgesByType>(*module), 1u);
}

TEST_F(FuseScanInEdgesByLabelCodegenTest, unlabelledInHopStillBecomesAWholeEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)<--(b) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 1u);
}

TEST_F(FuseScanInEdgesByLabelCodegenTest, twoHopChainFusesOnlyItsFirstHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)<--(b)<--(c) RETURN a, c");

    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetInEdges>(*module), 1u);
}

TEST_F(FuseScanInEdgesByLabelCodegenTest, labelledPredicateSinksToTheScanAndKeepsIt) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)<--(b) WHERE a.name = 'Remy' RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanInEdgesByLabelTgt>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetInEdges>(*module), 1u);
}
