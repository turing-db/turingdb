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
class FuseScanEdgesCodegenTest : public TuringTest {
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

    // The program opens on one edge scan and no node scan or hop is left.
    void expectFusedToEdgeScan(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::ScanEdges>(module), 1u);
        EXPECT_EQ(countOps<mlir::db::ScanNodes>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::GetOutEdges>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::GetInEdges>(module), 0u);
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(FuseScanEdgesCodegenTest, bothEndpointsOfAHopBecomeAnEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b) RETURN a, b");

    expectFusedToEdgeScan(*module);

    llvm::SmallVector<mlir::db::ScanEdges> edgeScans = collect<mlir::db::ScanEdges>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    mlir::db::ScanEdges edgeScan = edgeScans.front();

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], edgeScan.getSrcids());
    EXPECT_EQ(columns[1], edgeScan.getTgtids());
}

TEST_F(FuseScanEdgesCodegenTest, targetOnlyProjectionBecomesAnEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b) RETURN b");

    expectFusedToEdgeScan(*module);
}

TEST_F(FuseScanEdgesCodegenTest, reverseHopBecomesAnEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)<--(b) RETURN a, b");

    expectFusedToEdgeScan(*module);
}

TEST_F(FuseScanEdgesCodegenTest, edgeVariableAndItsPropertyReadOffTheEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-[r]->(b) RETURN a, r.name, b");

    expectFusedToEdgeScan(*module);

    llvm::SmallVector<mlir::db::ScanEdges> edgeScans = collect<mlir::db::ScanEdges>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);

    // The edge property is read off the scan's own edge-ID column.
    llvm::SmallVector<mlir::db::GetEdgeProperties> properties = collect<mlir::db::GetEdgeProperties>(*module);
    ASSERT_EQ(properties.size(), 1u);
    EXPECT_EQ(properties.front().getInputEdges(), edgeScans.front().getEids());
}

TEST_F(FuseScanEdgesCodegenTest, labelledEndpointKeepsItsNodeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)-->(b) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
}

// A relationship type compiles to a plain hop and a type check over its edge column, so the
// hop fuses to an edge scan and the by-type scan fusion then folds the check into it.
TEST_F(FuseScanEdgesCodegenTest, typedEdgeFusesIntoAByTypeEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-[:KNOWS_WELL]->(b) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByType>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 0u);

    llvm::SmallVector<mlir::db::ScanEdgesByType> edgeScans = collect<mlir::db::ScanEdgesByType>(*module);
    ASSERT_EQ(edgeScans.size(), 1u);
    EXPECT_EQ(edgeScans.front().getEdgeType(), "KNOWS_WELL");
}

TEST_F(FuseScanEdgesCodegenTest, undirectedHopKeepsItsNodeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)--(b) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetEdges>(*module), 1u);
}

TEST_F(FuseScanEdgesCodegenTest, sourcePredicateSinksToTheScanAndKeepsIt) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b) WHERE a.name = 'Remy' RETURN a, b");

    // The predicate sank onto a's scan and fused into it, so the hop expands the property
    // scan rather than the raw one.
    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
}

TEST_F(FuseScanEdgesCodegenTest, twoHopChainFusesOnlyItsFirstHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b)-->(c) RETURN a, c");

    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
}
