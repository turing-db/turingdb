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
class FuseEdgesByTypeCodegenTest : public TuringTest {
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

    // No plain hop and no edge-type check is left: the walk itself keeps the type.
    void expectFusedToTypedHop(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::GetOutEdges>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::GetInEdges>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(module), 0u);
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(FuseEdgesByTypeCodegenTest, typedHopOffALabelScanBecomesATypedHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)-[:KNOWS_WELL]->(b) RETURN a, b");

    expectFusedToTypedHop(*module);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);

    llvm::SmallVector<mlir::db::GetOutEdgesByType> hops = collect<mlir::db::GetOutEdgesByType>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdgesByType hop = hops.front();
    EXPECT_EQ(hop.getEdgeType(), "KNOWS_WELL");
    EXPECT_TRUE(hop.getInputNodes().getDefiningOp<mlir::db::ScanNodesByLabel>());

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    const mlir::Operation::operand_range columns = outputs.front().getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(columns[0], hop.getSrcids());
    EXPECT_EQ(columns[1], hop.getTgtids());
}

TEST_F(FuseEdgesByTypeCodegenTest, reverseTypedHopBecomesATypedInHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)<-[:KNOWS_WELL]-(b) RETURN a, b");

    expectFusedToTypedHop(*module);

    llvm::SmallVector<mlir::db::GetInEdgesByType> hops = collect<mlir::db::GetInEdgesByType>(*module);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_EQ(hops.front().getEdgeType(), "KNOWS_WELL");
}

TEST_F(FuseEdgesByTypeCodegenTest, edgePropertyIsReadOffTheTypedHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (a:Person)-[r:KNOWS_WELL]->(b) RETURN a, r.name, b");

    expectFusedToTypedHop(*module);

    llvm::SmallVector<mlir::db::GetOutEdgesByType> hops = collect<mlir::db::GetOutEdgesByType>(*module);
    ASSERT_EQ(hops.size(), 1u);

    llvm::SmallVector<mlir::db::GetEdgeProperties> properties = collect<mlir::db::GetEdgeProperties>(*module);
    ASSERT_EQ(properties.size(), 1u);
    EXPECT_EQ(properties.front().getInputEdges(), hops.front().getEids());
}

// The edge type is applied before the target's labels, so the hop fuses and the label
// constraint stays behind it as an ordinary filter.
TEST_F(FuseEdgesByTypeCodegenTest, labelledTargetKeepsItsFilterBehindTheTypedHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (a:Person)-[:KNOWS_WELL]->(b:Interest) RETURN a, b");

    expectFusedToTypedHop(*module);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByType>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

// A predicate on the far end sits behind the type check, so the hop fuses under it.
TEST_F(FuseEdgesByTypeCodegenTest, predicateOnTheTargetKeepsTheTypedHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (a:Person)-[:KNOWS_WELL]->(b) WHERE b.name = 'Remy' RETURN a, b");

    expectFusedToTypedHop(*module);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByType>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(FuseEdgesByTypeCodegenTest, bothHopsOfATypedChainFuse) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (a:Person)-[:KNOWS_WELL]->(b)-[:INTERESTED_IN]->(c) RETURN a, c");

    expectFusedToTypedHop(*module);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);

    llvm::SmallVector<mlir::db::GetOutEdgesByType> hops = collect<mlir::db::GetOutEdgesByType>(*module);
    ASSERT_EQ(hops.size(), 2u);
    EXPECT_EQ(hops[0].getEdgeType(), "KNOWS_WELL");
    EXPECT_EQ(hops[1].getEdgeType(), "INTERESTED_IN");
    EXPECT_EQ(hops[1].getInputNodes(), hops[0].getTgtids());
}

// A whole-graph scan and its hop are the edge set, which the edge-scan fusion takes first,
// so the type lands on that scan as a scan_edges_by_type rather than on the walk.
TEST_F(FuseEdgesByTypeCodegenTest, typedHopOffAWholeGraphScanBecomesAByTypeEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-[:KNOWS_WELL]->(b) RETURN a, b");

    expectFusedToTypedHop(*module);
    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByType>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanEdgesByType>(*module), 1u);
}

TEST_F(FuseEdgesByTypeCodegenTest, undirectedTypedHopKeepsItsTypeCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)-[:KNOWS_WELL]-(b) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::GetEdges>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByType>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetInEdgesByType>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 1u);
}
