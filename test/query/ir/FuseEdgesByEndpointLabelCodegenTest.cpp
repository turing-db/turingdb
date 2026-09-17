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
class FuseEdgesByEndpointLabelCodegenTest : public TuringTest {
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

    // The hop walks the labels itself and neither the plain hop nor the label check it was
    // cut down by is left.
    void expectFusedToLabelledHop(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(module), 0u);
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

// The second hop of a two-hop pattern carries the first hop's node column, so it is no edge
// scan's to take and stays a hop - one that now walks the labels itself.
TEST_F(FuseEdgesByEndpointLabelCodegenTest, labelledTargetOfASecondOutHopBecomesAByLabelHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b)-->(c:Person) RETURN a, c");

    expectFusedToLabelledHop(*module);

    llvm::SmallVector<mlir::db::GetOutEdgesByLabel> hops = collect<mlir::db::GetOutEdgesByLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);

    ASSERT_EQ(hops.front().getLabels().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(hops.front().getLabels()[0]).getValue(), "Person");
}

TEST_F(FuseEdgesByEndpointLabelCodegenTest, labelledSourceOfASecondInHopBecomesAByLabelHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)<--(b)<--(c:Person) RETURN a, c");

    expectFusedToLabelledHop(*module);
    EXPECT_EQ(countOps<mlir::db::GetInEdgesByLabel>(*module), 1u);
}

// A predicate on the near end anchors the scan on the property index, so the hop off it is
// never the whole edge scan the by-label edge scans read.
TEST_F(FuseEdgesByEndpointLabelCodegenTest, predicateOnTheNearEndKeepsAByLabelHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b:Person) WHERE a.name = 'Remy' RETURN a, b");

    expectFusedToLabelledHop(*module);

    EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByLabel>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 0u);
}

TEST_F(FuseEdgesByEndpointLabelCodegenTest, twoLabelsRideOntoTheHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b)-->(c:Person:Founder) RETURN a, c");

    expectFusedToLabelledHop(*module);

    llvm::SmallVector<mlir::db::GetOutEdgesByLabel> hops = collect<mlir::db::GetOutEdgesByLabel>(*module);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_EQ(hops.front().getLabels().size(), 2u);
}

// A single hop off a whole node scan is the by-label edge scan's shape, which the pass
// before this one takes first.
TEST_F(FuseEdgesByEndpointLabelCodegenTest, singleHopStaysAnEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b:Person) RETURN a, b");

    EXPECT_EQ(countOps<mlir::db::ScanOutEdgesByLabelTgt>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByLabel>(*module), 0u);
}

// The labels sit on the end the hop leaves, which the by-label node scan already carries.
TEST_F(FuseEdgesByEndpointLabelCodegenTest, labelledSourceOfAnOutHopKeepsItsNodeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)-->(b)-->(c) RETURN a, c");

    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetInEdgesByLabel>(*module), 0u);
}

// The type narrows the hop first and there is no op holding both an edge type and a label
// set, so the label stays a check over the by-type hop.
TEST_F(FuseEdgesByEndpointLabelCodegenTest, typedHopKeepsItsLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b)-[:KNOWS_WELL]->(c:Person) RETURN a, c");

    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByType>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
}

// An undirected hop reads both edge directions of its nodes, so neither end is the one it
// reaches.
TEST_F(FuseEdgesByEndpointLabelCodegenTest, undirectedHopKeepsItsLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b)--(c:Person) RETURN a, c");

    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetEdges>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
}
