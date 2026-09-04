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

// A pattern component the leading calls do not reach matches on its own and is crossed with
// the rows they drive, which must leave the driven component itself seeded: the column a
// call bound feeding the hop, with no scan of the graph to cross and filter back down.
class CallDrivenIslandCodegenTest : public TuringTest {
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

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(CallDrivenIslandCodegenTest, oneComponentHopsStraightFromTheYieldedColumn) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("CALL db.getNodes([0]) YIELD id AS a MATCH (a)-->(m) RETURN m");

    EXPECT_EQ(countOps<mlir::db::CallProcedure>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
}

TEST_F(CallDrivenIslandCodegenTest, islandIsCrossedWithTheDrivenComponent) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("CALL db.getNodes([0]) YIELD id AS a MATCH (a)-->(m), (x) RETURN a, m, x");

    llvm::SmallVector<mlir::db::CrossProduct> products = collect<mlir::db::CrossProduct>(*module);
    ASSERT_EQ(products.size(), 1u);

    // The island's own root is the only scan: the driven component still opens on the
    // column the call bound, so nothing has to be filtered back down to it.
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);

    llvm::SmallVector<mlir::db::CallProcedure> calls = collect<mlir::db::CallProcedure>(*module);
    ASSERT_EQ(calls.size(), 1u);

    mlir::db::CrossProduct product = products.front();
    EXPECT_TRUE(calls.front()->getParentRegion() == &product.getLeftFactor());
}

// A second yielded column would be left inside the product's factor, out of reach of the
// clause that reads it, so an island makes the whole part fall back to matching and joining.
TEST_F(CallDrivenIslandCodegenTest, secondYieldedColumnDeclinesTheDrivenPath) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("CALL db.getNodes([0]) YIELD id AS a, inEdgeCount MATCH (a)-->(m), (x) RETURN a, m, x, inEdgeCount");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 1u);
}

TEST_F(CallDrivenIslandCodegenTest, yieldedPatternEndpointsDeclineTheDrivenPath) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("CALL db.getEdges([0]) YIELD src, tgt MATCH (src)-->(tgt), (x) RETURN src, tgt, x");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanEdges>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 2u);
}

// Without an island the pair of yielded endpoints still drives the traversal: one is the
// root and the hop reaches the other, which is a pairing rather than a product.
TEST_F(CallDrivenIslandCodegenTest, yieldedPatternEndpointsDriveOneComponent) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("CALL db.getEdges([0]) YIELD src, tgt MATCH (src)-->(tgt) RETURN src, tgt");

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
}
