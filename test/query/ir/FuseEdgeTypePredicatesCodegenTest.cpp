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

#include "IRTestEdgeTypes.h"

#include "IRTestOps.h"

using namespace db;
using namespace turing::test;

// Generates the db program a Cypher query compiles to, passes included, so a test reads
// the shape the engine will lower rather than a hand-written approximation of it.
class FuseEdgeTypePredicatesCodegenTest : public TuringTest {
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
        analyzer.analyze();

        mlir::OpBuilder builder(&_context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        return owningModule;
    }

    // Nothing of the predicate is left: no checks, no boolean op joining them, no filter.
    void expectPredicateFullyPushed(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::OrOp>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::ScanEdges>(module), 0u);
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

// A WHERE disjunction reaches codegen as one check per type joined by db.or, which no
// by-type fusion can read. The fold has to run before them for the types to reach the scan.
TEST_F(FuseEdgeTypePredicatesCodegenTest, orOfTwoTypesReachesTheEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n)-[e]->(m) WHERE e:KNOWS_WELL OR e:INTERESTED_IN RETURN n");

    expectPredicateFullyPushed(*module);

    llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(*module);
    ASSERT_EQ(scans.size(), 1u);
    expectEdgeTypes(scans.front().getEdgeTypes(), {"KNOWS_WELL", "INTERESTED_IN"});
}

// The chain folds left to right in one run, so a third type reaches the scan as well.
TEST_F(FuseEdgeTypePredicatesCodegenTest, orOfThreeTypesReachesTheEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n)-[e]->(m) WHERE e:KNOWS_WELL OR e:INTERESTED_IN OR e:LIKES RETURN n");

    expectPredicateFullyPushed(*module);

    llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(*module);
    ASSERT_EQ(scans.size(), 1u);
    expectEdgeTypes(scans.front().getEdgeTypes(), {"KNOWS_WELL", "INTERESTED_IN", "LIKES"});
}

// Off a labelled scan the disjunction lands on the hop rather than on an edge scan.
TEST_F(FuseEdgeTypePredicatesCodegenTest, orOfTwoTypesReachesAHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n:Person)-[e]->(m) WHERE e:KNOWS_WELL OR e:INTERESTED_IN RETURN n");

    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 0u);

    llvm::SmallVector<mlir::db::GetOutEdgesByType> hops = collect<mlir::db::GetOutEdgesByType>(*module);
    ASSERT_EQ(hops.size(), 1u);
    expectEdgeTypes(hops.front().getEdgeTypes(), {"KNOWS_WELL", "INTERESTED_IN"});
}
