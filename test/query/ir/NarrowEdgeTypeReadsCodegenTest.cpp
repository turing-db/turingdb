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
class NarrowEdgeTypeReadsCodegenTest : public TuringTest {
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

    // The check folded into the read, so neither it nor its filter is left.
    void expectCheckFolded(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(module), 0u);
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

// The pattern names two types and the WHERE one of them, so the read is narrowed to the
// intersection. Narrowing needs a by-type read to exist, so this only works if the pass runs
// after the by-type fusions.
TEST_F(NarrowEdgeTypeReadsCodegenTest, aCheckNarrowsTheByTypeEdgeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n)-[e:KNOWS_WELL|INTERESTED_IN]->(m) WHERE e:KNOWS_WELL RETURN n");

    expectCheckFolded(*module);

    llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(*module);
    ASSERT_EQ(scans.size(), 1u);
    expectEdgeTypes(scans.front().getEdgeTypes(), {"KNOWS_WELL"});
}

// The same type twice: the intersection changes nothing, so only the check goes.
TEST_F(NarrowEdgeTypeReadsCodegenTest, aCheckTheReadAlreadyGuaranteesIsDropped) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n)-[e:KNOWS_WELL]->(m) WHERE e:KNOWS_WELL RETURN n");

    expectCheckFolded(*module);

    llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(*module);
    ASSERT_EQ(scans.size(), 1u);
    expectEdgeTypes(scans.front().getEdgeTypes(), {"KNOWS_WELL"});
}

// An edge carries one type, so this matches nothing. The read says so with an empty type set
// rather than walking every KNOWS_WELL edge to drop them all.
TEST_F(NarrowEdgeTypeReadsCodegenTest, aCheckNoEdgeOfTheReadPassesEmptiesTheRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n)-[e:KNOWS_WELL]->(m) WHERE e:INTERESTED_IN RETURN n");

    expectCheckFolded(*module);

    llvm::SmallVector<mlir::db::ScanEdgesByType> scans = collect<mlir::db::ScanEdgesByType>(*module);
    ASSERT_EQ(scans.size(), 1u);
    EXPECT_EQ(scans.front().getEdgeTypes().size(), 0u);
}
