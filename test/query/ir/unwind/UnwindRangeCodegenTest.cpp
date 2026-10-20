#include <gtest/gtest.h>

#include <memory>
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

#include "IRTestOps.h"

using namespace db;
using namespace turing::test;

class UnwindRangeCodegenTest : public TuringTest {
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

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(UnwindRangeCodegenTest, nestedRangesBuildNoList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(1, 100000) AS i UNWIND range(1, 1000) AS j RETURN count(*)");

    EXPECT_EQ(countOps<mlir::db::UnwindRange>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::Unwind>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 0u);
}

TEST_F(UnwindRangeCodegenTest, aRangeOverAStoredBoundBuildsNoList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) UNWIND range(1, n.age) AS x RETURN n.name, x");

    EXPECT_EQ(countOps<mlir::db::UnwindRange>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::Unwind>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 0u);
}

TEST_F(UnwindRangeCodegenTest, aSteppedRangeBuildsNoList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(10, 1, -3) AS x RETURN x");

    EXPECT_EQ(countOps<mlir::db::UnwindRange>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 0u);
}

TEST_F(UnwindRangeCodegenTest, aRangeAnotherProjectionReadsIsKept) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("WITH range(1, 3) AS xs UNWIND xs AS x RETURN x, xs");

    EXPECT_EQ(countOps<mlir::db::UnwindRange>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::Unwind>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 1u);
}
