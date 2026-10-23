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

class UnwindComputedCodegenTest : public TuringTest {
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

TEST_F(UnwindComputedCodegenTest, aSplitPerRowIsComputedInTheUnwind) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(1, 3) AS i UNWIND split(toString(i) + ',a,b', ',') AS j RETURN i, j");

    EXPECT_EQ(countOps<mlir::db::UnwindComputed>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::Unwind>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Split>(*module), 1u);
}

TEST_F(UnwindComputedCodegenTest, aReversedComprehensionIsComputedInTheUnwind) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(1, 65536) AS i UNWIND reverse([x IN range(i, i + 999) | x * 2]) AS j RETURN count(*)");

    EXPECT_EQ(countOps<mlir::db::UnwindComputed>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::Unwind>(*module), 0u);
}

TEST_F(UnwindComputedCodegenTest, aRangeIsCountedOutRatherThanComputed) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(1, 3) AS i UNWIND range(i, i + 9) AS j RETURN count(*)");

    EXPECT_EQ(countOps<mlir::db::UnwindComputed>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::UnwindRange>(*module), 2u);
}

TEST_F(UnwindComputedCodegenTest, aConstantListIsNotComputedInTheUnwind) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(1, 3) AS i UNWIND [1, 2, 3] AS j RETURN count(*)");

    EXPECT_EQ(countOps<mlir::db::UnwindComputed>(*module), 0u);
}

TEST_F(UnwindComputedCodegenTest, aStoredPropertyIsNotComputedInTheUnwind) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) UNWIND n.name AS x RETURN x");

    EXPECT_EQ(countOps<mlir::db::UnwindComputed>(*module), 0u);
}

TEST_F(UnwindComputedCodegenTest, aListAnotherProjectionReadsIsNotComputedInTheUnwind) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(1, 2) AS i WITH i, reverse(range(1, i) + [0]) AS l UNWIND l AS j RETURN i, j, l");

    EXPECT_EQ(countOps<mlir::db::UnwindComputed>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Unwind>(*module), 1u);
}
