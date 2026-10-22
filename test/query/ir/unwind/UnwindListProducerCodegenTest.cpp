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

class UnwindListProducerCodegenTest : public TuringTest {
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

TEST_F(UnwindListProducerCodegenTest, aComprehensionOverARangeBuildsNoList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(1, 65536) AS i UNWIND [x IN range(i, i + 999) | x * 2] AS j RETURN count(*)");

    EXPECT_EQ(countOps<mlir::db::ListComprehension>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Unwind>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::UnwindRange>(*module), 2u);
}

TEST_F(UnwindListProducerCodegenTest, aFilteringComprehensionBuildsNoList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) UNWIND [x IN range(1, 3) WHERE x < n.age | n.name + toString(x)] AS y RETURN n.name, y");

    EXPECT_EQ(countOps<mlir::db::ListComprehension>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 0u);
}

TEST_F(UnwindListProducerCodegenTest, aComprehensionAnotherProjectionReadsIsKept) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("WITH [x IN range(1, 3) | x * 2] AS xs UNWIND xs AS y RETURN y, xs");

    EXPECT_EQ(countOps<mlir::db::ListComprehension>(*module), 1u);
}

TEST_F(UnwindListProducerCodegenTest, aReversedRangeBuildsNoList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(1, 65536) AS i UNWIND reverse(range(i, i + 999)) AS j RETURN count(*)");

    EXPECT_EQ(countOps<mlir::db::Reverse>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::UnwindRange>(*module), 2u);
}

TEST_F(UnwindListProducerCodegenTest, aSliceOfARangeBuildsNoList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(1, 65536) AS i UNWIND range(i, i + 999)[1..] AS j RETURN count(*)");

    EXPECT_EQ(countOps<mlir::db::ListSlice>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::UnwindRange>(*module), 2u);
}

TEST_F(UnwindListProducerCodegenTest, theTailOfARangeBuildsNoList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND range(1, 65536) AS i UNWIND tail(range(i, i + 999)) AS j RETURN count(*)");

    EXPECT_EQ(countOps<mlir::db::Tail>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 0u);
}

TEST_F(UnwindListProducerCodegenTest, aReversedSliceBuildsNoList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND reverse(range(1, 10)[2..5]) AS x RETURN x");

    EXPECT_EQ(countOps<mlir::db::Reverse>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ListSlice>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 0u);
}

TEST_F(UnwindListProducerCodegenTest, aSliceOfAReversedRangeIsBuilt) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND reverse(range(1, 10))[2..5] AS x RETURN x");

    EXPECT_EQ(countOps<mlir::db::ListSlice>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::UnwindRange>(*module), 0u);
}

TEST_F(UnwindListProducerCodegenTest, aComprehensionOverAComprehensionBuildsNoList) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [x IN [y IN range(1, 5) | y * 2] WHERE x > 4] AS z RETURN z");

    EXPECT_EQ(countOps<mlir::db::ListComprehension>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::Range>(*module), 0u);
}
