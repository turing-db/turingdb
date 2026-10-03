#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestOps.h"
#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class UnwindEqualityHopTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

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

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\nactual:\n" << actualText;
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(UnwindEqualityHopTest, hopFromTheComparedNodeOpensWithAConstScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [2, 0] AS x MATCH (n)--(m) WHERE n = x RETURN n, m");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);

    const llvm::ArrayRef<int64_t> nodeIDs = constScans.front().getNodeIDs();
    const std::vector<int64_t> actual(nodeIDs.begin(), nodeIDs.end());
    const std::vector<int64_t> expected {0, 2};
    EXPECT_EQ(actual, expected);

    EXPECT_EQ(countOps<mlir::db::UnwindConst>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetEdges>(*module), 1u);
}

TEST_F(UnwindEqualityHopTest, hopFromTheComparedNodeReturnsItsEdges) {
    expectRows("UNWIND [2, 0] AS x MATCH (n)--(m) WHERE n = x RETURN n.name, m.name",
               {{"Computers", "Luc"},
                {"Computers", "Remy"},
                {"Remy", "Adam"},
                {"Remy", "Adam"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"},
                {"Remy", "Ghosts"}});
}

TEST_F(UnwindEqualityHopTest, countStarOverTheHopOpensWithAConstScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [2, 0] AS x MATCH (n)--(m) WHERE n = x RETURN count(*)");

    EXPECT_EQ(countOps<mlir::db::ConstScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::UnwindConst>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 0u);
}

TEST_F(UnwindEqualityHopTest, countStarOverTheHopCountsItsEdges) {
    expectRows("UNWIND [2, 0] AS x MATCH (n)--(m) WHERE n = x RETURN count(*)", {{"8"}});
}
