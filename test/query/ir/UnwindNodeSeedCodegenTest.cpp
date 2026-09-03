#include <gtest/gtest.h>

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

#include "TuringException.h"
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

void nodeIDsOf(mlir::db::ConstScanNodes constScan, std::vector<int64_t>& nodeIDs) {
    const llvm::ArrayRef<int64_t> listed = constScan.getNodeIDs();
    nodeIDs.assign(listed.begin(), listed.end());
}

}

// An UNWIND whose variable names a pattern node opens the dataflow from those nodes, so the
// db program the query compiles to reads the same as the node-ID disjunction's - a const
// scan feeding the hop, with no scan of the graph to cross and filter.
class UnwindNodeSeedCodegenTest : public TuringTest {
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

TEST_F(UnwindNodeSeedCodegenTest, seededHopOpensWithAConstScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [5, 2] AS x MATCH (x)-->(w) RETURN w");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);

    std::vector<int64_t> nodeIDs;
    nodeIDsOf(constScans.front(), nodeIDs);
    const std::vector<int64_t> expected {5, 2};
    EXPECT_EQ(nodeIDs, expected);

    EXPECT_EQ(countOps<mlir::db::UnwindConst>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
}

// The list's own order rides the op: the disjunction form sorts and dedups its IDs because
// it stands for a set, and this one must not.
TEST_F(UnwindNodeSeedCodegenTest, keepsTheListOrder) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [5, 2] AS x MATCH (x) RETURN x");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);

    std::vector<int64_t> nodeIDs;
    nodeIDsOf(constScans.front(), nodeIDs);
    const std::vector<int64_t> expected {5, 2};
    EXPECT_EQ(nodeIDs, expected);
}

TEST_F(UnwindNodeSeedCodegenTest, keepsARepeatedID) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [3, 3] AS x MATCH (x) RETURN x");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);

    std::vector<int64_t> nodeIDs;
    nodeIDsOf(constScans.front(), nodeIDs);
    const std::vector<int64_t> expected {3, 3};
    EXPECT_EQ(nodeIDs, expected);
}

TEST_F(UnwindNodeSeedCodegenTest, labelledSeedKeepsALabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [5, 2] AS x MATCH (x:Person) RETURN x");

    EXPECT_EQ(countOps<mlir::db::ConstScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
}

// The pattern is walked from the seed whichever end of the hop it sits on.
TEST_F(UnwindNodeSeedCodegenTest, seedAtTheFarEndWalksTheHopBackwards) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [1] AS x MATCH (a)-->(x) RETURN a");

    EXPECT_EQ(countOps<mlir::db::ConstScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetInEdges>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
}

// Only the node a component opens from can be seeded: a second seed inside one pattern
// would be reached by the hop, whose rows would take it over and leave its list unmatched.
TEST_F(UnwindNodeSeedCodegenTest, rejectsASeedAHopReaches) {
    EXPECT_THROW(generate("UNWIND [0] AS x UNWIND [1] AS y MATCH (x)-->(y) RETURN x, y"), TuringException);
}

TEST_F(UnwindNodeSeedCodegenTest, rejectsASeedTheHopReturnsTo) {
    EXPECT_THROW(generate("UNWIND [0] AS x MATCH (x)-->(x) RETURN x"), TuringException);
}

// A barrier publishes the unwound variable as the integers it held, not as the nodes a
// later pattern would expand.
TEST_F(UnwindNodeSeedCodegenTest, rejectsASeedUsedAsANodeAfterAWith) {
    EXPECT_THROW(generate("UNWIND [0] AS x WITH x MATCH (x)-->(w) RETURN w"), TuringException);
}

// An UNWIND naming no pattern node still binds a column of values, whatever the query then
// compares it to.
TEST_F(UnwindNodeSeedCodegenTest, valueUnwindStaysAnUnwindConst) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [5, 2] AS x MATCH (n) WHERE n = x RETURN n");

    EXPECT_EQ(countOps<mlir::db::UnwindConst>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ConstScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
}

TEST_F(UnwindNodeSeedCodegenTest, unwindWithoutAMatchStaysAnUnwindConst) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [5, 2] AS x RETURN x");

    EXPECT_EQ(countOps<mlir::db::UnwindConst>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ConstScanNodes>(*module), 0u);
}
