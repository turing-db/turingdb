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

// Generates the db program a Cypher query compiles to, passes included, so a test reads
// the shape the engine will lower rather than a hand-written approximation of it.
class FuseScanByNodeIDsCodegenTest : public TuringTest {
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

TEST_F(FuseScanByNodeIDsCodegenTest, rootDisjunctionBecomesConstScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n = 5 OR n = 2 RETURN n");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);

    std::vector<int64_t> nodeIDs;
    nodeIDsOf(constScans.front(), nodeIDs);
    const std::vector<int64_t> expected {2, 5};
    EXPECT_EQ(nodeIDs, expected);

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByNodeIDsCodegenTest, disjunctionOnAnExpandedRootFeedsTheHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n)-->(m) WHERE n = 1 OR n = 3 RETURN n, m");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);
    mlir::db::ConstScanNodes constScan = constScans.front();

    std::vector<int64_t> nodeIDs;
    nodeIDsOf(constScan, nodeIDs);
    const std::vector<int64_t> expected {1, 3};
    EXPECT_EQ(nodeIDs, expected);

    // The filter sank to the scan and fused into it, so the hop expands the listed set.
    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_EQ(hops.front().getInputNodes().getDefiningOp(), constScan.getOperation());

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByNodeIDsCodegenTest, disjunctionInACrossProductFactor) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n), (m) WHERE n = 2 OR n = 4 RETURN n, m");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);
    mlir::db::ConstScanNodes constScan = constScans.front();

    std::vector<int64_t> nodeIDs;
    nodeIDsOf(constScan, nodeIDs);
    const std::vector<int64_t> expected {2, 4};
    EXPECT_EQ(nodeIDs, expected);

    // The const scan sits inside a factor of the product; the other factor keeps its scan.
    EXPECT_TRUE(constScan.getOperation()->getParentOfType<mlir::db::CrossProduct>());
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByNodeIDsCodegenTest, mixedPredicateKeepsItsFilter) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n = 5 OR n.name = 'Remy' RETURN n");

    EXPECT_EQ(countOps<mlir::db::ConstScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(FuseScanByNodeIDsCodegenTest, labelledDisjunctionKeepsALabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) WHERE n = 2 OR n = 0 RETURN n");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);
    mlir::db::ConstScanNodes constScan = constScans.front();

    std::vector<int64_t> nodeIDs;
    nodeIDsOf(constScan, nodeIDs);
    const std::vector<int64_t> expected {0, 2};
    EXPECT_EQ(nodeIDs, expected);

    // The Person test survives as a check over the listed nodes; no scan of any kind remains.
    llvm::SmallVector<mlir::db::CheckLabelConstraint> checks = collect<mlir::db::CheckLabelConstraint>(*module);
    ASSERT_EQ(checks.size(), 1u);
    const mlir::ArrayAttr labels = checks.front().getLabels();
    ASSERT_EQ(labels.size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[0]).getValue(), "Person");

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
}

TEST_F(FuseScanByNodeIDsCodegenTest, filteredClauseCrossedWithASecondClause) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n = 0 OR n = 1 MATCH (m) RETURN count(*)");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);
    mlir::db::ConstScanNodes constScan = constScans.front();

    std::vector<int64_t> nodeIDs;
    nodeIDsOf(constScan, nodeIDs);
    const std::vector<int64_t> expected {0, 1};
    EXPECT_EQ(nodeIDs, expected);

    EXPECT_TRUE(constScan.getOperation()->getParentOfType<mlir::db::CrossProduct>());
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByNodeIDsCodegenTest, laterClausePredicateOnAnEarlierClauseVariable) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) MATCH (m) WHERE n = 0 OR n = 1 RETURN count(*)");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);
    mlir::db::ConstScanNodes constScan = constScans.front();

    std::vector<int64_t> nodeIDs;
    nodeIDsOf(constScan, nodeIDs);
    const std::vector<int64_t> expected {0, 1};
    EXPECT_EQ(nodeIDs, expected);

    // The predicate sits after the product in the query; it sinks into n's factor and fuses.
    EXPECT_TRUE(constScan.getOperation()->getParentOfType<mlir::db::CrossProduct>());
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByNodeIDsCodegenTest, bothClausesFiltered) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n) WHERE n = 0 OR n = 1 MATCH (m) WHERE m = 3 OR m = 2 RETURN n, m");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 2u);

    std::vector<int64_t> firstIDs;
    nodeIDsOf(constScans[0], firstIDs);
    const std::vector<int64_t> expectedFirst {0, 1};
    EXPECT_EQ(firstIDs, expectedFirst);

    std::vector<int64_t> secondIDs;
    nodeIDsOf(constScans[1], secondIDs);
    const std::vector<int64_t> expectedSecond {2, 3};
    EXPECT_EQ(secondIDs, expectedSecond);

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByNodeIDsCodegenTest, filteredClauseExpandedByALaterClause) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n = 0 OR n = 1 MATCH (n)-->(m) RETURN m");

    llvm::SmallVector<mlir::db::ConstScanNodes> constScans = collect<mlir::db::ConstScanNodes>(*module);
    ASSERT_EQ(constScans.size(), 1u);
    mlir::db::ConstScanNodes constScan = constScans.front();

    std::vector<int64_t> nodeIDs;
    nodeIDsOf(constScan, nodeIDs);
    const std::vector<int64_t> expected {0, 1};
    EXPECT_EQ(nodeIDs, expected);

    // The second clause's hop expands the listed set directly.
    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_EQ(hops.front().getInputNodes().getDefiningOp(), constScan.getOperation());

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}
