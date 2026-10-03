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

#include "IRTestOps.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view searchTwo = "VECTOR SEARCH IN people FOR 2 (1.0, 0.0) YIELD ids ";

}

class FuseScanByNodeColumnCodegenTest : public TuringTest {
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

TEST_F(FuseScanByNodeColumnCodegenTest, labelledMatchChecksTheSearchedNodes) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate(std::string(searchTwo) + "MATCH (n:Person) WHERE n = ids RETURN n.name");

    llvm::SmallVector<mlir::db::CheckNodeExists> existenceChecks = collect<mlir::db::CheckNodeExists>(*module);
    ASSERT_EQ(existenceChecks.size(), 1u);
    EXPECT_TRUE(existenceChecks.front().getInputNodes().getDefiningOp<mlir::db::VectorSearch>());

    llvm::SmallVector<mlir::db::CheckLabelConstraint> labelChecks = collect<mlir::db::CheckLabelConstraint>(*module);
    ASSERT_EQ(labelChecks.size(), 1u);
    const mlir::ArrayAttr labels = labelChecks.front().getLabels();
    ASSERT_EQ(labels.size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[0]).getValue(), "Person");

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 0u);
}

TEST_F(FuseScanByNodeColumnCodegenTest, unlabelledMatchOnlyChecksTheNodesExist) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate(std::string(searchTwo) + "MATCH (n) WHERE n = ids RETURN n.name");

    EXPECT_EQ(countOps<mlir::db::CheckNodeExists>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 0u);
}

TEST_F(FuseScanByNodeColumnCodegenTest, equalityWithOtherConjunctsKeepsThem) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate(std::string(searchTwo) + "MATCH (n:Person) WHERE n = ids AND n.age > 20 RETURN n.name");

    EXPECT_EQ(countOps<mlir::db::CheckNodeExists>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GtOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
}

TEST_F(FuseScanByNodeColumnCodegenTest, matchOnAnEarlierClauseVariableDropsTheSecondScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (a:Interest) WITH a MATCH (n:Person) WHERE n = a RETURN n.name");

    llvm::SmallVector<mlir::db::ScanNodesByLabel> scans = collect<mlir::db::ScanNodesByLabel>(*module);
    ASSERT_EQ(scans.size(), 1u);
    const mlir::StringRef scannedLabel = mlir::cast<mlir::StringAttr>(scans.front().getLabels()[0]).getValue();

    llvm::SmallVector<mlir::db::CheckNodeExists> existenceChecks = collect<mlir::db::CheckNodeExists>(*module);
    ASSERT_EQ(existenceChecks.size(), 1u);
    EXPECT_EQ(existenceChecks.front().getInputNodes().getDefiningOp(), scans.front().getOperation());

    llvm::SmallVector<mlir::db::CheckLabelConstraint> labelChecks = collect<mlir::db::CheckLabelConstraint>(*module);
    ASSERT_EQ(labelChecks.size(), 1u);
    const mlir::StringRef checkedLabel = mlir::cast<mlir::StringAttr>(labelChecks.front().getLabels()[0]).getValue();

    const bool scansOneLabelAndChecksTheOther = (scannedLabel == "Interest" && checkedLabel == "Person")
                                             || (scannedLabel == "Person" && checkedLabel == "Interest");
    EXPECT_TRUE(scansOneLabelAndChecksTheOther) << scannedLabel.str() << ", " << checkedLabel.str();

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 0u);
}

TEST_F(FuseScanByNodeColumnCodegenTest, unlabelledScanIsTheOneDropped) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person), (n) WHERE n = a RETURN n.name");

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckNodeExists>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
}

TEST_F(FuseScanByNodeColumnCodegenTest, disjunctionKeepsTheScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate(std::string(searchTwo) + "MATCH (n:Person) WHERE n = ids OR n.name = 'Remy' RETURN n.name");

    EXPECT_EQ(countOps<mlir::db::CheckNodeExists>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}
