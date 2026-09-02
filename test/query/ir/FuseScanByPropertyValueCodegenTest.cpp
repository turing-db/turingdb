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

// Generates the db program a Cypher query compiles to, passes included, so a test reads
// the shape the engine will lower rather than a hand-written approximation of it.
class FuseScanByPropertyValueCodegenTest : public TuringTest {
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

    // The program holds exactly one fused scan, over `property`, and no plain node scan.
    mlir::db::ScanNodesByPropertyValue expectSoleFusedScan(mlir::ModuleOp module, llvm::StringRef property) {
        llvm::SmallVector<mlir::db::ScanNodesByPropertyValue> scans = collect<mlir::db::ScanNodesByPropertyValue>(module);
        EXPECT_EQ(scans.size(), 1u);
        EXPECT_EQ(countOps<mlir::db::ScanNodes>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(module), 0u);

        if (scans.empty()) {
            return mlir::db::ScanNodesByPropertyValue();
        }

        EXPECT_EQ(scans.front().getProperty(), property);
        return scans.front();
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(FuseScanByPropertyValueCodegenTest, rootIntegerEqualityBecomesAFusedScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n.age = 32 RETURN n");

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module, "age");
    ASSERT_TRUE(scan);

    const mlir::IntegerAttr literal = mlir::dyn_cast<mlir::IntegerAttr>(scan.getValue());
    ASSERT_TRUE(literal);
    EXPECT_EQ(literal.getInt(), 32);

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 0u);
}

TEST_F(FuseScanByPropertyValueCodegenTest, constantOnTheLeftBecomesAFusedScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE 32 = n.age RETURN n");

    ASSERT_TRUE(expectSoleFusedScan(*module, "age"));
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByPropertyValueCodegenTest, stringEqualityBecomesAFusedScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n.name = 'Remy' RETURN n.age");

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module, "name");
    ASSERT_TRUE(scan);

    const mlir::StringAttr literal = mlir::dyn_cast<mlir::StringAttr>(scan.getValue());
    ASSERT_TRUE(literal);
    EXPECT_EQ(literal.getValue(), "Remy");

    // The projected property is read off the fused scan; the filter is gone.
    llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);
    EXPECT_EQ(reads.front().getProperty(), "age");
    EXPECT_EQ(reads.front().getInputNodes().getDefiningOp(), scan.getOperation());
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByPropertyValueCodegenTest, boolEqualityBecomesAFusedScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n.isFrench = true RETURN n");

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module, "isFrench");
    ASSERT_TRUE(scan);

    const mlir::BoolAttr literal = mlir::dyn_cast<mlir::BoolAttr>(scan.getValue());
    ASSERT_TRUE(literal);
    EXPECT_TRUE(literal.getValue());
}

TEST_F(FuseScanByPropertyValueCodegenTest, labelledEqualityCarriesTheLabels) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person:Founder) WHERE n.age = 32 RETURN n");

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module, "age");
    ASSERT_TRUE(scan);

    const std::optional<mlir::ArrayAttr> labels = scan.getLabels();
    ASSERT_TRUE(labels);
    ASSERT_EQ(labels->size(), 2u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>((*labels)[0]).getValue(), "Person");
    EXPECT_EQ(mlir::cast<mlir::StringAttr>((*labels)[1]).getValue(), "Founder");

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByPropertyValueCodegenTest, conjunctionFusesTheFirstPredicateAndKeepsTheSecond) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n.age = 32 AND n.isFrench = true RETURN n");

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module, "age");
    ASSERT_TRUE(scan);

    // Codegen splits the conjunction into two filters: the first fused into the scan, the
    // second still reads the other property off the fused rows.
    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    ASSERT_EQ(filters.front().getColumnsToFilter().size(), 1u);
    EXPECT_EQ(filters.front().getColumnsToFilter().front().getDefiningOp(), scan.getOperation());

    llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);
    EXPECT_EQ(reads.front().getProperty(), "isFrench");
}

TEST_F(FuseScanByPropertyValueCodegenTest, equalityOnAnExpandedRootFeedsTheHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n)-->(m) WHERE n.name = 'Remy' RETURN m");

    mlir::db::ScanNodesByPropertyValue scan = expectSoleFusedScan(*module, "name");
    ASSERT_TRUE(scan);

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_EQ(hops.front().getInputNodes().getDefiningOp(), scan.getOperation());

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByPropertyValueCodegenTest, equalityInACrossProductFactor) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n), (m) WHERE n.age = 32 RETURN n, m");

    llvm::SmallVector<mlir::db::ScanNodesByPropertyValue> scans = collect<mlir::db::ScanNodesByPropertyValue>(*module);
    ASSERT_EQ(scans.size(), 1u);
    EXPECT_TRUE(scans.front().getOperation()->getParentOfType<mlir::db::CrossProduct>());

    // The other factor keeps its plain scan.
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(FuseScanByPropertyValueCodegenTest, disjunctionKeepsItsFilter) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Luc' RETURN n");

    EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(FuseScanByPropertyValueCodegenTest, hopTargetEqualityKeepsItsFilter) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n)-->(m) WHERE m.name = 'Bio' RETURN n");

    EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}
