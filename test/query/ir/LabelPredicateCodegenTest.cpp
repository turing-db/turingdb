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

void namesOf(mlir::ArrayAttr attr, std::vector<std::string>& names) {
    names.clear();

    for (const mlir::Attribute name : attr) {
        const llvm::StringRef text = mlir::cast<mlir::StringAttr>(name).getValue();
        names.emplace_back(text.data(), text.size());
    }
}

}

// The db program a WHERE label predicate compiles to, passes included. Codegen emits the
// same label-set fetch and check whatever shape the predicate sits in, and the fusion pass
// is what turns the one over a bare scan into a scan by label - which is why the generator
// has no pushdown of its own.
class LabelPredicateCodegenTest : public TuringTest {
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

// `WHERE n:Person` over a bare scan is the shape `MATCH (n:Person)` already compiled to.
TEST_F(LabelPredicateCodegenTest, aRootLabelPredicateFusesIntoTheScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n:Person RETURN n");

    llvm::SmallVector<mlir::db::ScanNodesByLabel> scans = collect<mlir::db::ScanNodesByLabel>(*module);
    ASSERT_EQ(scans.size(), 1u);

    std::vector<std::string> labels;
    namesOf(scans.front().getLabels(), labels);
    const std::vector<std::string> expected {"Person"};
    EXPECT_EQ(labels, expected);

    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(LabelPredicateCodegenTest, aChainOfLabelsFusesAsTheWholeConjunction) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n) WHERE n:Person:SoftwareEngineering RETURN n");

    llvm::SmallVector<mlir::db::ScanNodesByLabel> scans = collect<mlir::db::ScanNodesByLabel>(*module);
    ASSERT_EQ(scans.size(), 1u);

    std::vector<std::string> labels;
    namesOf(scans.front().getLabels(), labels);
    const std::vector<std::string> expected {"Person", "SoftwareEngineering"};
    EXPECT_EQ(labels, expected);
}

// A disjunction is no scan: both sides are computed as columns and the filter takes the
// rows either one keeps.
TEST_F(LabelPredicateCodegenTest, aDisjunctionOfLabelsStaysACheckPerSide) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n) WHERE n:Founder OR n:Sales RETURN n");

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
}

TEST_F(LabelPredicateCodegenTest, aNegatedLabelPredicateStaysACheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n) WHERE NOT n:Person RETURN n");

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::NotOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
}

// A label the graph never assigned is a label like any other to codegen: what no node
// carries is settled against the graph when the check lowers, not here.
TEST_F(LabelPredicateCodegenTest, anUnknownLabelCompilesLikeAnyOther) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n) WHERE n:Sasquatch RETURN n");

    llvm::SmallVector<mlir::db::ScanNodesByLabel> scans = collect<mlir::db::ScanNodesByLabel>(*module);
    ASSERT_EQ(scans.size(), 1u);

    std::vector<std::string> labels;
    namesOf(scans.front().getLabels(), labels);
    const std::vector<std::string> expected {"Sasquatch"};
    EXPECT_EQ(labels, expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
