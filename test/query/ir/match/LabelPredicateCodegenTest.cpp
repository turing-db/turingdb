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

#include "IRTestOps.h"

using namespace db;
using namespace turing::test;

namespace {

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

// A disjunction is no scan, but it is one check: each side becomes one alternative of it.
TEST_F(LabelPredicateCodegenTest, aDisjunctionOfLabelsIsOneCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n) WHERE n:Person:Founder OR n:Sales OR n:Exotic RETURN n");

    llvm::SmallVector<mlir::db::CheckLabelConstraint> checks = collect<mlir::db::CheckLabelConstraint>(*module);
    ASSERT_EQ(checks.size(), 1u);

    const mlir::ArrayAttr alternatives = checks.front().getAlternatives();
    ASSERT_EQ(alternatives.size(), 3u);

    std::vector<std::string> labels;
    namesOf(mlir::cast<mlir::ArrayAttr>(alternatives[0]), labels);
    EXPECT_EQ(labels, (std::vector<std::string> {"Person", "Founder"}));
    namesOf(mlir::cast<mlir::ArrayAttr>(alternatives[1]), labels);
    EXPECT_EQ(labels, (std::vector<std::string> {"Sales"}));
    namesOf(mlir::cast<mlir::ArrayAttr>(alternatives[2]), labels);
    EXPECT_EQ(labels, (std::vector<std::string> {"Exotic"}));

    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 0u);
}

// n:Person:Founder adds no node n:Person does not already keep, so what is left is one
// conjunction, and that fuses into the scan.
TEST_F(LabelPredicateCodegenTest, aDisjunctionThatIsOneConjunctionFusesIntoTheScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n) WHERE n:Person:Founder OR n:Person RETURN n");

    llvm::SmallVector<mlir::db::ScanNodesByLabel> scans = collect<mlir::db::ScanNodesByLabel>(*module);
    ASSERT_EQ(scans.size(), 1u);

    std::vector<std::string> labels;
    namesOf(scans.front().getLabels(), labels);
    EXPECT_EQ(labels, (std::vector<std::string> {"Person"}));

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 0u);
}

// Codegen filters once per conjunct, so the two labels arrive as stacked filters.
TEST_F(LabelPredicateCodegenTest, aConjunctionOfLabelsFusesIntoTheScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n) WHERE n:Person AND n:Founder RETURN n");

    llvm::SmallVector<mlir::db::ScanNodesByLabel> scans = collect<mlir::db::ScanNodesByLabel>(*module);
    ASSERT_EQ(scans.size(), 1u);

    std::vector<std::string> labels;
    namesOf(scans.front().getLabels(), labels);
    EXPECT_EQ(labels, (std::vector<std::string> {"Person", "Founder"}));

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(LabelPredicateCodegenTest, aPatternLabelAndAWhereLabelFuseIntoTheScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n:Person) WHERE n:Founder RETURN n");

    llvm::SmallVector<mlir::db::ScanNodesByLabel> scans = collect<mlir::db::ScanNodesByLabel>(*module);
    ASSERT_EQ(scans.size(), 1u);

    std::vector<std::string> labels;
    namesOf(scans.front().getLabels(), labels);
    EXPECT_EQ(labels, (std::vector<std::string> {"Person", "Founder"}));

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

// The Founder filter codegen puts after the hop moves above it, onto the Person one.
TEST_F(LabelPredicateCodegenTest, aWhereLabelOnAHopSourceJoinsThePatternLabel) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n:Person)-->(m) WHERE n:Founder RETURN n, m");

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(LabelPredicateCodegenTest, aConjunctionInsideADisjunctionIsOneCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n) WHERE (n:Person AND n:Founder) OR n:Sales RETURN n");

    llvm::SmallVector<mlir::db::CheckLabelConstraint> checks = collect<mlir::db::CheckLabelConstraint>(*module);
    ASSERT_EQ(checks.size(), 1u);

    const mlir::ArrayAttr alternatives = checks.front().getAlternatives();
    ASSERT_EQ(alternatives.size(), 2u);

    std::vector<std::string> labels;
    namesOf(mlir::cast<mlir::ArrayAttr>(alternatives[0]), labels);
    EXPECT_EQ(labels, (std::vector<std::string> {"Person", "Founder"}));
    namesOf(mlir::cast<mlir::ArrayAttr>(alternatives[1]), labels);
    EXPECT_EQ(labels, (std::vector<std::string> {"Sales"}));

    EXPECT_EQ(countOps<mlir::db::AndOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 0u);
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

// The traversal already published the type of each edge it walked, so the check reads that
// column - and the passes go further, collapsing the scan, the hop and the check into the
// one by-type edge scan that walks only the edges the predicate keeps.
TEST_F(LabelPredicateCodegenTest, anEdgeTypePredicateOnAHopFusesIntoAByTypeScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n)-[e]->(m) WHERE e:KNOWS_WELL RETURN n");

    EXPECT_EQ(countOps<mlir::db::ScanEdgesByType>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CheckEdgeTypeConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetEdgeTypes>(*module), 0u);
}

// An undirected hop is no by-type hop, so the check stays - still reading the column the
// traversal published rather than a read of its own.
TEST_F(LabelPredicateCodegenTest, anEdgeTypePredicateReadsTheColumnAHopPublished) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n)-[e]-(m) WHERE e:KNOWS_WELL RETURN n");

    llvm::SmallVector<mlir::db::CheckEdgeTypeConstraint> checks =
        collect<mlir::db::CheckEdgeTypeConstraint>(*module);
    ASSERT_EQ(checks.size(), 1u);

    EXPECT_TRUE(mlir::isa<mlir::db::GetEdges>(checks.front().getEdgeTypeIds().getDefiningOp()));
    EXPECT_EQ(countOps<mlir::db::GetEdgeTypes>(*module), 0u);
}

// Below a barrier there is no such column, so the check reads the type of the edge the row
// holds instead.
TEST_F(LabelPredicateCodegenTest, anEdgeTypePredicateBelowABarrierFetchesTheType) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n)-[e]->(m) WITH e, n WHERE e:KNOWS_WELL RETURN n");

    llvm::SmallVector<mlir::db::GetEdgeTypes> fetches = collect<mlir::db::GetEdgeTypes>(*module);
    ASSERT_EQ(fetches.size(), 1u);

    llvm::SmallVector<mlir::db::CheckEdgeTypeConstraint> checks =
        collect<mlir::db::CheckEdgeTypeConstraint>(*module);
    ASSERT_EQ(checks.size(), 1u);
    EXPECT_EQ(checks.front().getEdgeTypeIds().getDefiningOp(), fetches.front().getOperation());
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
