#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "iterators/ChunkConfig.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

template <typename OpType>
size_t countOps(mlir::ModuleOp module) {
    size_t count = 0;
    module.walk([&count](OpType) {
        count++;
    });

    return count;
}

}

// An UNWIND of a literal list whose elements are only compared against a property is the
// disjunction that comparison spells out, so the db program the query compiles to reads
// the same as the OR's - one scan, filtered - with no product of the list against it.
class UnwindValueFilterCodegenTest : public TuringTest {
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

    void expectRows(std::string_view query, const Rows& expected) {
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

        RowSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();

        Rows actual = sink.rows();
        std::sort(actual.begin(), actual.end());

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(UnwindValueFilterCodegenTest, inlineConstraintCompilesToADisjunction) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [32, 99] AS a MATCH (n {age: a}) RETURN n.name, a");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::UnwindConst>(*module), 0u);

    // One equality per element, folded into the single mask the one filter reads.
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(UnwindValueFilterCodegenTest, whereEqualityCompilesToADisjunction) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [32, 99] AS a MATCH (n) WHERE n.age = a RETURN n.name, a");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::UnwindConst>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 1u);
}

TEST_F(UnwindValueFilterCodegenTest, aSingleElementNeedsNoDisjunction) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [32] AS a MATCH (n {age: a}) RETURN n.name, a");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 0u);
}

// A repeated element emits the row it matches once per copy, which a disjunction - a set
// membership - cannot express, so the product stands.
TEST_F(UnwindValueFilterCodegenTest, repeatedElementsKeepTheProduct) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [32, 32] AS a MATCH (n {age: a}) RETURN n.name, a");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}

TEST_F(UnwindValueFilterCodegenTest, anEmptyListKeepsTheProduct) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [] AS a MATCH (n {age: a}) RETURN n.name, a");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}

// A null element is no value to compare against - and neither is a nested list - so the
// list is left to the column it already rides.
TEST_F(UnwindValueFilterCodegenTest, aNullElementKeepsTheProduct) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [32, null] AS a MATCH (n {age: a}) RETURN n.name, a");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}

// The elements reach the projection as well as the comparison, so dropping the unwind has
// to leave the rows carrying what it bound - which the property they matched now stands for.
TEST_F(UnwindValueFilterCodegenTest, projectsTheElementEachRowMatched) {
    expectRows("UNWIND [32, 99] AS a MATCH (n {age: a}) RETURN n.name, a",
               {{"Remy", "32"}, {"Adam", "32"}});
}

TEST_F(UnwindValueFilterCodegenTest, filtersAWhereEqualityAgainstTheElements) {
    expectRows("UNWIND [32, 99] AS a MATCH (n) WHERE n.age = a RETURN n.name, a",
               {{"Remy", "32"}, {"Adam", "32"}});
}

TEST_F(UnwindValueFilterCodegenTest, matchesNothingWhenNoElementIsHeld) {
    expectRows("UNWIND [98, 99] AS a MATCH (n {age: a}) RETURN n.name, a", {});
}

// The product the rewrite declines to touch still has to emit one row per copy.
TEST_F(UnwindValueFilterCodegenTest, repeatedElementsEmitOneRowEach) {
    expectRows("UNWIND [32, 32] AS a MATCH (n {age: a}) RETURN n.name, a",
               {{"Remy", "32"}, {"Remy", "32"}, {"Adam", "32"}, {"Adam", "32"}});
}

// Node IDs compared against the node itself fold the same way, and the disjunction that
// leaves is what fuse_scan_by_node_ids reads: the listed nodes are scanned directly rather
// than every node being crossed with the list and filtered back down.
TEST_F(UnwindValueFilterCodegenTest, nodeIDElementsFuseIntoAConstScan) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [0, 1] AS x MATCH (n) WHERE n = x RETURN n");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::UnwindConst>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ConstScanNodes>(*module), 1u);
}

TEST_F(UnwindValueFilterCodegenTest, scansTheListedNodes) {
    expectRows("UNWIND [0, 1] AS x MATCH (n) WHERE n = x RETURN n.name",
               {{"Remy"}, {"Adam"}});
}

// A node equals a node ID by the number it carries, not by type, so a projection of the
// element cannot read the node column that matched it: the product stands instead.
TEST_F(UnwindValueFilterCodegenTest, projectedNodeIDElementsKeepTheProduct) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [0, 1] AS x MATCH (n) WHERE n = x RETURN n.name, x");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}

TEST_F(UnwindValueFilterCodegenTest, projectsTheNodeIDElementsBesideWhatTheyMatched) {
    expectRows("UNWIND [0, 1] AS x MATCH (n) WHERE n = x RETURN n.name, x",
               {{"Remy", "0"}, {"Adam", "1"}});
}

// A mixed list rides the type-erased column whose cells are compared by their own tag, so
// a string cell simply matches no age - which one typed comparison per element, against a
// column of integers, cannot express.
TEST_F(UnwindValueFilterCodegenTest, heterogeneousElementsKeepTheProduct) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [32, 'x'] AS a MATCH (n) WHERE n.age = a RETURN n.name, a");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}

TEST_F(UnwindValueFilterCodegenTest, dropsTheElementsWhenNothingProjectsThem) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [32, 99] AS a MATCH (n {age: a}) RETURN n.name");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::UnwindConst>(*module), 0u);
}

// A part that continues from a WITH crosses the rows it published with the unwind and its
// scan, so that unwind is a factor of an inner product rather than of the one its equality
// reads - which this match does not see through. The leading part still folds.
TEST_F(UnwindValueFilterCodegenTest, foldsOnlyTheUnwindThatIsAFactorOfItsOwnProduct) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("UNWIND [32] AS a MATCH (n {age: a}) "
                 "WITH n UNWIND ['Remy'] AS nm MATCH (m {name: nm}) RETURN n.name, m.name");

    // The published rows crossed with the second part's own product of the list and its scan.
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::UnwindConst>(*module), 1u);
}

TEST_F(UnwindValueFilterCodegenTest, filtersTwoUnwindsOfOneQuery) {
    expectRows("UNWIND [32] AS a MATCH (n {age: a}) "
               "WITH n UNWIND ['Remy'] AS nm MATCH (m {name: nm}) RETURN n.name, m.name",
               {{"Remy", "Remy"}, {"Adam", "Remy"}});
}

TEST_F(UnwindValueFilterCodegenTest, projectsThePropertyBesideTheElementItMatched) {
    expectRows("UNWIND [32, 99] AS a MATCH (n {age: a}) RETURN n.name, n.age, a",
               {{"Remy", "32", "32"}, {"Adam", "32", "32"}});
}

TEST_F(UnwindValueFilterCodegenTest, filtersAStringPropertyAgainstTheElements) {
    expectRows("UNWIND ['Remy', 'Adam'] AS nm MATCH (n {name: nm}) RETURN n.name, nm",
               {{"Remy", "Remy"}, {"Adam", "Adam"}});
}
