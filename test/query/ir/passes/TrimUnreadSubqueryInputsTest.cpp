#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBInterfaces.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An op with a body takes every column in flight as an input, which comes into the body as an
// argument. One the body never reads leaves the op, unless it sizes the rows the body runs
// over or the op hands it back to a reader past it. A subquery in the body of a list op that
// no longer takes the element is then computed once per row, in front of the list op.
// Remy and Adam know each other well, and no other person knows anyone well.
class TrimUnreadSubqueryInputsTest : public WriteQueryTest {
protected:
    struct RegionOpShape {
        size_t _count {0};
        size_t _inElementBodies {0};
        size_t _inputs {0};
        size_t _unreadInputs {0};
    };

    void initialize() override {
        WriteQueryTest::initialize();

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    static bool isInElementBody(mlir::Operation* op) {
        for (mlir::Operation* parent = op->getParentOp(); parent; parent = parent->getParentOp()) {
            if (mlir::isa<mlir::db::ListComprehension, mlir::db::ListPredicate, mlir::db::Reduce, mlir::db::AllReduce>(parent)) {
                return true;
            }
        }

        return false;
    }

    static size_t countUnreadInputs(mlir::Operation* op) {
        mlir::Region& region = op->getRegion(0);

        mlir::db::RegionCarrySet carrySet;
        if (!mlir::db::matchRegionCarrySet(region, carrySet)) {
            return 0;
        }

        mlir::Block& body = region.front();

        size_t unread = 0;
        for (size_t carriedIndex = 0; carriedIndex < carrySet._count; carriedIndex++) {
            if (body.getArgument(static_cast<unsigned>(carrySet._argumentOffset + carriedIndex)).use_empty()) {
                unread++;
            }
        }

        return unread;
    }

    template <typename RegionOp>
    void describeOps(std::string_view query, RegionOpShape& shape, std::string& program) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();
        Graph* graph = system.getGraph(_graphName);

        const FrozenCommitTx transaction = graph->openTransaction();
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

        module.walk([&shape](RegionOp op) {
            shape._count++;
            shape._inputs += op.getInputColumns().size();
            shape._unreadInputs += countUnreadInputs(op.getOperation());

            if (isInElementBody(op.getOperation())) {
                shape._inElementBodies++;
            }
        });

        llvm::raw_string_ostream stream(program);
        module.print(stream);
    }

    template <typename RegionOp>
    void expectShape(std::string_view query, const RegionOpShape& expected) {
        RegionOpShape shape;
        std::string program;
        describeOps<RegionOp>(query, shape, program);

        EXPECT_EQ(shape._count, expected._count) << query << "\n" << program;
        EXPECT_EQ(shape._inElementBodies, expected._inElementBodies) << query << "\n" << program;
        EXPECT_EQ(shape._inputs, expected._inputs) << query << "\n" << program;
        EXPECT_EQ(shape._unreadInputs, expected._unreadInputs) << query << "\n" << program;
    }

    void trimProgram(const char* programText, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        module = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module);

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createTrimUnreadColumns());

        ASSERT_TRUE(mlir::succeeded(passManager.run(*module)));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));
    }

private:
    mlir::MLIRContext _context;
};

TEST_F(TrimUnreadSubqueryInputsTest, decidesAnExistsReadingNoElementOncePerRow) {
    expectShape<mlir::db::ExistsSubquery>("MATCH (n:Person) RETURN [x IN [1, 2] WHERE EXISTS { (n)-->() } | x]",
                                          {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 0});

    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] WHERE EXISTS { (n)-[:KNOWS_WELL]->() } | x]";
    expectShape<mlir::db::ExistsSubquery>(query, {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 0});

    expectRows(query, {{"Remy", "[1, 2]"}, {"Adam", "[1, 2]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                       {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

TEST_F(TrimUnreadSubqueryInputsTest, buildsAPatternComprehensionReadingNoElementOncePerRow) {
    expectShape<mlir::db::PatternComprehension>("MATCH (n:Person) RETURN [x IN [1, 2] | [(n)-->(m) | m.name]]",
                                                {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 0});

    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | [(n)-[:KNOWS_WELL]->(m) | m.name]]";
    expectShape<mlir::db::PatternComprehension>(query, {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 0});

    expectRows(query, {{"Remy", "[[Adam], [Adam]]"}, {"Adam", "[[Remy], [Remy]]"}, {"Maxime", "[[], []]"},
                       {"Luc", "[[], []]"}, {"Martina", "[[], []]"}, {"Suhas", "[[], []]"},
                       {"Cyrus", "[[], []]"}, {"Doruk", "[[], []]"}});
}

TEST_F(TrimUnreadSubqueryInputsTest, countsReadingNoElementOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | COUNT { (n)-->() }]";
    expectShape<mlir::db::CountSubquery>(query, {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 0});

    expectRows(query, {{"Remy", "[4, 4]"}, {"Adam", "[3, 3]"}, {"Maxime", "[2, 2]"}, {"Luc", "[2, 2]"},
                       {"Martina", "[1, 1]"}, {"Suhas", "[2, 2]"}, {"Cyrus", "[2, 2]"}, {"Doruk", "[1, 1]"}});
}

TEST_F(TrimUnreadSubqueryInputsTest, collectsReadingNoElementOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, "
                                   "[x IN [1, 2] | COLLECT { MATCH (n)-[:KNOWS_WELL]->(m) RETURN m.name }]";
    expectShape<mlir::db::CollectSubquery>(query, {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 0});

    expectRows(query, {{"Remy", "[[Adam], [Adam]]"}, {"Adam", "[[Remy], [Remy]]"}, {"Maxime", "[[], []]"},
                       {"Luc", "[[], []]"}, {"Martina", "[[], []]"}, {"Suhas", "[[], []]"},
                       {"Cyrus", "[[], []]"}, {"Doruk", "[[], []]"}});
}

TEST_F(TrimUnreadSubqueryInputsTest, dropsTheElementFromABodyRunOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | COUNT { MATCH (n)-->(m) RETURN DISTINCT m }]";
    expectShape<mlir::db::CountSubquery>(query, {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 0});

    expectRows(query, {{"Remy", "[4, 4]"}, {"Adam", "[3, 3]"}, {"Maxime", "[2, 2]"}, {"Luc", "[2, 2]"},
                       {"Martina", "[1, 1]"}, {"Suhas", "[2, 2]"}, {"Cyrus", "[2, 2]"}, {"Doruk", "[1, 1]"}});
}

// Remy and Adam each know a person aged 32, which is above 30 + 1 but not 30 + 2
TEST_F(TrimUnreadSubqueryInputsTest, keepsTheElementACorrelatedSubqueryReads) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] WHERE EXISTS { MATCH (n)-->(m) WHERE m.age > 30 + x } | x]";
    expectShape<mlir::db::ExistsSubquery>(query, {._count = 1, ._inElementBodies = 1, ._inputs = 2, ._unreadInputs = 0});

    expectRows(query, {{"Remy", "[1]"}, {"Adam", "[1]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                       {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

// 17 edges leave a person, and each row counts the 2 people aged 32
TEST_F(TrimUnreadSubqueryInputsTest, keepsAColumnSizingTheRowsOfABodyReadingNone) {
    const std::string_view counted = "MATCH (n:Person)-->(i) "
                                     "WITH n, i, COUNT { MATCH (m:Person) WHERE m.age = 32 } AS c "
                                     "RETURN count(*), sum(c)";
    expectShape<mlir::db::CountSubquery>(counted, {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 1});
    expectRows(counted, {{"17", "34"}});

    const std::string_view decided = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] WHERE EXISTS { MATCH (m:Person) WHERE m.age = 32 } | x]";
    expectShape<mlir::db::ExistsSubquery>(decided, {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 1});

    expectRows(decided, {{"Remy", "[1, 2]"}, {"Adam", "[1, 2]"}, {"Maxime", "[1, 2]"}, {"Luc", "[1, 2]"},
                         {"Martina", "[1, 2]"}, {"Suhas", "[1, 2]"}, {"Cyrus", "[1, 2]"}, {"Doruk", "[1, 2]"}});
}

// The pattern walks from n alone, and hands i back to its reader; the edge it only hands back
TEST_F(TrimUnreadSubqueryInputsTest, keepsTheInputsAnOptionalMatchHandsBack) {
    const std::string_view query = "MATCH (n:Person)-->(i:Interest) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(k) RETURN i.name, k.name";
    expectShape<mlir::db::OptionalMatch>(query, {._count = 1, ._inElementBodies = 0, ._inputs = 2, ._unreadInputs = 0});

    expectRows(query, {{"Ghosts", "Adam"}, {"Computers", "Adam"}, {"Eighties", "Adam"}, {"Bio", "Remy"},
                       {"Cooking", "Remy"}, {"Bio", "null"}, {"Padel", "null"}, {"Animals", "null"},
                       {"Computers", "null"}, {"Cooking", "null"}, {"Gym", "null"}, {"JiuJitsu", "null"},
                       {"Gym", "null"}, {"Travel", "null"}, {"Gym", "null"}});
}

// The body aggregates, so it runs once per row and the call pairs what it returns with the row
TEST_F(TrimUnreadSubqueryInputsTest, dropsAnInputOfACallNothingReads) {
    const std::string_view query = "MATCH (n:Person)-[:KNOWS_WELL]->(f) "
                                   "CALL { WITH n MATCH (n)-->(k) RETURN count(k) AS c } "
                                   "RETURN n.name, c";
    expectShape<mlir::db::CallSubquery>(query, {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 0});

    expectRows(query, {{"Remy", "4"}, {"Adam", "3"}});
}

TEST_F(TrimUnreadSubqueryInputsTest, keepsAnInputACallHandsBackToAReader) {
    const std::string_view query = "MATCH (n:Person)-[:KNOWS_WELL]->(f) "
                                   "CALL { WITH n MATCH (n)-->(k) RETURN count(k) AS c } "
                                   "RETURN n.name, f.name, c";
    expectShape<mlir::db::CallSubquery>(query, {._count = 1, ._inElementBodies = 0, ._inputs = 2, ._unreadInputs = 1});

    expectRows(query, {{"Remy", "Adam", "4"}, {"Adam", "Remy", "3"}});
}

// A body carrying its scope yields every input back, as its hops left it; the edge to f it
// only yields back
TEST_F(TrimUnreadSubqueryInputsTest, keepsTheInputsACallCarryingItsScopeYieldsBack) {
    const std::string_view query = "MATCH (n:Person)-[:KNOWS_WELL]->(f) "
                                   "CALL { WITH n MATCH (n)-->(k:Interest) RETURN k } "
                                   "RETURN f.name, k.name";
    expectShape<mlir::db::CallSubquery>(query, {._count = 1, ._inElementBodies = 0, ._inputs = 2, ._unreadInputs = 0});

    expectRows(query, {{"Adam", "Ghosts"}, {"Adam", "Computers"}, {"Adam", "Eighties"}, {"Remy", "Bio"},
                       {"Remy", "Cooking"}});
}

TEST_F(TrimUnreadSubqueryInputsTest, dropsAnInputOfAUnitCallItsBodyDoesNotRead) {
    const std::string_view query = "MATCH (n:Person)-[:KNOWS_WELL]->(f) CALL { WITH n CREATE (:Visit {name: n.name}) }";
    expectShape<mlir::db::CallSubquery>(query, {._count = 1, ._inElementBodies = 0, ._inputs = 1, ._unreadInputs = 0});

    applyWrite(query);
    expectRows("MATCH (v:Visit) RETURN v.name", {{"Remy"}, {"Adam"}});
}

// An EXISTS whose body reads k, a constant a fold left, and n.age, but not n
const char* const existsReadingAConstantInput = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %k = db.constant(1 : i64)
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
  %b = db.exists_subquery(%n, %k, %age) carries_scope {
  ^bb0(%nIn: !db.column<!storage.node_id>, %kIn: !db.column<i64>, %ageIn: !db.column<!storage.nullable<i64>>, %tag: !db.column<ui64>):
    %gt = db.gt %ageIn, %kIn : (!db.column<!storage.nullable<i64>>, !db.column<i64>) -> !db.column<!storage.bool>
    %kept = db.filter(%gt, {%tag}) : (!db.column<!storage.bool>, !db.column<ui64>) -> !db.column<ui64>
    db.exists_yield %kept, {}
  } : (!db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.nullable<i64>>) -> !db.column<!storage.bool>
  db.output(%b) : !db.column<!storage.bool>
  return
}
)mlir";

// n.age holds the rows of n, so the unread n goes, though the constant k then leads the inputs
TEST_F(TrimUnreadSubqueryInputsTest, dropsAnUnreadInputThoughAConstantThenLeads) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    trimProgram(existsReadingAConstantInput, module);
    ASSERT_FALSE(HasFatalFailure());

    mlir::db::ExistsSubquery exists;
    module->walk([&exists](mlir::db::ExistsSubquery op) {
        exists = op;
    });
    ASSERT_TRUE(exists);

    const mlir::OperandRange inputs = exists.getInputColumns();
    ASSERT_EQ(inputs.size(), 2u);
    EXPECT_TRUE(mlir::isa<mlir::db::ConstantOp>(inputs[0].getDefiningOp()));
    EXPECT_TRUE(mlir::isa<mlir::db::GetNodeProperties>(inputs[1].getDefiningOp()));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
