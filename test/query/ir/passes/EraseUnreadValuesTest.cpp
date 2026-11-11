#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"
#include "llvm/Support/raw_ostream.h"

#include "Graph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "DBPasses.h"
#include "HashJoinQueryTest.h"
#include "StringRowSink.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// No constant or value computed per row is left in the db program, or in the nl program it
// lowers to, that nothing reads and that cannot raise. The queries are the shapes codegen and
// the passes left one in, planned with the join forced and as the cost model plans them.
class EraseUnreadValuesTest : public HashJoinQueryTest {
protected:
    void expectNoUnreadOp(std::string_view query) {
        expectNoUnreadOp(query, true);
        expectNoUnreadOp(query, false);
    }

    void expectNoUnreadOp(std::string_view query, bool forcesJoin) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        generate(query, view, module.get(), forcesJoin);

        std::string unread;
        std::string program;
        collectUnreadOps(module.get(), unread);
        render(module.get(), program);

        EXPECT_TRUE(unread.empty()) << "query: " << query << "\nunread:\n" << unread << program;

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        lower(view, module.get(), nlModule.get());

        collectUnreadOps(nlModule.get(), unread);
        render(nlModule.get(), program);

        EXPECT_TRUE(unread.empty()) << "query: " << query << "\nunread:\n" << unread << program;
    }

    void expectSortedRows(std::string_view query, const Rows& expected) {
        expectSortedRows(query, expected, true);
        expectSortedRows(query, expected, false);
    }

    void expectSortedRows(std::string_view query, const Rows& expected, bool forcesJoin) {
        StringRowSink sink;
        runQuery(query, sink, forcesJoin);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    void expectRaises(std::string_view query) {
        StringRowSink sink;
        EXPECT_THROW(runQuery(query, sink), TuringException) << "query: " << query;
    }

    void eraseUnreadValues(std::string_view ops, std::string& erased) {
        std::string program = R"mlir(
!nodes = !db.column<!storage.node_id>
!strings = !db.column<!storage.string>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>
!tags = !db.column<ui64>

func.func @main() {
)mlir";

        program += ops;
        program += "\n  return\n}\n";

        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module) << ops;

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createEraseUnreadValues());

        ASSERT_TRUE(mlir::succeeded(passManager.run(*module))) << ops;
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module))) << ops;

        render(*module, erased);
    }

    static void collectUnreadOps(mlir::ModuleOp module, std::string& unread) {
        unread.clear();
        llvm::raw_string_ostream stream(unread);

        mlir::OpPrintingFlags flags;
        flags.assumeVerified();
        flags.skipRegions();

        module->walk([&stream, &flags](mlir::Operation* op) {
            if (mlir::isOpTriviallyDead(op)) {
                op->print(stream, flags);
                stream << "\n";
            }
        });
    }
};

TEST_F(EraseUnreadValuesTest, erasesTheNullCodegenLeavesBesideAFilterOverAConstant) {
    const std::string_view query = "WITH 't' AS k WHERE k IS NOT NULL UNWIND [{s: {t: 1}}] AS row RETURN row.s.t";

    expectNoUnreadOp(query);
    expectSortedRows(query, {{"1"}});
}

TEST_F(EraseUnreadValuesTest, erasesTheConstantADroppedBroadcastRead) {
    const std::string_view query = "UNWIND ['Remy'] AS k MATCH (a:Person) WHERE a.name = k WITH a, k "
                                   "UNWIND [{Remy: 1}] AS row RETURN a.name, row[k]";

    expectNoUnreadOp(query);
    expectSortedRows(query, {{"Remy", "1"}});
}

TEST_F(EraseUnreadValuesTest, erasesTheConstantAnUnreadListHeld) {
    const std::string_view query = "WITH 't' AS x WITH [x] AS l UNWIND [{t: 1}, {t: 2}] AS row RETURN row.t AS v";

    expectNoUnreadOp(query);
    expectSortedRows(query, {{"1"}, {"2"}});
}

TEST_F(EraseUnreadValuesTest, erasesAConstantAWithProjectsAndNothingReads) {
    const std::string_view query = "MATCH (n:Person) WITH n, 'age' AS k WHERE n.age = 32 RETURN n.name";

    expectNoUnreadOp(query);
    expectSortedRows(query, {{"Adam"}, {"Remy"}});
}

TEST_F(EraseUnreadValuesTest, erasesTheConstantAnUnreadChainOfValuesRead) {
    const std::string_view query = "MATCH (n:Person) WITH n, toUpper(n.name) + 'x' AS k RETURN n.name";

    expectNoUnreadOp(query);
    expectSortedRows(query, {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});
}

TEST_F(EraseUnreadValuesTest, keepsAnUnreadValueThatRaises) {
    expectRaises("WITH {k: 5}.k.j AS v RETURN 1 AS x");
    expectRaises("MATCH (n:Person) WITH n, n.name.j AS v RETURN n.name");
    expectRaises("MATCH (n:Person) WITH n, 1 / 0 AS v RETURN n.name");
    expectRaises("MATCH (n:Person) WITH n, [x IN [0] | 1 / x] AS v RETURN n.name");
}

// Nothing reads the scan once the unwind carries no column, but lowering lays the list out
// over the scan's rows: 2 elements for each of the 8 people
TEST_F(EraseUnreadValuesTest, keepsAnUnreadScanAConstantIsLaidOutOver) {
    const StringRowSink::Row one {"1"};
    const StringRowSink::Row two {"2"};

    Rows expected(8, one);
    expected.insert(expected.end(), 8, two);

    expectSortedRows("MATCH (n:Person) UNWIND {k: [1, 2]}.k AS x RETURN x", expected);
}

TEST_F(EraseUnreadValuesTest, erasesAnUnreadValueAndTheValuesOnlyItRead) {
    std::string erased;
    ASSERT_NO_FATAL_FAILURE(eraseUnreadValues(R"mlir(
  %n = db.scan_nodes() : !nodes
  %t = db.constant("t" : !storage.string)
  %upper = db.to_upper(%t) : (!strings) -> !strings
  %name = db.get_node_properties(%n, "name") : (!nodes) -> !db.column<!storage.nullable<!storage.string>>
  db.output(%n) : !nodes
)mlir",
                                              erased));

    EXPECT_FALSE(contains(erased, "db.to_upper")) << erased;
    EXPECT_FALSE(contains(erased, "db.constant")) << erased;
    EXPECT_FALSE(contains(erased, "db.get_node_properties")) << erased;
    EXPECT_TRUE(contains(erased, "db.scan_nodes")) << erased;
}

TEST_F(EraseUnreadValuesTest, erasesAnUnreadBodyAndWhatItReadFromAbove) {
    std::string erased;
    ASSERT_NO_FATAL_FAILURE(eraseUnreadValues(R"mlir(
  %n = db.scan_nodes() : !nodes
  %xs = db.constant([1, 2])
  %t = db.constant("t" : !storage.string)
  %r = db.list_comprehension(%xs, {%n}) {
  ^bb0(%x: !unresolved, %tag: !tags, %nIn: !nodes):
    db.comprehension_yield %tag, %t : !strings
  } : (!db.column<!storage.list<i64>>, !nodes) -> !unresolved
  db.output(%n) : !nodes
)mlir",
                                              erased));

    EXPECT_FALSE(contains(erased, "db.list_comprehension")) << erased;
    EXPECT_FALSE(contains(erased, "db.constant")) << erased;
}

TEST_F(EraseUnreadValuesTest, keepsAnUnreadReadThatMayRaise) {
    std::string erased;
    ASSERT_NO_FATAL_FAILURE(eraseUnreadValues(R"mlir(
  %n = db.scan_nodes() : !nodes
  %five = db.constant(5 : i64) : !cells
  %read = db.static_map_key %five key "j" : (!cells) -> !cells
  db.output(%n) : !nodes
)mlir",
                                              erased));

    EXPECT_TRUE(contains(erased, "db.static_map_key")) << erased;
    EXPECT_TRUE(contains(erased, "db.constant(5 : i64)")) << erased;
}

TEST_F(EraseUnreadValuesTest, keepsAnUnreadBodyThatMayRaise) {
    std::string erased;
    ASSERT_NO_FATAL_FAILURE(eraseUnreadValues(R"mlir(
  %n = db.scan_nodes() : !nodes
  %xs = db.constant([0])
  %r = db.list_comprehension(%xs, {%n}) {
  ^bb0(%x: !unresolved, %tag: !tags, %nIn: !nodes):
    %one = db.constant(1 : i64)
    %quotient = db.div %one, %x : (!db.column<i64>, !unresolved) -> !unresolved
    db.comprehension_yield %tag, %quotient : !unresolved
  } : (!db.column<!storage.list<i64>>, !nodes) -> !unresolved
  db.output(%n) : !nodes
)mlir",
                                              erased));

    EXPECT_TRUE(contains(erased, "db.list_comprehension")) << erased;
    EXPECT_TRUE(contains(erased, "db.div")) << erased;
}

TEST_F(EraseUnreadValuesTest, keepsAnUnreadOpThatShapesTheRows) {
    std::string erased;
    ASSERT_NO_FATAL_FAILURE(eraseUnreadValues(R"mlir(
  %n = db.scan_nodes() : !nodes
  %xs = db.constant([1, 2]) : !cells
  %element = db.unwind(%xs, {}) : (!cells) -> !cells
  db.output(%element) : !cells
)mlir",
                                              erased));

    EXPECT_TRUE(contains(erased, "db.scan_nodes")) << erased;
}
