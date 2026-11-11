#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBPasses.h"
#include "HashJoinQueryTest.h"
#include "StringRowSink.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// A key read over constants holds one value in every row, so the plan holds that value as a
// db.constant of the read's own column of tagged cells. The read of a list element is kept, and
// so is a read that raises on its constants.
class MaterializeFoldedConstantsTest : public HashJoinQueryTest {
protected:
    void expectPlansTheConstant(std::string_view query, std::string_view read, std::string_view constant) {
        std::string program;
        dbProgram(query, program);

        EXPECT_FALSE(contains(program, read)) << program;
        EXPECT_TRUE(contains(program, constant)) << program;
    }

    void expectKeepsTheRead(std::string_view query, std::string_view read) {
        std::string program;
        dbProgram(query, program);

        EXPECT_TRUE(contains(program, read)) << program;
    }

    void expectSortedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    void expectRaises(std::string_view query) {
        StringRowSink sink;
        EXPECT_THROW(runQuery(query, sink), TuringException) << "query: " << query;
    }

    void expectEveryRowHolds(std::string_view query, size_t rowCount, std::string_view value) {
        StringRowSink sink;
        runQuery(query, sink);

        const Rows& rows = sink.getRows();
        ASSERT_EQ(rows.size(), rowCount) << "query: " << query;

        size_t holding = 0;
        for (const StringRowSink::Row& row : rows) {
            if (row.size() == 2 && row[1] == value) {
                holding++;
            }
        }

        EXPECT_EQ(holding, rowCount) << "query: " << query;
    }

    void parse(std::string_view ops, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        std::string program = R"mlir(
!strings = !db.column<!storage.string>
!integers = !db.column<i64>
!null = !db.column<!storage.nullable<none>>
!maps = !db.column<!storage.map>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>

func.func @main() {
)mlir";

        program += ops;
        program += "\n  return\n}\n";

        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
    }

    bool verifies(std::string_view ops) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        parse(ops, module);

        return static_cast<bool>(module);
    }

    void foldMapKeys(std::string_view ops, std::string& folded) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        parse(ops, module);
        ASSERT_TRUE(module) << ops;

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFoldConstantMapKeys());

        ASSERT_TRUE(mlir::succeeded(passManager.run(*module))) << ops;
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module))) << ops;

        render(*module, folded);
    }
};

TEST_F(MaterializeFoldedConstantsTest, plansAKeyReadOffAConstantMapAsACell) {
    const std::string_view query = "RETURN {k: 't'}.k AS v";

    expectPlansTheConstant(query, "db.static_map_key", R"(db.constant("t" : !storage.string) : !db.column<!storage.any>)");
    expectSortedRows(query, {{"t"}});
}

TEST_F(MaterializeFoldedConstantsTest, plansAMissingKeyAsANullCell) {
    const std::string_view query = "RETURN {k: 't'}.j AS v";

    expectPlansTheConstant(query, "db.static_map_key", "db.constant(unit) : !db.column<!storage.any>");
    expectSortedRows(query, {{"null"}});
}

TEST_F(MaterializeFoldedConstantsTest, plansAKeyReadOffAMapHeldByAConstantMap) {
    const std::string_view query = "RETURN {a: {b: 1}}.a.b AS v";

    expectPlansTheConstant(query, "db.static_map_key", "db.constant(1 : i64) : !db.column<!storage.any>");
    expectSortedRows(query, {{"1"}});
}

TEST_F(MaterializeFoldedConstantsTest, plansAListOrAMapHeldByAConstantMapAsACell) {
    expectPlansTheConstant("RETURN {a: [1, 2]}.a AS v", "db.static_map_key", "db.constant([1, 2]) : !db.column<!storage.any>");
    expectSortedRows("RETURN {a: [1, 2]}.a AS v", {{"[1, 2]"}});

    expectPlansTheConstant("RETURN {a: {b: 1}}.a AS v", "db.static_map_key", "db.constant({b = 1 : i64}) : !db.column<!storage.any>");
    expectSortedRows("RETURN {a: {b: 1}}.a AS v", {{"{b: 1}"}});
}

TEST_F(MaterializeFoldedConstantsTest, plansAReadInsideAListComprehensionAsAConstant) {
    const std::string_view query = "RETURN [x IN [1, 2, 3] | {k: 10}.k + x] AS l";

    expectPlansTheConstant(query, "db.static_map_key", "db.constant(10 : i64) : !db.column<!storage.any>");
    expectSortedRows(query, {{"11, 12, 13"}});
}

TEST_F(MaterializeFoldedConstantsTest, plansAListOrAMapMadeOfFoldedReadsAsAConstant) {
    expectPlansTheConstant("RETURN [{k: 't'}.k, {k: 1}.k] AS l", "db.make_list", R"(db.constant(["t" : !storage.string, 1]))");
    expectSortedRows("RETURN [{k: 't'}.k, {k: 1}.k] AS l", {{"t, 1"}});

    expectPlansTheConstant("RETURN {a: {k: 't'}.k} AS m", "db.make_map", R"(db.constant({a = "t" : !storage.string}))");
    expectSortedRows("RETURN {a: {k: 't'}.k} AS m", {{"{a: t}"}});
}

// Lowering types [1, 2][0] as an integer and [1, 'a'][0] as a tagged cell, from the list
TEST_F(MaterializeFoldedConstantsTest, keepsTheReadOfAListElement) {
    expectKeepsTheRead("RETURN [1, 2][5] AS v", "db.list_index");
    expectSortedRows("RETURN [1, 2][5] AS v", {{"null"}});

    expectKeepsTheRead("RETURN head(['a']) AS v", "db.head");
    expectSortedRows("RETURN head(['a']) AS v", {{"a"}});

    expectKeepsTheRead("RETURN last([1, 'a']) AS v", "db.last");
    expectSortedRows("RETURN last([1, 'a']) AS v", {{"a"}});
}

TEST_F(MaterializeFoldedConstantsTest, readsAFoldedConstantAsTheReadDid) {
    expectSortedRows("RETURN {k: 1}.k + 1 AS v", {{"2"}});
    expectSortedRows("RETURN {k: 't'}.j + 1 AS v", {{"null"}});
    expectSortedRows("RETURN coalesce({k: 't'}.j, 'd') AS v", {{"d"}});
    expectSortedRows("RETURN CASE WHEN {k: 't'}.j IS NULL THEN 'y' ELSE 'n' END AS v", {{"y"}});
    expectSortedRows("RETURN CASE WHEN {k: 1}.k = 1 THEN 'y' ELSE 'n' END AS v", {{"y"}});
    expectSortedRows("RETURN CASE WHEN {k: 't'}.j = 1 THEN 'y' ELSE 'n' END AS v", {{"n"}});
    expectSortedRows("RETURN [{k: 't'}.k, [1, 2][0]] AS l", {{"t, 1"}});
    expectSortedRows("UNWIND [1, 2] AS x RETURN sum({k: 't'}.j) AS v", {{"0"}});
    expectSortedRows("UNWIND [1, 2] AS x RETURN sum({k: 3}.k) AS v", {{"6"}});
    expectSortedRows("MATCH (n:Person) WHERE n.name = 'Remy' OR n.name = 'Adam' RETURN n.name, {k: 't'}.k",
                     {{"Adam", "t"}, {"Remy", "t"}});
    expectSortedRows("WITH {k: 't'}.k AS v MATCH (n:Person {name: 'Remy'}) RETURN v, n.name", {{"t", "Remy"}});
    expectSortedRows("UNWIND {k: [1, 2]}.k AS x RETURN x", {{"1"}, {"2"}});
    expectSortedRows("RETURN [x IN {k: [1, 2]}.k | x * 2] AS l", {{"2, 4"}});
    expectSortedRows("RETURN size({k: 'abc'}.k) AS v", {{"3"}});
    expectSortedRows("UNWIND [1, 2] AS x RETURN {k: 't'}.k AS v, count(*) AS c", {{"t", "2"}});
}

TEST_F(MaterializeFoldedConstantsTest, keepsAKeyReadOffAnIntegerToRaise) {
    const std::string_view query = "RETURN {k: 5}.k.j AS v";

    expectKeepsTheRead(query, "db.static_map_key");
    expectRaises(query);
}

TEST_F(MaterializeFoldedConstantsTest, keepsAListReadByAStringToRaise) {
    const std::string_view query = "RETURN [1, 2][{k: 'a'}.k] AS v";

    expectKeepsTheRead(query, "db.container_index");
    expectRaises(query);
}

TEST_F(MaterializeFoldedConstantsTest, laysAConstantCellOutOverRowsAcrossChunks) {
    expectEveryRowHolds("UNWIND range(1, 140000) AS x RETURN x, {k: 't'}.k AS v", 140000, "t");
    expectEveryRowHolds("UNWIND range(1, 140000) AS x RETURN x, {k: 't'}.j AS v", 140000, "null");
    expectEveryRowHolds("UNWIND range(1, 140000) AS x RETURN x, {k: [1, 2]}.k AS v", 140000, "[1, 2]");
    expectSortedRows("UNWIND range(1, 140000) AS x WITH x, {k: 't'}.k AS v WHERE v = 't' RETURN count(*)", {{"140000"}});
}

TEST_F(MaterializeFoldedConstantsTest, joinsOnAKeyNamedByAFoldedRead) {
    const std::string_view query = "UNWIND [{name: 'Remy'}, {name: 'Luc'}] AS row "
                                   "MATCH (a:Person) WHERE a.name = row[{k: 'name'}.k] RETURN a.name";

    std::string program;
    dbProgram(query, program);
    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;

    expectSortedRows(query, {{"Luc"}, {"Remy"}});
}

TEST_F(MaterializeFoldedConstantsTest, scansByAPropertyValueAFoldedReadHolds) {
    const std::string_view query = "MATCH (a:Person) WHERE a.name = {k: 'Remy'}.k RETURN a.name";

    expectPlansTheConstant(query, "db.static_map_key", "db.scan_nodes_by_property_value");
    expectSortedRows(query, {{"Remy"}});
}

TEST_F(MaterializeFoldedConstantsTest, scansByANodeIDAFoldedReadHolds) {
    const std::string_view query = "MATCH (n) WHERE id(n) = {k: 1}.k RETURN n.name";

    expectPlansTheConstant(query, "db.static_map_key", "db.const_scan_nodes([1])");
    expectSortedRows(query, {{"Adam"}});
}

TEST_F(MaterializeFoldedConstantsTest, lowersACellConstantAsOneConstantCell) {
    std::string program;

    nlProgram("RETURN {k: 't'}.k AS v", program);
    EXPECT_TRUE(contains(program, R"(nl.constant("t" : !storage.string) : !nl.chunk<!storage.any>)")) << program;

    nlProgram("RETURN {k: 't'}.j AS v", program);
    EXPECT_TRUE(contains(program, "nl.constant(unit) : !nl.chunk<!storage.any>")) << program;
}

TEST_F(MaterializeFoldedConstantsTest, foldsAReadOverConstantsAndErasesWhatItRead) {
    std::string folded;
    ASSERT_NO_FATAL_FAILURE(foldMapKeys(R"mlir(
  %m = db.constant({k = "t" : !storage.string})
  %r = db.static_map_key %m key "k" : (!maps) -> !cells
  db.output(%r) : !cells
)mlir",
                                        folded));

    EXPECT_FALSE(contains(folded, "db.static_map_key")) << folded;
    EXPECT_FALSE(contains(folded, "db.constant({k")) << folded;
    EXPECT_TRUE(contains(folded, R"(db.constant("t" : !storage.string) : !db.column<!storage.any>)")) << folded;
}

TEST_F(MaterializeFoldedConstantsTest, keepsAReadWhoseTypeLoweringResolves) {
    std::string folded;
    ASSERT_NO_FATAL_FAILURE(foldMapKeys(R"mlir(
  %l = db.constant([1, 2])
  %at = db.constant(1 : i64)
  %r = db.list_index %l, %at : (!db.column<!storage.list<i64>>, !integers) -> !unresolved
  db.output(%r) : !unresolved
)mlir",
                                        folded));

    EXPECT_TRUE(contains(folded, "db.list_index")) << folded;
}

TEST_F(MaterializeFoldedConstantsTest, keepsAListWhoseLiteralHoldsAnotherElementType) {
    std::string folded;
    ASSERT_NO_FATAL_FAILURE(foldMapKeys(R"mlir(
  %a = db.constant("t" : !storage.string)
  %n = db.constant("" : !storage.nullable<none>)
  %r = db.make_list(%a, %n) : (!strings, !null) -> !db.column<!storage.list<!storage.any>>
  db.output(%r) : !db.column<!storage.list<!storage.any>>
)mlir",
                                        folded));

    EXPECT_TRUE(contains(folded, "db.make_list")) << folded;
}

TEST_F(MaterializeFoldedConstantsTest, printsTheResultTypeOnlyWhereTheLiteralDoesNotName) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    parse(R"mlir(
  %a = db.constant(1 : i64)
  %b = db.constant("t" : !storage.string) : !cells
  %c = db.constant(unit) : !cells
  %d = db.constant([1, 2]) : !db.column<!storage.list<i64>>
  db.output(%a, %b, %c, %d) : !integers, !cells, !cells, !db.column<!storage.list<i64>>
)mlir",
          module);
    ASSERT_TRUE(module);

    std::string printed;
    render(*module, printed);

    EXPECT_TRUE(contains(printed, "= db.constant(1 : i64)\n")) << printed;
    EXPECT_TRUE(contains(printed, R"(= db.constant("t" : !storage.string) : !db.column<!storage.any>)")) << printed;
    EXPECT_TRUE(contains(printed, "= db.constant(unit) : !db.column<!storage.any>\n")) << printed;
    EXPECT_TRUE(contains(printed, "= db.constant([1, 2])\n")) << printed;
}

TEST_F(MaterializeFoldedConstantsTest, acceptsACellColumnOfAnyLiteral) {
    EXPECT_TRUE(verifies("  %c = db.constant(1 : i64) : !cells"));
    EXPECT_TRUE(verifies("  %c = db.constant(1.5 : f64) : !cells"));
    EXPECT_TRUE(verifies("  %c = db.constant(true) : !cells"));
    EXPECT_TRUE(verifies("  %c = db.constant({k = unit}) : !cells"));
    EXPECT_TRUE(verifies("  %c = db.constant([unit, 1]) : !cells"));
    EXPECT_TRUE(verifies("  %c = db.constant(unit) : !cells"));
}

TEST_F(MaterializeFoldedConstantsTest, rejectsAColumnOfAnotherType) {
    EXPECT_FALSE(verifies("  %c = db.constant(1 : i64) : !strings"));
    EXPECT_FALSE(verifies("  %c = db.constant([1, 2]) : !db.column<!storage.list<!storage.string>>"));
    EXPECT_FALSE(verifies("  %c = db.constant(1 : i64) : !unresolved"));
    EXPECT_FALSE(verifies("  %c = db.constant(unit) : !unresolved"));
    EXPECT_FALSE(verifies("  %c = db.constant(unit) : !integers"));
    EXPECT_FALSE(verifies("  %c = db.constant(\"\" : !storage.nullable<none>) : !cells"));
    EXPECT_FALSE(verifies("  %c = db.constant(unit)"));
}
