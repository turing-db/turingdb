#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "HashJoinQueryTest.h"
#include "StorageDialect.h"
#include "StringRowSink.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// m['k'] on a type-erased cell reads what m.k reads where every row of m holds a map, a node,
// an edge or null, so it plans as m.k does. Over any other value the two differ: m.year reads
// a component of a datetime that m['year'] raises on.
class FoldConstantMapKeysTest : public HashJoinQueryTest {
protected:
    void expectPlansAsTheDottedForm(std::string_view bracketQuery, std::string_view dottedQuery) {
        std::string bracketProgram;
        dbProgram(bracketQuery, bracketProgram);

        std::string dottedProgram;
        dbProgram(dottedQuery, dottedProgram);

        EXPECT_FALSE(contains(bracketProgram, "db.dynamic_map_key")) << bracketProgram;
        EXPECT_EQ(bracketProgram, dottedProgram);
    }

    void expectKeepsTheBracketRead(std::string_view query) {
        std::string program;
        dbProgram(query, program);

        EXPECT_TRUE(contains(program, "db.dynamic_map_key")) << program;
    }

    void expectSortedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());

        EXPECT_EQ(rows, expected) << "query: " << query;
    }
};

TEST_F(FoldConstantMapKeysTest, joinsAValueOfAMapReadByABracketKey) {
    const std::string_view bracketQuery = "UNWIND [{s: {t: {name: 'Remy'}}}, {s: {t: {name: 'Luc'}}}] AS row "
                                          "MATCH (a:Person {name: row.s['t'].name}) RETURN a.name";
    const std::string_view dottedQuery = "UNWIND [{s: {t: {name: 'Remy'}}}, {s: {t: {name: 'Luc'}}}] AS row "
                                         "MATCH (a:Person {name: row.s.t.name}) RETURN a.name";

    std::string program;
    dbProgram(bracketQuery, program);
    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;

    expectPlansAsTheDottedForm(bracketQuery, dottedQuery);
    expectSortedRows(bracketQuery, {{"Luc"}, {"Remy"}});
}

TEST_F(FoldConstantMapKeysTest, readsAKeyOfAMapHeldByAMap) {
    const std::string_view bracketQuery = "UNWIND [{s: {t: 1}}, {s: null}, null] AS row RETURN row.s['t'] AS v";
    const std::string_view dottedQuery = "UNWIND [{s: {t: 1}}, {s: null}, null] AS row RETURN row.s.t AS v";

    expectPlansAsTheDottedForm(bracketQuery, dottedQuery);
    expectSortedRows(bracketQuery, {{"1"}, {"null"}, {"null"}});
}

TEST_F(FoldConstantMapKeysTest, readsAPropertyOfANodeHeldByAMap) {
    const std::string_view bracketQuery = "MATCH (n:Person {name: 'Remy'}) WITH {p: n} AS m "
                                          "UNWIND [m] AS row RETURN row.p['name'] AS v";
    const std::string_view dottedQuery = "MATCH (n:Person {name: 'Remy'}) WITH {p: n} AS m "
                                         "UNWIND [m] AS row RETURN row.p.name AS v";

    expectPlansAsTheDottedForm(bracketQuery, dottedQuery);
    expectSortedRows(bracketQuery, {{"Remy"}});
}

TEST_F(FoldConstantMapKeysTest, readsAKeyOfAReadCarriedPastASort) {
    const std::string_view bracketQuery = "UNWIND [{s: {t: 2}}, {s: {t: 1}}] AS row "
                                          "WITH row['s'] AS s ORDER BY s['t'] RETURN s['t'] AS v";
    const std::string_view dottedQuery = "UNWIND [{s: {t: 2}}, {s: {t: 1}}] AS row "
                                         "WITH row.s AS s ORDER BY s.t RETURN s.t AS v";

    expectPlansAsTheDottedForm(bracketQuery, dottedQuery);
    expectRows(bracketQuery, {{"1"}, {"2"}});
}

// WITH ... WHERE over constants alone lays the key out over the one row it keeps, and the
// product carries it to the read
TEST_F(FoldConstantMapKeysTest, joinsOnAKeyCarriedFromAWhereOverConstants) {
    const std::string_view query = "WITH 't' AS k WHERE k IS NOT NULL "
                                   "UNWIND [{s: {t: {name: 'Remy'}}}, {s: {t: {name: 'Luc'}}}] AS row "
                                   "MATCH (a:Person {name: row.s[k].name}) RETURN a.name";

    std::string program;
    dbProgram(query, program);
    EXPECT_FALSE(contains(program, "db.dynamic_map_key")) << program;
    EXPECT_TRUE(contains(program, "db.hash_join")) << program;

    expectSortedRows(query, {{"Luc"}, {"Remy"}});
}

// The equality fuses the one-element unwind away and lays its element out as the key
TEST_F(FoldConstantMapKeysTest, readsAKeyLaidOutByAnUnwindEquality) {
    const std::string_view query = "UNWIND ['Remy'] AS k MATCH (a:Person) WHERE a.name = k "
                                   "WITH a, k UNWIND [{Remy: 1}] AS row RETURN a.name, row[k]";

    std::string program;
    dbProgram(query, program);
    EXPECT_FALSE(contains(program, "db.dynamic_map_key")) << program;

    expectSortedRows(query, {{"Remy", "1"}});
}

TEST_F(FoldConstantMapKeysTest, keepsAKeyEachRowComputes) {
    const std::string_view query = "UNWIND ['t', 'u'] AS k UNWIND [{t: 1, u: 2}] AS row RETURN row[k]";

    expectKeepsTheBracketRead(query);
    expectSortedRows(query, {{"1"}, {"2"}});
}

// A CASE with no ELSE gives the rows no branch takes null, so they read no key
TEST_F(FoldConstantMapKeysTest, keepsAKeyOnlySomeRowsHold) {
    const std::string_view query = "UNWIND [1, 2] AS x WITH x, CASE WHEN x = 1 THEN 't' END AS k "
                                   "UNWIND [{t: 5}] AS row RETURN x, row[k]";

    expectKeepsTheBracketRead(query);
    expectSortedRows(query, {{"1", "5"}, {"2", "null"}});
}

TEST_F(FoldConstantMapKeysTest, raisesOnADatetimeWhoseComponentTheDottedFormReads) {
    const std::string_view query = "UNWIND [{year: 1}, datetime('2020-01-01T00:00:00Z')] AS row RETURN row['year']";

    expectKeepsTheBracketRead(query);

    StringRowSink sink;
    EXPECT_THROW(runQuery(query, sink), TuringException);
}

TEST_F(FoldConstantMapKeysTest, raisesOnAValueWithNoKeys) {
    const std::string_view query = "UNWIND [{t: 1}, 5] AS row RETURN row['t']";

    expectKeepsTheBracketRead(query);

    StringRowSink sink;
    EXPECT_THROW(runQuery(query, sink), TuringException);
}

// The pipeline runs the pass twice, which would hide a read left for a second round
class FoldConstantMapKeysPassTest : public ::testing::Test {
protected:
    FoldConstantMapKeysPassTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    void fold(std::string_view ops, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        std::string program = R"mlir(
!strings = !db.column<!storage.string>
!cells = !db.column<!storage.any>
!null = !db.column<!storage.nullable<none>>

func.func @main() {
  %rows = db.unwind_const([{s = {t = 2 : i64}}, {s = {t = 1 : i64}}]) : !cells
)mlir";

        program += ops;
        program += "\n  return\n}\n";

        module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module) << program;

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFoldConstantMapKeys());

        ASSERT_TRUE(mlir::succeeded(passManager.run(*module))) << program;
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module))) << program;
    }

    template <typename OpType>
    static size_t countOps(mlir::ModuleOp module) {
        size_t count = 0;
        module->walk([&count](OpType) {
            count++;
        });

        return count;
    }

    mlir::MLIRContext _context;
};

TEST_F(FoldConstantMapKeysPassTest, foldsAReadOfAReadCarriedPastASort) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    ASSERT_NO_FATAL_FAILURE(fold(R"mlir(
  %sKey = db.constant("s" : !storage.string)
  %s = db.dynamic_map_key %rows, %sKey : (!cells, !strings) -> !cells
  %tKey = db.constant("t" : !storage.string)
  %t = db.dynamic_map_key %s, %tKey : (!cells, !strings) -> !cells
  %sorted:2 = db.sort(%s, %t) keys [1] ascending [true] : (!cells, !cells) -> (!cells, !cells)
  %sortedT = db.dynamic_map_key %sorted#0, %tKey : (!cells, !strings) -> !cells
  db.output(%sortedT) : !cells
)mlir",
                                 module));

    EXPECT_EQ(countOps<mlir::db::DynamicMapKey>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::StaticMapKey>(*module), 3u);
    EXPECT_EQ(countOps<mlir::db::ConstantOp>(*module), 0u);
}

TEST_F(FoldConstantMapKeysPassTest, foldsAKeyLaidOutOverTheRows) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    ASSERT_NO_FATAL_FAILURE(fold(R"mlir(
  %key = db.constant("s" : !storage.string)
  %laid = db.broadcast_constant(%key, %rows) : (!strings, !cells) -> !strings
  %s = db.dynamic_map_key %rows, %laid : (!cells, !strings) -> !cells
  db.output(%s) : !cells
)mlir",
                                 module));

    EXPECT_EQ(countOps<mlir::db::DynamicMapKey>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::StaticMapKey>(*module), 1u);
}

TEST_F(FoldConstantMapKeysPassTest, keepsANullKey) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    ASSERT_NO_FATAL_FAILURE(fold(R"mlir(
  %key = db.constant("" : !storage.nullable<none>)
  %s = db.dynamic_map_key %rows, %key : (!cells, !null) -> !cells
  db.output(%s) : !cells
)mlir",
                                 module));

    EXPECT_EQ(countOps<mlir::db::DynamicMapKey>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::StaticMapKey>(*module), 0u);
}
