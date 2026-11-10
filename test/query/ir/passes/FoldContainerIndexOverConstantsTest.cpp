#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "DBTypes.h"
#include "HashJoinQueryTest.h"
#include "StorageDialect.h"
#include "StringRowSink.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// c[i] with an index held in a tagged cell reads a key where the row's index holds a string,
// and a position where it holds an integer. An index holding one literal in every row reads
// what c.key or c[position] reads, over a base whose rows all take that kind of index.
class FoldContainerIndexOverConstantsTest : public HashJoinQueryTest {
protected:
    void expectPlansAs(std::string_view indexQuery, std::string_view plainQuery) {
        std::string indexProgram;
        dbProgram(indexQuery, indexProgram);
        EXPECT_FALSE(contains(indexProgram, "db.container_index")) << indexProgram;

        std::string indexLowered;
        nlProgram(indexQuery, indexLowered);

        std::string plainLowered;
        nlProgram(plainQuery, plainLowered);

        EXPECT_EQ(indexLowered, plainLowered);
    }

    void expectReadsAKeyOffTheBase(std::string_view query, std::string_view key) {
        std::string program;
        dbProgram(query, program);

        const std::string keyRead = "key \"" + std::string(key) + "\"";

        EXPECT_FALSE(contains(program, "db.container_index")) << program;
        EXPECT_TRUE(contains(program, "db.static_map_key")) << program;
        EXPECT_TRUE(contains(program, keyRead)) << program;
    }

    void expectReadsAPropertyOffTheBase(std::string_view query, std::string_view read, std::string_view property) {
        std::string program;
        dbProgram(query, program);

        const std::string propertyRead = std::string(read) + "(%";
        const std::string propertyName = "\"" + std::string(property) + "\")";

        EXPECT_FALSE(contains(program, "db.container_index")) << program;
        EXPECT_TRUE(contains(program, propertyRead)) << program;
        EXPECT_TRUE(contains(program, propertyName)) << program;
    }

    void expectKeepsTheContainerIndex(std::string_view query) {
        std::string program;
        dbProgram(query, program);

        EXPECT_TRUE(contains(program, "db.container_index")) << program;
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
};

TEST_F(FoldContainerIndexOverConstantsTest, readsAKeyReadOutOfAConstantMap) {
    const std::string_view indexQuery = "WITH {k: 't'} AS m UNWIND [{t: 1}] AS row RETURN row[m.k] AS v";
    const std::string_view plainQuery = "UNWIND [{t: 1}] AS row RETURN row.t AS v";

    expectPlansAs(indexQuery, plainQuery);
    expectReadsAKeyOffTheBase(indexQuery, "t");
    expectSortedRows(indexQuery, {{"1"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, readsAKeyAtAPositionOfAMixedList) {
    const std::string_view indexQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row[['t', 1][0]] AS v";
    const std::string_view plainQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row.t AS v";

    expectPlansAs(indexQuery, plainQuery);
    expectSortedRows(indexQuery, {{"1"}, {"2"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, readsAKeyOfAMapHeldByAMap) {
    const std::string_view indexQuery = "WITH {k: 't'} AS m UNWIND [{s: {t: 1}}, {s: null}, null] AS row "
                                        "RETURN row.s[m.k] AS v";
    const std::string_view plainQuery = "UNWIND [{s: {t: 1}}, {s: null}, null] AS row RETURN row.s.t AS v";

    expectPlansAs(indexQuery, plainQuery);
    expectSortedRows(indexQuery, {{"1"}, {"null"}, {"null"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, joinsOnAValueReadByAKeyOutOfAConstantMap) {
    const std::string_view query = "WITH {k: 't'} AS m UNWIND [{s: {t: {name: 'Remy'}}}, {s: {t: {name: 'Luc'}}}] AS row "
                                   "MATCH (a:Person {name: row.s[m.k].name}) RETURN a.name";

    std::string program;
    dbProgram(query, program);
    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;
    EXPECT_FALSE(contains(program, "db.container_index")) << program;

    expectSortedRows(query, {{"Luc"}, {"Remy"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, readsAPropertyOfANode) {
    const std::string_view query = "MATCH (n:Person {name: 'Remy'}) WITH n, {k: 'name'} AS m RETURN n[m.k] AS v";

    expectReadsAPropertyOffTheBase(query, "db.get_node_properties", "name");
    expectSortedRows(query, {{"Remy"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, readsAPropertyOfAnEdge) {
    const std::string_view query = "MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() "
                                   "WITH e, {k: 'name'} AS m RETURN e[m.k] AS v";

    expectReadsAPropertyOffTheBase(query, "db.get_edge_properties", "name");
    expectSortedRows(query, {{"Remy -> Adam"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, readsNullOffANodeAnOptionalMatchMissed) {
    const std::string_view query = "OPTIONAL MATCH (n:Person {name: 'Nobody'}) "
                                   "WITH n, {k: 'name'} AS m RETURN n[m.k] AS v";

    expectReadsAPropertyOffTheBase(query, "db.get_node_properties", "name");
    expectSortedRows(query, {{"null"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, readsAnElementAtAPositionReadOutOfAConstantMap) {
    expectPlansAs("WITH {k: 1} AS m UNWIND [[1, 2], [3, 4]] AS row RETURN row[m.k] AS v",
                  "UNWIND [[1, 2], [3, 4]] AS row RETURN row[1] AS v");
    expectPlansAs("WITH {k: -1} AS m UNWIND [[1, 2], [3, 4]] AS row RETURN row[m.k] AS v",
                  "UNWIND [[1, 2], [3, 4]] AS row RETURN row[-1] AS v");

    expectSortedRows("WITH {k: 1} AS m UNWIND [[1, 2], [3, 4]] AS row RETURN row[m.k] AS v", {{"2"}, {"4"}});
    expectSortedRows("WITH {k: -2} AS m UNWIND [[1, 2], [3, 4]] AS row RETURN row[m.k] AS v", {{"1"}, {"3"}});
    expectSortedRows("WITH {k: 2} AS m UNWIND [[1, 2], [3, 4]] AS row RETURN row[m.k] AS v", {{"null"}, {"null"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, readsAnElementOfAListHeldByAMap) {
    const std::string_view indexQuery = "WITH {k: 0} AS m UNWIND [{s: [5, 6]}, {s: null}] AS row "
                                        "RETURN row.s[m.k] AS v";
    const std::string_view plainQuery = "UNWIND [{s: [5, 6]}, {s: null}] AS row RETURN row.s[0] AS v";

    expectPlansAs(indexQuery, plainQuery);
    expectSortedRows(indexQuery, {{"5"}, {"null"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, keepsAPositionOverAMap) {
    const std::string_view query = "WITH {k: 0} AS m UNWIND [{t: 1}] AS row RETURN row[m.k] AS v";

    expectKeepsTheContainerIndex(query);
    expectRaises(query);
}

TEST_F(FoldContainerIndexOverConstantsTest, keepsAKeyOverAList) {
    const std::string_view query = "WITH {k: 't'} AS m UNWIND [[1, 2]] AS row RETURN row[m.k] AS v";

    expectKeepsTheContainerIndex(query);
    expectRaises(query);
}

TEST_F(FoldContainerIndexOverConstantsTest, keepsAKeyEachRowReads) {
    const std::string_view query = "UNWIND [{k: 't'}, {k: 'u'}] AS m UNWIND [{t: 1, u: 2}] AS row RETURN row[m.k] AS v";

    expectKeepsTheContainerIndex(query);
    expectSortedRows(query, {{"1"}, {"2"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, keepsAPositionEachRowReads) {
    const std::string_view query = "UNWIND [{k: 0}, {k: 1}] AS m UNWIND [[5, 6]] AS row RETURN row[m.k] AS v";

    expectKeepsTheContainerIndex(query);
    expectSortedRows(query, {{"5"}, {"6"}});
}

TEST_F(FoldContainerIndexOverConstantsTest, keepsAKeyOverABaseThatMayHoldAList) {
    const std::string_view query = "WITH {k: 't'} AS m UNWIND [{t: 1}, [5]] AS row RETURN row[m.k] AS v";

    expectKeepsTheContainerIndex(query);
    expectRaises(query);
}

TEST_F(FoldContainerIndexOverConstantsTest, keepsAPositionOverABaseThatMayHoldAMap) {
    const std::string_view query = "WITH {k: 0} AS m UNWIND [[5], {t: 1}] AS row RETURN row[m.k] AS v";

    expectKeepsTheContainerIndex(query);
    expectRaises(query);
}

// A column carried past a sort keeps the type it was read with, so the op standing in for the
// index read takes the index read's untyped column
class FoldContainerIndexOverConstantsPassTest : public ::testing::Test {
protected:
    FoldContainerIndexOverConstantsPassTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    void fold(std::string_view ops, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        std::string program = R"mlir(
!cells = !db.column<!storage.any>
!maps = !db.column<!storage.map>
!unresolved = !db.column<none>

func.func @main() {
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

    static bool isUnresolved(mlir::Value column) {
        const mlir::db::ColumnType columnType = mlir::cast<mlir::db::ColumnType>(column.getType());
        return mlir::isa<mlir::NoneType>(columnType.getType());
    }

    mlir::MLIRContext _context;
};

TEST_F(FoldContainerIndexOverConstantsPassTest, readsAKeyIntoTheIndexReadsColumn) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    ASSERT_NO_FATAL_FAILURE(fold(R"mlir(
  %rows = db.unwind_const([{t = 2 : i64}, {t = 1 : i64}]) : !cells
  %m = db.constant({k = "t" : !storage.string})
  %key = db.static_map_key %m key "k" : (!maps) -> !cells
  %t = db.container_index %rows, %key : (!cells, !cells) -> !unresolved
  %sorted:2 = db.sort(%rows, %t) keys [1] ascending [true] : (!cells, !unresolved) -> (!cells, !unresolved)
  db.output(%sorted#1) : !unresolved
)mlir",
                                 module));

    EXPECT_EQ(countOps<mlir::db::ContainerIndex>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ConstantOp>(*module), 0u);
    ASSERT_EQ(countOps<mlir::db::StaticMapKey>(*module), 1u);

    module->walk([](mlir::db::StaticMapKey keyRead) {
        EXPECT_EQ(keyRead.getKey(), "t");
        EXPECT_TRUE(isUnresolved(keyRead.getResult()));
    });
}

TEST_F(FoldContainerIndexOverConstantsPassTest, readsAnElementIntoTheIndexReadsColumn) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    ASSERT_NO_FATAL_FAILURE(fold(R"mlir(
  %rows = db.unwind_const([[1 : i64, 2 : i64], [3 : i64, 4 : i64]]) : !cells
  %m = db.constant({k = 1 : i64})
  %at = db.static_map_key %m key "k" : (!maps) -> !cells
  %e = db.container_index %rows, %at : (!cells, !cells) -> !unresolved
  %sorted:2 = db.sort(%rows, %e) keys [1] ascending [true] : (!cells, !unresolved) -> (!cells, !unresolved)
  db.output(%sorted#1) : !unresolved
)mlir",
                                 module));

    EXPECT_EQ(countOps<mlir::db::ContainerIndex>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::StaticMapKey>(*module), 0u);
    ASSERT_EQ(countOps<mlir::db::ListIndex>(*module), 1u);

    module->walk([](mlir::db::ListIndex element) {
        mlir::db::ConstantOp position = element.getRhs().getDefiningOp<mlir::db::ConstantOp>();
        ASSERT_TRUE(position);

        const mlir::IntegerAttr literal = mlir::dyn_cast<mlir::IntegerAttr>(position.getValue());
        ASSERT_TRUE(literal);
        EXPECT_EQ(literal.getInt(), 1);

        EXPECT_TRUE(isUnresolved(element.getResult()));
    });
}
