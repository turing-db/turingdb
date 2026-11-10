#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/Support/raw_ostream.h"

#include "DBOps.h"
#include "DBPasses.h"
#include "HashJoinQueryTest.h"
#include "StringRowSink.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// A key or a position read off literals holds one literal in every row, which a read of a map
// keyed by it takes as the key m.k spells
class FoldKeyReadsOverConstantsTest : public HashJoinQueryTest {
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

    // The literal the op ahead of the return folds to over the literals of the constants it
    // reads, or "none" where it does not fold
    void expectFolds(std::string_view ops, std::string_view expected) {
        std::string program = R"mlir(
!strings = !db.column<!storage.string>
!integers = !db.column<i64>
!null = !db.column<!storage.nullable<none>>
!maps = !db.column<!storage.map>
!strList = !db.column<!storage.list<!storage.string>>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>

func.func @main() {
  )mlir";

        program += ops;
        program += "\n  return\n}\n";

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module) << ops;

        mlir::func::FuncOp main = module->lookupSymbol<mlir::func::FuncOp>("main");
        mlir::Block& body = main.getBody().front();
        mlir::Operation* const read = body.getTerminator()->getPrevNode();

        llvm::SmallVector<mlir::Attribute> operandLiterals;
        for (const mlir::Value operand : read->getOperands()) {
            mlir::db::ConstantOp constant = operand.getDefiningOp<mlir::db::ConstantOp>();
            operandLiterals.push_back(constant ? constant.getValue() : mlir::Attribute());
        }

        llvm::SmallVector<mlir::OpFoldResult> folded;
        const bool folds = mlir::succeeded(read->fold(operandLiterals, folded)) && folded.size() == 1;
        const mlir::Attribute literal = folds ? llvm::dyn_cast_if_present<mlir::Attribute>(folded.front()) : mlir::Attribute();

        std::string text = "none";
        if (literal) {
            text.clear();
            llvm::raw_string_ostream stream(text);
            literal.print(stream);
        }

        EXPECT_EQ(text, expected) << ops;
    }

    void foldMapKeys(std::string_view ops, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        std::string program = R"mlir(
!strList = !db.column<!storage.list<!storage.string>>
!integers = !db.column<i64>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>

func.func @main() {
  %rows = db.unwind_const([{t = 1 : i64}, {t = 2 : i64}]) : !cells
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
};

TEST_F(FoldKeyReadsOverConstantsTest, readsAKeyAtAPositionOfAConstantList) {
    const std::string_view bracketQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row[['u', 't'][1]] AS v";
    const std::string_view dottedQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row.t AS v";

    expectPlansAsTheDottedForm(bracketQuery, dottedQuery);
    expectSortedRows(bracketQuery, {{"1"}, {"2"}});
}

TEST_F(FoldKeyReadsOverConstantsTest, readsAKeyAtAPositionCountedFromTheEnd) {
    const std::string_view bracketQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row[['t', 'u'][-2]] AS v";
    const std::string_view dottedQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row.t AS v";

    expectPlansAsTheDottedForm(bracketQuery, dottedQuery);
    expectSortedRows(bracketQuery, {{"1"}, {"2"}});
}

TEST_F(FoldKeyReadsOverConstantsTest, readsAKeyAtTheHeadOrTheLastOfAConstantList) {
    const std::string_view dottedQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row.t AS v";

    expectPlansAsTheDottedForm("UNWIND [{t: 1}, {t: 2}] AS row RETURN row[head(['t', 'u'])] AS v", dottedQuery);
    expectPlansAsTheDottedForm("UNWIND [{t: 1}, {t: 2}] AS row RETURN row[last(['u', 't'])] AS v", dottedQuery);
    expectSortedRows("UNWIND [{t: 1}, {t: 2}] AS row RETURN row[last(['u', 't'])] AS v", {{"1"}, {"2"}});
}

TEST_F(FoldKeyReadsOverConstantsTest, readsAKeyOfAListMadeOfConstants) {
    const std::string_view bracketQuery = "WITH 't' AS x WITH [x] AS l UNWIND [{t: 1}, {t: 2}] AS row RETURN row[l[0]] AS v";
    const std::string_view dottedQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row.t AS v";

    expectPlansAsTheDottedForm(bracketQuery, dottedQuery);
    expectSortedRows(bracketQuery, {{"1"}, {"2"}});
}

TEST_F(FoldKeyReadsOverConstantsTest, readsAKeyAtAPositionReadOutOfAConstantMap) {
    const std::string_view bracketQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row[['t'][{k: 0}.k]] AS v";
    const std::string_view dottedQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row.t AS v";

    expectPlansAsTheDottedForm(bracketQuery, dottedQuery);
    expectSortedRows(bracketQuery, {{"1"}, {"2"}});
}

TEST_F(FoldKeyReadsOverConstantsTest, readsAKeyAtAPositionReadOutOfAMapMadeOfConstants) {
    const std::string_view bracketQuery = "WITH 0 AS x WITH {k: x} AS m UNWIND [{t: 1}, {t: 2}] AS row RETURN row[['t'][m.k]] AS v";
    const std::string_view dottedQuery = "UNWIND [{t: 1}, {t: 2}] AS row RETURN row.t AS v";

    expectPlansAsTheDottedForm(bracketQuery, dottedQuery);
    expectSortedRows(bracketQuery, {{"1"}, {"2"}});
}

TEST_F(FoldKeyReadsOverConstantsTest, readsAKeyOfAListLaidOutOverTheRows) {
    const std::string_view query = "MATCH (n:Person {name: 'Remy'}) WITH n, ['t'] AS l "
                                   "UNWIND [{t: 1}, {t: 2}] AS row RETURN n.name, row[l[0]]";

    std::string program;
    dbProgram(query, program);
    EXPECT_FALSE(contains(program, "db.dynamic_map_key")) << program;

    expectSortedRows(query, {{"Remy", "1"}, {"Remy", "2"}});
}

TEST_F(FoldKeyReadsOverConstantsTest, joinsOnAValueReadByAKeyOutOfAConstantList) {
    const std::string_view query = "UNWIND [{s: {t: {name: 'Remy'}}}, {s: {t: {name: 'Luc'}}}] AS row "
                                   "MATCH (a:Person {name: row.s[['t'][0]].name}) RETURN a.name";

    std::string program;
    dbProgram(query, program);
    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;

    expectSortedRows(query, {{"Luc"}, {"Remy"}});
}

// A position past the end reads null, and a null key reads null off every map
TEST_F(FoldKeyReadsOverConstantsTest, keepsAKeyPastTheEndOfAConstantList) {
    const std::string_view query = "UNWIND [{t: 1}] AS row RETURN row[['t'][1]] AS v";

    expectKeepsTheBracketRead(query);
    expectSortedRows(query, {{"null"}});
}

TEST_F(FoldKeyReadsOverConstantsTest, keepsAKeyEachRowReads) {
    const std::string_view query = "UNWIND [0, 1] AS i UNWIND [{t: 1, u: 2}] AS row RETURN row[['t', 'u'][i]] AS v";

    expectKeepsTheBracketRead(query);
    expectSortedRows(query, {{"1"}, {"2"}});
}

TEST_F(FoldKeyReadsOverConstantsTest, raisesOnAListReadByAKey) {
    const std::string_view query = "UNWIND [{t: 1}] AS row RETURN row[['t'][{k: 'k'}.k]] AS v";

    expectKeepsTheBracketRead(query);

    StringRowSink sink;
    EXPECT_THROW(runQuery(query, sink), TuringException);
}

TEST_F(FoldKeyReadsOverConstantsTest, erasesTheReadsTheKeyWasFoldedFrom) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    ASSERT_NO_FATAL_FAILURE(foldMapKeys(R"mlir(
  %keys = db.constant(["u" : !storage.string, "t" : !storage.string])
  %at = db.constant(1 : i64)
  %key = db.list_index %keys, %at : (!strList, !integers) -> !unresolved
  %t = db.dynamic_map_key %rows, %key : (!cells, !unresolved) -> !cells
  db.output(%t) : !cells
)mlir",
                                        module));

    EXPECT_EQ(countOps<mlir::db::DynamicMapKey>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::StaticMapKey>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ListIndex>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ConstantOp>(*module), 0u);
}

TEST_F(FoldKeyReadsOverConstantsTest, foldsAKeyReadOffAMap) {
    expectFolds(R"mlir(%m = db.constant({k = "t" : !storage.string})
  %r = db.static_map_key %m key "k" : (!maps) -> !cells)mlir",
                R"("t" : !storage.string)");
    expectFolds(R"mlir(%m = db.constant({k = "t" : !storage.string})
  %r = db.static_map_key %m key "j" : (!maps) -> !cells)mlir",
                "unit");
    expectFolds(R"mlir(%m = db.constant("" : !storage.nullable<none>)
  %r = db.static_map_key %m key "k" : (!null) -> !cells)mlir",
                "unit");
    expectFolds(R"mlir(%m = db.constant(5 : i64)
  %r = db.static_map_key %m key "k" : (!integers) -> !cells)mlir",
                "none");
    expectFolds(R"mlir(%m = db.constant(["k" : !storage.string])
  %r = db.static_map_key %m key "k" : (!strList) -> !cells)mlir",
                "none");
}

TEST_F(FoldKeyReadsOverConstantsTest, foldsADynamicKeyReadOffAMap) {
    expectFolds(R"mlir(%m = db.constant({k = 1 : i64})
  %k = db.constant("k" : !storage.string)
  %r = db.dynamic_map_key %m, %k : (!maps, !strings) -> !cells)mlir",
                "1 : i64");
    expectFolds(R"mlir(%m = db.constant({k = 1 : i64})
  %k = db.constant("" : !storage.nullable<none>)
  %r = db.dynamic_map_key %m, %k : (!maps, !null) -> !cells)mlir",
                "unit");
    expectFolds(R"mlir(%m = db.constant("" : !storage.nullable<none>)
  %k = db.constant("k" : !storage.string)
  %r = db.dynamic_map_key %m, %k : (!null, !strings) -> !cells)mlir",
                "unit");
    expectFolds(R"mlir(%m = db.constant(5 : i64)
  %k = db.constant("k" : !storage.string)
  %r = db.dynamic_map_key %m, %k : (!integers, !strings) -> !cells)mlir",
                "none");
    expectFolds(R"mlir(%m = db.constant({k = 1 : i64})
  %k = db.constant(0 : i64)
  %r = db.dynamic_map_key %m, %k : (!maps, !integers) -> !cells)mlir",
                "none");
}

TEST_F(FoldKeyReadsOverConstantsTest, foldsAnElementReadOffAList) {
    const std::string_view list = R"mlir(%l = db.constant(["a" : !storage.string, "b" : !storage.string])
  )mlir";

    const auto at = [list](std::string_view position) {
        std::string ops(list);
        ops += "%at = db.constant(";
        ops += position;
        ops += ")\n  %r = db.list_index %l, %at : (!strList, !integers) -> !unresolved";
        return ops;
    };

    expectFolds(at("1 : i64"), R"("b" : !storage.string)");
    expectFolds(at("-1 : i64"), R"("b" : !storage.string)");
    expectFolds(at("-2 : i64"), R"("a" : !storage.string)");
    expectFolds(at("2 : i64"), "unit");
    expectFolds(at("-3 : i64"), "unit");
    expectFolds(R"mlir(%l = db.constant(["a" : !storage.string])
  %at = db.constant("" : !storage.nullable<none>)
  %r = db.list_index %l, %at : (!strList, !null) -> !unresolved)mlir",
                "unit");
    expectFolds(R"mlir(%l = db.constant("" : !storage.nullable<none>)
  %at = db.constant(0 : i64)
  %r = db.list_index %l, %at : (!null, !integers) -> !unresolved)mlir",
                "unit");
    expectFolds(R"mlir(%l = db.constant({k = 1 : i64})
  %at = db.constant(0 : i64)
  %r = db.list_index %l, %at : (!maps, !integers) -> !unresolved)mlir",
                "none");
    expectFolds(R"mlir(%l = db.constant(["a" : !storage.string])
  %at = db.constant(0.0 : f64)
  %r = db.list_index %l, %at : (!strList, !db.column<f64>) -> !unresolved)mlir",
                "none");
}

TEST_F(FoldKeyReadsOverConstantsTest, foldsTheHeadAndTheLastOfAList) {
    expectFolds(R"mlir(%l = db.constant(["a" : !storage.string, "b" : !storage.string])
  %r = db.head(%l) : (!strList) -> !unresolved)mlir",
                R"("a" : !storage.string)");
    expectFolds(R"mlir(%l = db.constant(["a" : !storage.string, "b" : !storage.string])
  %r = db.last(%l) : (!strList) -> !unresolved)mlir",
                R"("b" : !storage.string)");
    expectFolds(R"mlir(%l = db.constant([])
  %r = db.head(%l) : (!db.column<!storage.list<none>>) -> !unresolved)mlir",
                "unit");
    expectFolds(R"mlir(%l = db.constant("" : !storage.nullable<none>)
  %r = db.last(%l) : (!null) -> !unresolved)mlir",
                "unit");
    expectFolds(R"mlir(%l = db.constant(5 : i64)
  %r = db.head(%l) : (!integers) -> !unresolved)mlir",
                "none");
}

TEST_F(FoldKeyReadsOverConstantsTest, foldsAContainerIndexOfAListOrAMap) {
    expectFolds(R"mlir(%c = db.constant(["a" : !storage.string, "b" : !storage.string])
  %i = db.constant(-1 : i64)
  %r = db.container_index %c, %i : (!strList, !integers) -> !unresolved)mlir",
                R"("b" : !storage.string)");
    expectFolds(R"mlir(%c = db.constant({k = 1 : i64})
  %i = db.constant("k" : !storage.string)
  %r = db.container_index %c, %i : (!maps, !strings) -> !unresolved)mlir",
                "1 : i64");
    expectFolds(R"mlir(%c = db.constant({k = 1 : i64})
  %i = db.constant("j" : !storage.string)
  %r = db.container_index %c, %i : (!maps, !strings) -> !unresolved)mlir",
                "unit");
    expectFolds(R"mlir(%c = db.constant("" : !storage.nullable<none>)
  %i = db.constant("k" : !storage.string)
  %r = db.container_index %c, %i : (!null, !strings) -> !unresolved)mlir",
                "unit");
    expectFolds(R"mlir(%c = db.constant({k = 1 : i64})
  %i = db.constant("" : !storage.nullable<none>)
  %r = db.container_index %c, %i : (!maps, !null) -> !unresolved)mlir",
                "unit");
    expectFolds(R"mlir(%c = db.constant(["a" : !storage.string])
  %i = db.constant("k" : !storage.string)
  %r = db.container_index %c, %i : (!strList, !strings) -> !unresolved)mlir",
                "none");
    expectFolds(R"mlir(%c = db.constant({k = 1 : i64})
  %i = db.constant(0 : i64)
  %r = db.container_index %c, %i : (!maps, !integers) -> !unresolved)mlir",
                "none");
}

TEST_F(FoldKeyReadsOverConstantsTest, foldsAMapOrAListMadeOfConstants) {
    expectFolds(R"mlir(%a = db.constant("t" : !storage.string)
  %n = db.constant("" : !storage.nullable<none>)
  %r = db.make_map(%a, %n) keys ["k", "n"] : !strings, !null)mlir",
                R"({k = "t" : !storage.string, n})");
    expectFolds(R"mlir(%a = db.constant("t" : !storage.string)
  %n = db.constant("" : !storage.nullable<none>)
  %r = db.make_list(%a, %n) : (!strings, !null) -> !strList)mlir",
                R"(["t" : !storage.string, unit])");
}
