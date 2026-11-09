#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Parser/Parser.h"

#include "DBOps.h"
#include "HashJoinQueryTest.h"
#include "StringRowSink.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// A column carried into the body of an op holds, in every row of the body, a value of the column
// carried in, and the element the body of an op over a list binds holds an element of that list.
// A key read off either raises on no row where those values are maps, nodes, edges or null.
class ListBodyArgumentHeldTagsTest : public HashJoinQueryTest {
protected:
    void expectFoldsTheBracketRead(std::string_view query, const Rows& expected) {
        std::string program;
        dbProgram(query, program);

        EXPECT_FALSE(contains(program, "db.dynamic_map_key")) << program;

        expectSortedRows(query, expected);
    }

    void expectKeepsTheBracketReadAndRaises(std::string_view query) {
        std::string program;
        dbProgram(query, program);

        EXPECT_TRUE(contains(program, "db.dynamic_map_key")) << program;

        StringRowSink sink;
        EXPECT_THROW(runQuery(query, sink), TuringException) << "query: " << query;
    }

    void expectSortedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());

        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    bool isKeyReadSpeculatable(std::string_view rows, std::string_view ops) {
        std::string program = R"mlir(
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>
!tags = !db.column<ui64>
!nodes = !db.column<!storage.node_id>
!edges = !db.column<!storage.edge_id>
!bools = !db.column<!storage.bool>

func.func @main(%n: !nodes) {
  %rows = db.unwind_const()mlir";

        program += rows;
        program += ") : !cells\n";
        program += ops;
        program += "\n  return\n}\n";

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module) << program;
        if (!module) {
            return false;
        }

        size_t keyReadCount = 0;
        mlir::Operation* keyRead = nullptr;
        module->walk([&keyReadCount, &keyRead](mlir::db::StaticMapKey mapKey) {
            keyReadCount++;
            keyRead = mapKey.getOperation();
        });

        EXPECT_EQ(keyReadCount, 1u) << program;

        return keyRead && mlir::isSpeculatable(keyRead);
    }
};

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffAColumnCarriedIntoAListComprehension) {
    expectFoldsTheBracketRead("WITH 't' AS k UNWIND [{s: {t: 1}}] AS row RETURN [x IN [1, 2] | row.s[k]] AS v",
                              {{"1, 1"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyCarriedIntoAListComprehension) {
    expectFoldsTheBracketRead("WITH 't' AS k WHERE k IS NOT NULL UNWIND [{s: {t: 1}}] AS row "
                              "RETURN [x IN [1, 2] | row.s[k]] AS v",
                              {{"1, 1"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffTheElementOfAListComprehension) {
    expectFoldsTheBracketRead("UNWIND [{s: {t: 1}}] AS row RETURN [x IN [row] | x.s['t']] AS v",
                              {{"1"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffAnElementCarriedIntoAnInnerComprehension) {
    expectFoldsTheBracketRead("UNWIND [{s: {t: 1}}] AS row RETURN [x IN [row] | [y IN [1, 2] | x.s['t']]] AS v",
                              {{"[1, 1]"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffTheElementOfAnElement) {
    expectFoldsTheBracketRead("UNWIND [[[{t: 1}], [{t: 2}, null]]] AS lists RETURN [l IN lists | [m IN l | m['t']]] AS v",
                              {{"[1], [2, null]"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffTheElementOfAListHeldByOnlySomeRows) {
    expectFoldsTheBracketRead("UNWIND [[{t: 1}], null] AS l RETURN [x IN l | x['t']] AS v",
                              {{"1"}, {"null"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffTheElementOfAListPredicate) {
    expectFoldsTheBracketRead("UNWIND [{s: {t: 1}}, {s: {t: 2}}] AS row RETURN any(x IN [row] WHERE x.s['t'] = 1) AS v",
                              {{"false"}, {"true"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffAColumnCarriedIntoAListPredicate) {
    expectFoldsTheBracketRead("UNWIND [{s: {t: 1}}, {s: {t: 2}}] AS row RETURN all(x IN [1, 2] WHERE row.s['t'] <= x) AS v",
                              {{"false"}, {"true"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffTheElementOfAReduce) {
    expectFoldsTheBracketRead("UNWIND [{s: {t: 1}}] AS row RETURN reduce(acc = 0, x IN [row, row] | acc + x.s['t']) AS v",
                              {{"2"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffAColumnCarriedIntoAReduce) {
    expectFoldsTheBracketRead("UNWIND [{s: {t: 3}}] AS row RETURN reduce(acc = 0, x IN [1, 2] | acc + x * row.s['t']) AS v",
                              {{"9"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffTheElementOfAnAllReduce) {
    expectFoldsTheBracketRead("UNWIND [{s: {t: 1}}] AS row "
                              "RETURN allReduce(acc = 0, x IN [row, row] | acc + x.s['t'], x.s['t'] < 3) AS v",
                              {{"true"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffAColumnCarriedIntoAnAllReduce) {
    expectFoldsTheBracketRead("UNWIND [{s: {t: 2}}] AS row "
                              "RETURN allReduce(acc = 0, x IN [1, 2] | acc + row.s['t'], acc <= row.s['t'] * x) AS v",
                              {{"true"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, readsAKeyOffAColumnCarriedIntoAPatternComprehension) {
    expectFoldsTheBracketRead("UNWIND [{s: {t: 1}}] AS row MATCH (a:Person {name: 'Remy'}) "
                              "RETURN [(a)-[:KNOWS_WELL]->(b) | row.s['t']] AS v",
                              {{"1"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, raisesOnAnElementHoldingNoKeys) {
    expectKeepsTheBracketReadAndRaises("UNWIND [[{t: 1}, 5]] AS l RETURN [x IN l | x['t']] AS v");
}

TEST_F(ListBodyArgumentHeldTagsTest, raisesOnAnElementOfAListPredicateHoldingNoKeys) {
    expectKeepsTheBracketReadAndRaises("UNWIND [[{t: 1}, 5]] AS l RETURN any(x IN l WHERE x['t'] = 1) AS v");
}

TEST_F(ListBodyArgumentHeldTagsTest, raisesOnACarriedColumnHoldingNoKeys) {
    expectKeepsTheBracketReadAndRaises("UNWIND [{s: {t: 1}}, {s: 5}] AS row RETURN [x IN [1] | row.s['t']] AS v");
}

TEST_F(ListBodyArgumentHeldTagsTest, readsNoKeyOffACarriedColumnInARowWithNoElement) {
    expectSortedRows("UNWIND [{l: [], s: 5}, {l: [1], s: {t: 1}}] AS row RETURN [x IN row.l | row.s['t']] AS v",
                     {{""}, {"1"}});
}

TEST_F(ListBodyArgumentHeldTagsTest, raisesOnAnAccumulatorHoldingNoKeys) {
    expectKeepsTheBracketReadAndRaises("UNWIND [{t: 5}] AS init RETURN reduce(acc = init, x IN [1, 2] | acc['t']) AS v");
}

TEST_F(ListBodyArgumentHeldTagsTest, carriesAColumnIntoAListComprehension) {
    const std::string_view ops = R"mlir(
  %xs = db.constant([1, 2])
  %r = db.list_comprehension(%xs, {%rows}) {
  ^bb0(%x: !unresolved, %tag: !tags, %row: !cells):
    %t = db.static_map_key %row key "t" : (!cells) -> !cells
    db.comprehension_yield %tag, %t : !cells
  } : (!db.column<!storage.list<i64>>, !cells) -> !unresolved
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}

TEST_F(ListBodyArgumentHeldTagsTest, bindsTheElementOfAListComprehension) {
    const std::string_view ops = R"mlir(
  %r = db.list_comprehension(%rows, {}) {
  ^bb0(%x: !unresolved, %tag: !tags):
    %t = db.static_map_key %x key "t" : (!unresolved) -> !cells
    db.comprehension_yield %tag, %t : !cells
  } : (!cells) -> !unresolved
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[[{t = 1 : i64}, unit], unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[[{t = 1 : i64}, 5 : i64]]", ops));
}

TEST_F(ListBodyArgumentHeldTagsTest, carriesAColumnIntoAListPredicate) {
    const std::string_view ops = R"mlir(
  %xs = db.constant([1, 2])
  %r = db.list_predicate(%xs, {%rows}) kind any {
  ^bb0(%x: !unresolved, %tag: !tags, %row: !cells):
    %t = db.static_map_key %row key "t" : (!cells) -> !cells
    %p = db.eq %t, %x : (!cells, !unresolved) -> !bools
    db.comprehension_yield %tag, %p : !bools
  } : (!db.column<!storage.list<i64>>, !cells) -> !bools
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}

TEST_F(ListBodyArgumentHeldTagsTest, bindsTheElementOfAListPredicate) {
    const std::string_view ops = R"mlir(
  %r = db.list_predicate(%rows, {}) kind all {
  ^bb0(%x: !unresolved, %tag: !tags):
    %t = db.static_map_key %x key "t" : (!unresolved) -> !cells
    %p = db.eq %t, %t : (!cells, !cells) -> !bools
    db.comprehension_yield %tag, %p : !bools
  } : (!cells) -> !bools
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[[{t = 1 : i64}, unit], unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[[{t = 1 : i64}, 5 : i64]]", ops));
}

TEST_F(ListBodyArgumentHeldTagsTest, carriesAColumnIntoAReduce) {
    const std::string_view ops = R"mlir(
  %xs = db.constant([1, 2])
  %zero = db.constant(0 : i64)
  %r = db.reduce(%xs, %zero, {%rows}) {
  ^bb0(%x: !unresolved, %acc: !unresolved, %row: !cells):
    %t = db.static_map_key %row key "t" : (!cells) -> !cells
    db.reduce_yield %t : !cells
  } : (!db.column<!storage.list<i64>>, !db.column<i64>, !cells) -> !unresolved
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}

TEST_F(ListBodyArgumentHeldTagsTest, bindsTheElementOfAReduce) {
    const std::string_view ops = R"mlir(
  %zero = db.constant(0 : i64)
  %r = db.reduce(%rows, %zero, {}) {
  ^bb0(%x: !unresolved, %acc: !unresolved):
    %t = db.static_map_key %x key "t" : (!unresolved) -> !cells
    db.reduce_yield %t : !cells
  } : (!cells, !db.column<i64>) -> !unresolved
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[[{t = 1 : i64}, unit], unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[[{t = 1 : i64}, 5 : i64]]", ops));
}

TEST_F(ListBodyArgumentHeldTagsTest, leavesTheAccumulatorOfAReduceUnknown) {
    const std::string_view ops = R"mlir(
  %xs = db.constant([1, 2])
  %r = db.reduce(%xs, %rows, {}) {
  ^bb0(%x: !unresolved, %acc: !unresolved):
    %t = db.static_map_key %acc key "t" : (!unresolved) -> !cells
    db.reduce_yield %acc : !unresolved
  } : (!db.column<!storage.list<i64>>, !cells) -> !unresolved
)mlir";

    EXPECT_FALSE(isKeyReadSpeculatable("[5 : i64]", ops));
}

TEST_F(ListBodyArgumentHeldTagsTest, carriesAColumnIntoThePredicateOfAnAllReduce) {
    const std::string_view ops = R"mlir(
  %xs = db.constant([1, 2])
  %zero = db.constant(0 : i64)
  %r = db.all_reduce(%xs, %zero, {%rows}) {
  ^bb0(%x: !unresolved, %acc: !unresolved, %row: !cells):
    db.reduce_yield %acc : !unresolved
  } predicate {
  ^bb0(%x: !unresolved, %acc: !unresolved, %row: !cells):
    %t = db.static_map_key %row key "t" : (!cells) -> !cells
    %p = db.eq %t, %acc : (!cells, !unresolved) -> !bools
    db.all_reduce_yield %p : !bools
  } : (!db.column<!storage.list<i64>>, !db.column<i64>, !cells) -> !bools
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}

TEST_F(ListBodyArgumentHeldTagsTest, bindsTheElementOfThePredicateOfAnAllReduce) {
    const std::string_view ops = R"mlir(
  %zero = db.constant(0 : i64)
  %r = db.all_reduce(%rows, %zero, {}) {
  ^bb0(%x: !unresolved, %acc: !unresolved):
    db.reduce_yield %acc : !unresolved
  } predicate {
  ^bb0(%x: !unresolved, %acc: !unresolved):
    %t = db.static_map_key %x key "t" : (!unresolved) -> !cells
    %p = db.eq %t, %acc : (!cells, !unresolved) -> !bools
    db.all_reduce_yield %p : !bools
  } : (!cells, !db.column<i64>) -> !bools
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[[{t = 1 : i64}, unit], unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[[{t = 1 : i64}, 5 : i64]]", ops));
}

TEST_F(ListBodyArgumentHeldTagsTest, carriesAColumnIntoAPatternComprehension) {
    const std::string_view ops = R"mlir(
  %r = db.pattern_comprehension(%n, %rows) {
  ^bb0(%nIn: !nodes, %row: !cells, %tag: !tags):
    %t = db.static_map_key %row key "t" : (!cells) -> !cells
    db.comprehension_yield %tag, %t : !cells
  } : (!nodes, !cells) -> !unresolved
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}

TEST_F(ListBodyArgumentHeldTagsTest, importsAColumnIntoTheHopOfAnExploration) {
    const std::string_view ops = R"mlir(
  %paths:3 = db.explore_paths(%n, {}) [forward] hops 1 to 2 hop_imports{%rows} {
  ^bb0(%x: !nodes, %e: !edges, %y: !nodes, %row: !cells):
    %t = db.static_map_key %row key "t" : (!cells) -> !cells
    %p = db.eq %t, %t : (!cells, !cells) -> !bools
    db.yield %p : !bools
  } : (!nodes, !cells) -> (!nodes, !nodes, !db.column<!storage.path_ref>)
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}
