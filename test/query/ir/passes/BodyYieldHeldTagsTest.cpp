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

// A list a body builds holds the values the body yields, and the accumulator of a fold holds its
// initial value or a value its body yields. A key read off either raises on no row where those
// values are maps, nodes, edges or null.
class BodyYieldHeldTagsTest : public HashJoinQueryTest {
protected:
    void expectPlansAsTheDottedForm(std::string_view bracketQuery, std::string_view dottedQuery, const Rows& expected) {
        std::string bracketProgram;
        dbProgram(bracketQuery, bracketProgram);

        std::string dottedProgram;
        dbProgram(dottedQuery, dottedProgram);

        EXPECT_FALSE(contains(bracketProgram, "db.dynamic_map_key")) << bracketProgram;
        EXPECT_EQ(bracketProgram, dottedProgram);

        expectSortedRows(bracketQuery, expected);
    }

    void expectKeepsTheBracketReadAndRaises(std::string_view query) {
        std::string program;
        dbProgram(query, program);

        EXPECT_TRUE(contains(program, "db.dynamic_map_key")) << program;

        expectRaises(query);
    }

    void expectRaises(std::string_view query) {
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

    template <typename ReadOp>
    bool isReadSpeculatable(std::string_view rows, std::string_view ops) {
        std::string program = R"mlir(
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>
!tags = !db.column<ui64>
!integers = !db.column<i64>
!nodes = !db.column<!storage.node_id>
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

        size_t readCount = 0;
        mlir::Operation* read = nullptr;
        module->walk([&readCount, &read](ReadOp readOp) {
            readCount++;
            read = readOp.getOperation();
        });

        EXPECT_EQ(readCount, 1u) << program;

        return read && mlir::isSpeculatable(read);
    }

    bool isKeyReadSpeculatable(std::string_view rows, std::string_view ops) {
        return isReadSpeculatable<mlir::db::StaticMapKey>(rows, ops);
    }
};

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAnElementOfAListComprehension) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row WITH [x IN [row] | x.s] AS l RETURN l[0]['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row WITH [x IN [row] | x.s] AS l RETURN l[0].t AS v",
                               {{"1"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAnElementOfAListComprehensionWithAWhere) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}, {s: {t: 2}}] AS row "
                               "WITH [x IN [row, row] WHERE x.s.t > 1 | x.s] AS l RETURN l[1]['t'] AS v",
                               "UNWIND [{s: {t: 1}}, {s: {t: 2}}] AS row "
                               "WITH [x IN [row, row] WHERE x.s.t > 1 | x.s] AS l RETURN l[1].t AS v",
                               {{"2"}, {"null"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAnElementOfAListComprehensionOverAnEmptyListOrNull) {
    expectPlansAsTheDottedForm("UNWIND [[], null, [{s: {t: 1}}]] AS xs WITH [x IN xs | x.s] AS l RETURN l[0]['t'] AS v",
                               "UNWIND [[], null, [{s: {t: 1}}]] AS xs WITH [x IN xs | x.s] AS l RETURN l[0].t AS v",
                               {{"1"}, {"null"}, {"null"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAnElementOfNestedListComprehensions) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row WITH [x IN [row] | [y IN [x] | y.s]] AS l RETURN l[0][0]['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row WITH [x IN [row] | [y IN [x] | y.s]] AS l RETURN l[0][0].t AS v",
                               {{"1"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAValueUnwoundFromAListComprehension) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row UNWIND [x IN [row] | x.s] AS m RETURN m['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row UNWIND [x IN [row] | x.s] AS m RETURN m.t AS v",
                               {{"1"}});
}

TEST_F(BodyYieldHeldTagsTest, raisesOnAnElementOfAListComprehensionHoldingNoKeys) {
    expectKeepsTheBracketReadAndRaises("UNWIND [{s: {t: 1}}, {s: 5}] AS row WITH [x IN [row] | x.s] AS l RETURN l[0]['t'] AS v");
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAnElementOfAPatternComprehension) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row MATCH (a:Person {name: 'Remy'}) "
                               "WITH [(a)-[:KNOWS_WELL]->(b) | row.s] AS l RETURN l[0]['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row MATCH (a:Person {name: 'Remy'}) "
                               "WITH [(a)-[:KNOWS_WELL]->(b) | row.s] AS l RETURN l[0].t AS v",
                               {{"1"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAnElementOfACollectSubquery) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row MATCH (a:Person {name: 'Remy'}) "
                               "WITH COLLECT { MATCH (a)-[:KNOWS_WELL]->(b) RETURN row.s AS s } AS l RETURN l[0]['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row MATCH (a:Person {name: 'Remy'}) "
                               "WITH COLLECT { MATCH (a)-[:KNOWS_WELL]->(b) RETURN row.s AS s } AS l RETURN l[0].t AS v",
                               {{"1"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffTheResultOfAReduce) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row WITH reduce(acc = row, x IN [1] | acc) AS m RETURN m.s['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row WITH reduce(acc = row, x IN [1] | acc) AS m RETURN m.s.t AS v",
                               {{"1"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffTheAccumulatorOfAReduce) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row RETURN reduce(acc = row, x IN [1, 2] | CASE WHEN acc.s['t'] = 1 THEN acc ELSE row END) AS v",
                               "UNWIND [{s: {t: 1}}] AS row RETURN reduce(acc = row, x IN [1, 2] | CASE WHEN acc.s.t = 1 THEN acc ELSE row END) AS v",
                               {{"{s: {t: 1}}"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAnAccumulatorStartingNull) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row WITH reduce(acc = null, x IN [row] | x) AS m RETURN m.s['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row WITH reduce(acc = null, x IN [row] | x) AS m RETURN m.s.t AS v",
                               {{"1"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAnAccumulatorStartingNullOverAnEmptyList) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row WITH reduce(acc = null, x IN [] | row) AS m RETURN m.s['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row WITH reduce(acc = null, x IN [] | row) AS m RETURN m.s.t AS v",
                               {{"null"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAnElementOfAListAReduceBuilds) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row WITH reduce(acc = [], x IN [row] | [x.s]) AS l RETURN l[0]['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row WITH reduce(acc = [], x IN [row] | [x.s]) AS l RETURN l[0].t AS v",
                               {{"1"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAReduceInAListComprehension) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row WITH [x IN [row] | reduce(acc = x, y IN [1, 2] | acc)] AS l "
                               "RETURN l[0].s['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row WITH [x IN [row] | reduce(acc = x, y IN [1, 2] | acc)] AS l "
                               "RETURN l[0].s.t AS v",
                               {{"1"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffAListComprehensionInAReduce) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row WITH reduce(acc = [row], y IN [1, 2] | [x IN acc | x]) AS l "
                               "RETURN l[0].s['t'] AS v",
                               "UNWIND [{s: {t: 1}}] AS row WITH reduce(acc = [row], y IN [1, 2] | [x IN acc | x]) AS l "
                               "RETURN l[0].s.t AS v",
                               {{"1"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffTheAccumulatorsOfAnAllReduce) {
    expectPlansAsTheDottedForm("UNWIND [{s: {t: 1}}] AS row "
                               "RETURN allReduce(acc = row, x IN [1, 2] | CASE WHEN acc.s['t'] = 1 THEN acc ELSE row END, acc.s['t'] = 1) AS v",
                               "UNWIND [{s: {t: 1}}] AS row "
                               "RETURN allReduce(acc = row, x IN [1, 2] | CASE WHEN acc.s.t = 1 THEN acc ELSE row END, acc.s.t = 1) AS v",
                               {{"true"}});
}

TEST_F(BodyYieldHeldTagsTest, readsAKeyOffTheAccumulatorOfAReduceOverOneElement) {
    expectSortedRows("UNWIND [{t: {t: 1}}] AS init RETURN reduce(acc = init, x IN [1] | acc['t']) AS v",
                     {{"{t: 1}"}});
}

TEST_F(BodyYieldHeldTagsTest, raisesOnAnAccumulatorABodyReadsIntoPastItsKeys) {
    expectKeepsTheBracketReadAndRaises("UNWIND [{t: {t: 1}}] AS init RETURN reduce(acc = init, x IN [1, 2, 3] | acc['t']) AS v");
}

TEST_F(BodyYieldHeldTagsTest, raisesOnAReduceTurningAMapIntoAnInteger) {
    expectKeepsTheBracketReadAndRaises("UNWIND [{t: 1}] AS init WITH reduce(acc = init, x IN [1] | acc.t) AS m RETURN m['t'] AS v");
}

TEST_F(BodyYieldHeldTagsTest, readsNullOffAnAccumulatorABodyReadsIntoPastItsKeys) {
    expectSortedRows("UNWIND [{}] AS m WITH reduce(acc = m, x IN [1, 2] | acc.t) AS r RETURN r['k'] AS v",
                     {{"null"}});
}

TEST_F(BodyYieldHeldTagsTest, raisesOnAnUnreadIndexAReduceWidensToAFloat) {
    expectRaises("UNWIND [1] AS k WITH k, [10, 20, 30][reduce(s = 0, x IN [1.5] | s + x)] AS unused RETURN k AS v");
    expectRaises("UNWIND [1] AS k WITH k, [10, 20, 30][reduce(s = 0, x IN [1.5] | x)] AS unused RETURN k AS v");
}

TEST_F(BodyYieldHeldTagsTest, readsAnIndexAReduceKeepsAnInteger) {
    expectSortedRows("UNWIND [1] AS k RETURN [10, 20, 30][reduce(s = 0, x IN [1, 2] | x)] AS v",
                     {{"30"}});
}

TEST_F(BodyYieldHeldTagsTest, listsTheValuesAListComprehensionYields) {
    const std::string_view ops = R"mlir(
  %xs = db.constant([1, 2])
  %zero = db.constant(0 : i64)
  %l = db.list_comprehension(%xs, {%rows}) {
  ^bb0(%x: !unresolved, %tag: !tags, %row: !cells):
    db.comprehension_yield %tag, %row : !cells
  } : (!db.column<!storage.list<i64>>, !cells) -> !unresolved
  %e = db.list_index %l, %zero : (!unresolved, !integers) -> !unresolved
  %t = db.static_map_key %e key "t" : (!unresolved) -> !cells
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}

TEST_F(BodyYieldHeldTagsTest, readsNoKeyOffTheListAListComprehensionBuilds) {
    const std::string_view ops = R"mlir(
  %xs = db.constant([1, 2])
  %l = db.list_comprehension(%xs, {%rows}) {
  ^bb0(%x: !unresolved, %tag: !tags, %row: !cells):
    db.comprehension_yield %tag, %row : !cells
  } : (!db.column<!storage.list<i64>>, !cells) -> !unresolved
  %t = db.static_map_key %l key "t" : (!unresolved) -> !cells
)mlir";

    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
}

TEST_F(BodyYieldHeldTagsTest, listsTheValuesAPatternComprehensionYields) {
    const std::string_view ops = R"mlir(
  %zero = db.constant(0 : i64)
  %l = db.pattern_comprehension(%n, %rows) {
  ^bb0(%nIn: !nodes, %row: !cells, %tag: !tags):
    db.comprehension_yield %tag, %row : !cells
  } : (!nodes, !cells) -> !unresolved
  %e = db.list_index %l, %zero : (!unresolved, !integers) -> !unresolved
  %t = db.static_map_key %e key "t" : (!unresolved) -> !cells
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}

TEST_F(BodyYieldHeldTagsTest, listsTheValuesACollectSubqueryYields) {
    const std::string_view ops = R"mlir(
  %zero = db.constant(0 : i64)
  %l = db.collect_subquery(%rows) carries_scope {
  ^bb0(%row: !cells, %tag: !tags):
    db.collect_subquery_yield tag %tag, %row, {} : !cells
  } : (!cells) -> !unresolved
  %e = db.list_index %l, %zero : (!unresolved, !integers) -> !unresolved
  %t = db.static_map_key %e key "t" : (!unresolved) -> !cells
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}

TEST_F(BodyYieldHeldTagsTest, foldsTheInitialValueAndTheYieldIntoTheResultOfAReduce) {
    const std::string_view ops = R"mlir(
  %xs = db.constant([1, 2])
  %r = db.reduce(%xs, %rows, {}) {
  ^bb0(%x: !unresolved, %acc: !unresolved):
    db.reduce_yield %acc : !unresolved
  } : (!db.column<!storage.list<i64>>, !cells) -> !unresolved
  %t = db.static_map_key %r key "t" : (!unresolved) -> !cells
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}

TEST_F(BodyYieldHeldTagsTest, foldsTheYieldIntoTheAccumulatorOfAReduce) {
    const std::string_view ops = R"mlir(
  %xs = db.constant([1, 2])
  %init = db.constant({t = 1 : i64})
  %r = db.reduce(%xs, %init, {%rows}) {
  ^bb0(%x: !unresolved, %acc: !unresolved, %row: !cells):
    %t = db.static_map_key %acc key "t" : (!unresolved) -> !cells
    db.reduce_yield %row : !cells
  } : (!db.column<!storage.list<i64>>, !db.column<!storage.map>, !cells) -> !unresolved
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", ops));
}

TEST_F(BodyYieldHeldTagsTest, stopsAtABodyReadingDeeperIntoItsAccumulatorAtEachStep) {
    const std::string_view ops = R"mlir(
  %xs = db.constant([1, 2])
  %r = db.reduce(%xs, %rows, {}) {
  ^bb0(%x: !unresolved, %acc: !unresolved):
    %next = db.static_map_key %acc key "t" : (!unresolved) -> !cells
    db.reduce_yield %next : !cells
  } : (!db.column<!storage.list<i64>>, !cells) -> !unresolved
)mlir";

    EXPECT_FALSE(isKeyReadSpeculatable("[{t = {t = {t = 1 : i64}}}]", ops));
    EXPECT_FALSE(isKeyReadSpeculatable("[{}]", ops));
}

TEST_F(BodyYieldHeldTagsTest, foldsTheBodyYieldIntoTheAccumulatorsOfAnAllReduce) {
    const std::string_view bodyRead = R"mlir(
  %xs = db.constant([1, 2])
  %r = db.all_reduce(%xs, %rows, {}) {
  ^bb0(%x: !unresolved, %acc: !unresolved):
    %t = db.static_map_key %acc key "t" : (!unresolved) -> !cells
    db.reduce_yield %acc : !unresolved
  } predicate {
  ^bb0(%x: !unresolved, %acc: !unresolved):
    %p = db.eq %acc, %acc : (!unresolved, !unresolved) -> !bools
    db.all_reduce_yield %p : !bools
  } : (!db.column<!storage.list<i64>>, !cells) -> !bools
)mlir";

    const std::string_view predicateRead = R"mlir(
  %xs = db.constant([1, 2])
  %init = db.constant(5 : i64)
  %r = db.all_reduce(%xs, %init, {%rows}) {
  ^bb0(%x: !unresolved, %acc: !unresolved, %row: !cells):
    db.reduce_yield %row : !cells
  } predicate {
  ^bb0(%x: !unresolved, %acc: !unresolved, %row: !cells):
    %t = db.static_map_key %acc key "t" : (!unresolved) -> !cells
    %p = db.eq %t, %t : (!cells, !cells) -> !bools
    db.all_reduce_yield %p : !bools
  } : (!db.column<!storage.list<i64>>, !integers, !cells) -> !bools
)mlir";

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", bodyRead));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", bodyRead));

    EXPECT_TRUE(isKeyReadSpeculatable("[{t = 1 : i64}, unit]", predicateRead));
    EXPECT_FALSE(isKeyReadSpeculatable("[{t = 1 : i64}, 5 : i64]", predicateRead));
}

TEST_F(BodyYieldHeldTagsTest, indexesByAReduceTakingOnlyIntegers) {
    const std::string_view integers = R"mlir(
  %list = db.constant([10, 20, 30])
  %xs = db.constant([1, 2])
  %zero = db.constant(0 : i64)
  %i = db.reduce(%xs, %zero, {}) {
  ^bb0(%x: !unresolved, %acc: !unresolved):
    db.reduce_yield %x : !unresolved
  } : (!db.column<!storage.list<i64>>, !integers) -> !unresolved
  %r = db.container_index %list, %i : (!db.column<!storage.list<i64>>, !unresolved) -> !unresolved
)mlir";

    const std::string_view widened = R"mlir(
  %list = db.constant([10, 20, 30])
  %xs = db.constant([1.5])
  %zero = db.constant(0 : i64)
  %i = db.reduce(%xs, %zero, {}) {
  ^bb0(%x: !unresolved, %acc: !unresolved):
    db.reduce_yield %x : !unresolved
  } : (!db.column<!storage.list<f64>>, !integers) -> !unresolved
  %r = db.container_index %list, %i : (!db.column<!storage.list<i64>>, !unresolved) -> !unresolved
)mlir";

    EXPECT_TRUE(isReadSpeculatable<mlir::db::ContainerIndex>("[unit]", integers));
    EXPECT_FALSE(isReadSpeculatable<mlir::db::ContainerIndex>("[unit]", widened));
}
