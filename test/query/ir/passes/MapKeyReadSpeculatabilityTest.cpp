#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "StorageDialect.h"

using mlir::Speculation::NotSpeculatable;
using mlir::Speculation::Speculatable;

// A key read raises on a row holding a value with no keys, so it is speculatable only over a
// column every row of which holds a map, a node, an edge or null
class MapKeyReadSpeculatabilityTest : public ::testing::Test {
protected:
    MapKeyReadSpeculatabilityTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::Speculation::Speculatability speculatabilityOfTheKeyRead(std::string_view ops) {
        std::string program = R"mlir(
!strings = !db.column<!storage.string>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>
!maps = !db.column<!storage.map>
!nodes = !db.column<!storage.node_id>
!edges = !db.column<!storage.edge_id>
!bools = !db.column<!storage.bool>

func.func @main(%string: !strings, %cell: !cells, %unresolved: !unresolved, %map: !maps,
                %node: !nodes, %edge: !edges) {
  )mlir";

        program += ops;
        program += "\n  return\n}\n";

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module) << ops;
        if (!module) {
            return NotSpeculatable;
        }

        mlir::Operation* keyRead = nullptr;
        module->walk([&keyRead](mlir::Operation* op) {
            if (mlir::isa<mlir::db::StaticMapKey, mlir::db::DynamicMapKey>(op)) {
                keyRead = op;
            }
        });

        EXPECT_TRUE(keyRead) << ops;
        if (!keyRead) {
            return NotSpeculatable;
        }

        return mlir::cast<mlir::ConditionallySpeculatable>(keyRead).getSpeculatability();
    }

    mlir::MLIRContext _context;
};

TEST_F(MapKeyReadSpeculatabilityTest, readsAColumnOfMapsNodesOrEdges) {
    EXPECT_EQ(speculatabilityOfTheKeyRead("%r = db.static_map_key %map key \"a\" : (!maps) -> !cells"), Speculatable);
    EXPECT_EQ(speculatabilityOfTheKeyRead("%r = db.static_map_key %node key \"a\" : (!nodes) -> !cells"), Speculatable);
    EXPECT_EQ(speculatabilityOfTheKeyRead("%r = db.dynamic_map_key %edge, %string : (!edges, !strings) -> !cells"), Speculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, raisesOnACellOrAnUnresolvedColumn) {
    EXPECT_EQ(speculatabilityOfTheKeyRead("%r = db.static_map_key %cell key \"a\" : (!cells) -> !cells"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOfTheKeyRead("%r = db.static_map_key %unresolved key \"a\" : (!unresolved) -> !cells"), NotSpeculatable);
    EXPECT_EQ(speculatabilityOfTheKeyRead("%r = db.dynamic_map_key %cell, %string : (!cells, !strings) -> !cells"), NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsAnUnwoundListOfMapsAndNulls) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, unit, {a = "b"}]) : !cells
  %r = db.static_map_key %rows key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, 2 : i64]) : !cells
  %r = db.static_map_key %rows key "a" : (!cells) -> !cells
)mlir"),
              NotSpeculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, [1 : i64]]) : !cells
  %r = db.dynamic_map_key %rows, %string : (!cells, !strings) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsAMergeOfMaps) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %merged = db.merge_maps(%map, %map) : (!maps, !maps) -> !unresolved
  %r = db.static_map_key %merged key "a" : (!unresolved) -> !cells
)mlir"),
              Speculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsAProjectionOfANode) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %projection = db.lazy_case({%node}) {
  ^bb0(%n: !nodes):
    %held = db.is_not_null %n : (!nodes) -> !bools
    db.case_yield %held : !bools
  }, {
  ^bb0(%n: !nodes):
    %properties = db.properties(%n) : (!nodes) -> !db.column<!storage.nullable<!storage.map>>
    %merged = db.merge_maps(%properties, %map) : (!db.column<!storage.nullable<!storage.map>>, !maps) -> !unresolved
    db.case_yield %merged : !unresolved
  } : (!nodes) -> !unresolved
  %r = db.static_map_key %projection key "a" : (!unresolved) -> !cells
)mlir"),
              Speculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsASelectionOnlyWhereEveryValueHoldsKeys) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %held = db.is_not_null %map : (!maps) -> !bools
  %selected = db.case({%held}, {%map}, %node) : (!bools, !maps, !nodes) -> !unresolved
  %r = db.static_map_key %selected key "a" : (!unresolved) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %held = db.is_not_null %map : (!maps) -> !bools
  %selected = db.case({%held}, {%map}, %cell) : (!bools, !maps, !cells) -> !unresolved
  %r = db.static_map_key %selected key "a" : (!unresolved) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsTheRowsACaseCarriesIntoItsBranch) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, unit]) : !cells
  %projection = db.lazy_case({%rows}) {
  ^bb0(%x: !cells):
    %held = db.is_not_null %x : (!cells) -> !bools
    db.case_yield %held : !bools
  }, {
  ^bb0(%x: !cells):
    %v = db.static_map_key %x key "a" : (!cells) -> !cells
    db.case_yield %v : !cells
  } : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %projection = db.lazy_case({%cell}) {
  ^bb0(%x: !cells):
    %held = db.is_not_null %x : (!cells) -> !bools
    db.case_yield %held : !bools
  }, {
  ^bb0(%x: !cells):
    %v = db.static_map_key %x key "a" : (!cells) -> !cells
    db.case_yield %v : !cells
  } : (!cells) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsMapsCarriedThroughAFilter) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, unit]) : !cells
  %held = db.is_not_null %rows : (!cells) -> !bools
  %kept = db.filter(%held, {%rows}) : (!bools, !cells) -> !cells
  %r = db.static_map_key %kept key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsMapsAFactorYields) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %pairs:2 = db.cross_product factor {
    %y = db.scan_nodes() : !nodes
    db.yield %y : !nodes
  } factor {
    %x = db.unwind_const([{a = 1 : i64}, {a = "b"}]) : !cells
    db.yield %x : !cells
  }
  %r = db.static_map_key %pairs#1 key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %joined:2 = db.hash_join factor {
    %y = db.scan_nodes() : !nodes
    db.yield %y : !nodes
  } factor {
    %x = db.unwind_const([{a = 1 : i64}, unit]) : !cells
    db.yield %x : !cells
  } on 0, 0
  %r = db.static_map_key %joined#1 key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %pairs:2 = db.cross_product factor {
    %x = db.unwind_const([{a = 1 : i64}, 2 : i64]) : !cells
    db.yield %x : !cells
  } factor {
    %y = db.scan_nodes() : !nodes
    db.yield %y : !nodes
  }
  %r = db.static_map_key %pairs#0 key "a" : (!cells) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, raisesOnASelectionOfAMapAndAString) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %held = db.is_not_null %map : (!maps) -> !bools
  %selected = db.case({%held}, {%map}, %string) : (!bools, !maps, !strings) -> !unresolved
  %r = db.static_map_key %selected key "a" : (!unresolved) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsMapsCarriedPastTheRowOperators) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, unit]) : !cells
  %deduped = db.remove_duplicates(%rows) : (!cells) -> !cells
  %skipped = db.skip(%deduped) count 1 : (!cells) -> !cells
  %limited = db.limit(%skipped) count 2 : (!cells) -> !cells
  %sorted = db.sort(%limited) keys [0] ascending [true] : (!cells) -> !cells
  %grouped:2 = db.group_aggregate(%sorted, %sorted) keys 1 aggregates [count_rows] : (!cells, !cells) -> (!cells, !unresolved)
  %r = db.static_map_key %grouped#0 key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, 2 : i64]) : !cells
  %sorted = db.sort(%rows) keys [0] ascending [true] : (!cells) -> !cells
  %r = db.static_map_key %sorted key "a" : (!cells) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsMapsCarriedPastAHop) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, unit]) : !cells
  %s, %e, %et, %t, %carried = db.get_out_edges(%node, {%rows}) : (!nodes, !cells) -> (!nodes, !edges, !db.column<!storage.edge_type_id>, !nodes, !cells)
  %r = db.static_map_key %carried key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsMapsCarriedPastAnUnwind) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, unit]) : !cells
  %x, %carried = db.unwind(%string, {%rows}) : (!strings, !cells) -> (!strings, !cells)
  %r = db.static_map_key %carried key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, unit]) : !cells
  %x, %carried = db.unwind_computed({%rows}) {
  ^bb0(%in: !cells):
    %xs = db.make_list(%in) : (!cells) -> !db.column<!storage.list<!storage.any>>
    db.unwind_yield %xs : !db.column<!storage.list<!storage.any>>
  } : (!cells) -> (!cells, !cells)
  %r = db.static_map_key %carried key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsMapsCarriedIntoAndPastAnOptionalMatch) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, unit]) : !cells
  %matched:2 = db.optional_match(%rows) {
  ^bb0(%in: !cells, %tag: !db.column<ui64>):
    %pairs:3 = db.cross_product factor {
      db.yield %in, %tag : !cells, !db.column<ui64>
    } factor {
      %n = db.scan_nodes() : !nodes
      db.yield %n : !nodes
    }
    db.optional_yield %pairs#1, {%pairs#0, %pairs#2} : {!cells, !nodes}
  } : (!cells) -> (!cells, !nodes)
  %r = db.static_map_key %matched#0 key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, 2 : i64]) : !cells
  %matched:2 = db.optional_match(%rows) {
  ^bb0(%in: !cells, %tag: !db.column<ui64>):
    %pairs:3 = db.cross_product factor {
      db.yield %in, %tag : !cells, !db.column<ui64>
    } factor {
      %n = db.scan_nodes() : !nodes
      db.yield %n : !nodes
    }
    db.optional_yield %pairs#1, {%pairs#0, %pairs#2} : {!cells, !nodes}
  } : (!cells) -> (!cells, !nodes)
  %r = db.static_map_key %matched#0 key "a" : (!cells) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsMapsCarriedIntoAndPastACallSubquery) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, unit]) : !cells
  %called:2 = db.call_subquery(%rows) carries_scope {
  ^bb0(%in: !cells):
    %v = db.static_map_key %in key "a" : (!cells) -> !cells
    db.subquery_yield {%in, %v} : {!cells, !cells}
  } : (!cells) -> (!cells, !cells)
  %r = db.static_map_key %called#0 key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsMapsEveryUnionBranchYields) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %called = db.call_subquery() {
    %union = db.union {
      %x = db.unwind_const([{a = 1 : i64}, unit]) : !cells
      db.yield %x : !cells
    }, {
      %y = db.unwind_const([{a = 2 : i64}, unit]) : !cells
      db.yield %y : !cells
    } : !cells
    db.subquery_yield {%union} : {!cells}
  } : () -> !cells
  %r = db.static_map_key %called key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %called = db.call_subquery() {
    %union = db.union {
      %x = db.unwind_const([{a = 1 : i64}, unit]) : !cells
      db.yield %x : !cells
    }, {
      %y = db.unwind_const([2 : i64, unit]) : !cells
      db.yield %y : !cells
    } : !cells
    db.subquery_yield {%union} : {!cells}
  } : () -> !cells
  %r = db.static_map_key %called key "a" : (!cells) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsTheMapsAnUnwoundListHolds) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %x, %carried = db.unwind_computed({%node}) {
  ^bb0(%in: !nodes):
    %name = db.get_node_properties(%in, "name") : (!nodes) -> !db.column<!storage.nullable<!storage.string>>
    %m = db.make_map(%name) keys ["a"] : !db.column<!storage.nullable<!storage.string>>
    %xs = db.make_list(%m) : (!maps) -> !db.column<!storage.list<!storage.map>>
    db.unwind_yield %xs : !db.column<!storage.list<!storage.map>>
  } : (!nodes) -> (!cells, !nodes)
  %r = db.static_map_key %x key "a" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %x, %carried = db.unwind_computed({%node}) {
  ^bb0(%in: !nodes):
    %xs = db.make_list(%in, %string) : (!nodes, !strings) -> !db.column<!storage.list<!storage.any>>
    db.unwind_yield %xs : !db.column<!storage.list<!storage.any>>
  } : (!nodes) -> (!cells, !nodes)
  %r = db.static_map_key %x key "a" : (!cells) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsAMapHeldByAConstantMap) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %m = db.constant({d = {e = 1 : i64}})
  %d = db.static_map_key %m key "d" : (!maps) -> !cells
  %laid = db.broadcast_constant(%d, %node) : (!cells, !nodes) -> !cells
  %r = db.static_map_key %laid key "e" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %m = db.constant({f = {e = 1 : i64}})
  %d = db.static_map_key %m key "d" : (!maps) -> !cells
  %r = db.static_map_key %d key "e" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %m = db.constant({d = 1 : i64})
  %d = db.static_map_key %m key "d" : (!maps) -> !cells
  %r = db.static_map_key %d key "e" : (!cells) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsAMapHeldByAnUnwoundMap) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{s = {name = "Remy" : !storage.string}}, {t = 1 : i64}, unit]) : !cells
  %s = db.static_map_key %rows key "s" : (!cells) -> !cells
  %r = db.static_map_key %s key "name" : (!cells) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{s = {name = "Remy" : !storage.string}}, unit]) : !cells
  %s = db.static_map_key %rows key "s" : (!cells) -> !cells
  %r = db.dynamic_map_key %s, %string : (!cells, !strings) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %rows = db.unwind_const([{s = {name = "Remy" : !storage.string}}, {s = "x" : !storage.string}]) : !cells
  %s = db.static_map_key %rows key "s" : (!cells) -> !cells
  %r = db.static_map_key %s key "name" : (!cells) -> !cells
)mlir"),
              NotSpeculatable);
}

TEST_F(MapKeyReadSpeculatabilityTest, readsAMapAListElementHolds) {
    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %lists = db.unwind_const([[{a = "Remy" : !storage.string}], [unit]]) : !db.column<!storage.list<!storage.any>>
  %zero = db.constant(0 : i64)
  %x = db.list_index %lists, %zero : (!db.column<!storage.list<!storage.any>>, !db.column<i64>) -> !unresolved
  %r = db.static_map_key %x key "a" : (!unresolved) -> !cells
)mlir"),
              Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
  %lists = db.unwind_const([[{a = "Remy" : !storage.string}], [1 : i64]]) : !db.column<!storage.list<!storage.any>>
  %zero = db.constant(0 : i64)
  %x = db.list_index %lists, %zero : (!db.column<!storage.list<!storage.any>>, !db.column<i64>) -> !unresolved
  %r = db.static_map_key %x key "a" : (!unresolved) -> !cells
)mlir"),
              NotSpeculatable);
}
