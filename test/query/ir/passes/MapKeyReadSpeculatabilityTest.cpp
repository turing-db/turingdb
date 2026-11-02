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
    %properties = db.properties(%n) : (!nodes) -> !unresolved
    %merged = db.merge_maps(%properties, %map) : (!unresolved, !maps) -> !unresolved
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
