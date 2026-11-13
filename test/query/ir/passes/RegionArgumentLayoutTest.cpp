#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "StorageDialect.h"

#include "IRTestOps.h"

using namespace turing::test;

using mlir::Speculation::NotSpeculatable;
using mlir::Speculation::Speculatable;

// The readers of a region argument take what it holds from the op's region carry set and the
// columns an earlier region forwards, never from the op's carried results
class RegionArgumentLayoutTest : public ::testing::Test {
protected:
    RegionArgumentLayoutTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(std::string_view programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runTrim(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createTrimUnreadColumns());

        return mlir::succeeded(passManager.run(module)) && mlir::succeeded(mlir::verify(module));
    }

    mlir::Speculation::Speculatability speculatabilityOfTheKeyRead(std::string_view forwarding) {
        std::string program = R"mlir(
!strings = !db.column<!storage.string>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>
!maps = !db.column<!storage.map>
!bools = !db.column<!storage.bool>

func.func @main(%map: !maps, %string: !strings) {
  %projection = db.lazy_case({%map}) {
  ^bb0(%m: !maps):
)mlir";

        program += forwarding;
        program += R"mlir(
    %forwardedHeld = db.is_not_null %forwarded : (!unresolved) -> !bools
    db.case_yield %forwardedHeld : !bools forwarding(%forwarded : !unresolved)
  }, {
  ^bb0(%forwardedIn: !unresolved, %m: !maps):
    %v = db.static_map_key %forwardedIn key "a" : (!unresolved) -> !cells
    db.case_yield %v : !cells
  } : (!maps) -> !cells
  return
}
)mlir";

        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        EXPECT_TRUE(module) << program;
        if (!module) {
            return NotSpeculatable;
        }

        llvm::SmallVector<mlir::db::StaticMapKey> keyReads = collect<mlir::db::StaticMapKey>(*module);
        EXPECT_EQ(keyReads.size(), 1u);
        if (keyReads.empty()) {
            return NotSpeculatable;
        }

        return mlir::cast<mlir::ConditionallySpeculatable>(keyReads.front().getOperation()).getSpeculatability();
    }

    mlir::MLIRContext _context;
};

// MATCH (n)-[e*1..3 WHERE e.since > n.age]->(m) WITH n.name AS name, m RETURN m
const char* const walkImportingAColumnBesideItsCarrySet = R"mlir(
!nodes = !db.column<!storage.node_id>
!edges = !db.column<!storage.edge_id>
!names = !db.column<!storage.nullable<!storage.string>>
!ages = !db.column<!storage.nullable<i64>>
!paths = !db.column<!storage.path_ref>
!bools = !db.column<!storage.bool>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %name = db.get_node_properties(%n, "name") : (!nodes) -> !names
  %age = db.get_node_properties(%n, "age") : (!nodes) -> !ages
  %s, %t, %p, %namec = db.explore_paths(%n, {%name}) forward hops 1 to 3 hop_imports{%age} {
  ^bb0(%source: !nodes, %edge: !edges, %target: !nodes, %ageIn: !ages):
    %since = db.get_edge_properties(%edge, "since") : (!edges) -> !ages
    %later = db.gt %since, %ageIn : (!ages, !ages) -> !bools
    db.yield %later : !bools
  } : (!nodes, !names, !ages) -> (!nodes, !nodes, !paths, !names)
  db.output(%t) : !nodes
  return
}
)mlir";

TEST_F(RegionArgumentLayoutTest, trimsTheCarrySetOfAWalkBesideTheColumnsItImports) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(walkImportingAColumnBesideItsCarrySet);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));

    llvm::SmallVector<mlir::db::ExplorePaths> walks = collect<mlir::db::ExplorePaths>(*module);
    ASSERT_EQ(walks.size(), 1u);
    mlir::db::ExplorePaths walk = walks.front();

    EXPECT_TRUE(walk.getColumnsToFilter().empty());
    EXPECT_EQ(walk.getHopImports().size(), 1u);
    EXPECT_EQ(walk->getNumResults(), 3u);
    EXPECT_EQ(collect<mlir::db::GetNodeProperties>(*module).size(), 1u);
}

TEST_F(RegionArgumentLayoutTest, readsAKeyOffAColumnAnEarlierRegionForwards) {
    EXPECT_EQ(speculatabilityOfTheKeyRead("%forwarded = db.merge_maps(%m, %m) : (!maps, !maps) -> !unresolved"), Speculatable);

    EXPECT_EQ(speculatabilityOfTheKeyRead(R"mlir(
    %held = db.is_not_null %m : (!maps) -> !bools
    %forwarded = db.case({%held}, {%m}, %string) : (!bools, !maps, !strings) -> !unresolved)mlir"),
              NotSpeculatable);
}
