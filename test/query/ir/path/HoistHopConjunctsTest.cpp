#include <gtest/gtest.h>

#include <memory>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "NLDialect.h"
#include "StorageDialect.h"

namespace {

// MATCH (n)((x)-[r]->(y)-[s]->(z) WHERE x.age > 3 AND y.age = z.age AND n = n){1,2}(m) as
// codegen writes it: the whole predicate in the region of the last step, the seed imported
const char* const lastStepProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) [forward, forward] hops 1 to 2 hop_imports{%n} {}, {
  ^bb0(%x: !db.column<!storage.node_id>, %r: !db.column<!storage.edge_id>, %y: !db.column<!storage.node_id>, %s: !db.column<!storage.edge_id>, %z: !db.column<!storage.node_id>, %seed: !db.column<!storage.node_id>):
    %xAge = db.get_node_properties(%x, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
    %three = db.constant(3 : i64)
    %first = db.gt %xAge, %three : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
    %yAge = db.get_node_properties(%y, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
    %zAge = db.get_node_properties(%z, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
    %second = db.eq %yAge, %zAge : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
    %seeded = db.eq %seed, %seed : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
    %both = db.and %first, %second : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
    %all = db.and %both, %seeded : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
    db.yield %all : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

// The same body testing only its first node: nothing is left for the last step
const char* const firstNodeProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) [forward, forward] hops 1 to 2 {}, {
  ^bb0(%x: !db.column<!storage.node_id>, %r: !db.column<!storage.edge_id>, %y: !db.column<!storage.node_id>, %s: !db.column<!storage.edge_id>, %z: !db.column<!storage.node_id>):
    %xAge = db.get_node_properties(%x, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
    %three = db.constant(3 : i64)
    %first = db.gt %xAge, %three : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
    db.yield %first : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

}

class HoistHopConjunctsTest : public ::testing::Test {
protected:
    HoistHopConjunctsTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    void runPass(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createHoistHopConjuncts());
        ASSERT_TRUE(mlir::succeeded(passManager.run(module)));
        EXPECT_TRUE(mlir::succeeded(mlir::verify(module)));
    }

    static mlir::db::ExplorePaths findExplorePaths(mlir::ModuleOp module) {
        mlir::db::ExplorePaths found;
        module.walk([&found](mlir::db::ExplorePaths op) {
            found = op;
        });

        return found;
    }

    template <typename Op>
    static size_t countOps(mlir::Region& region) {
        size_t count = 0;
        region.walk([&count](Op) {
            count++;
        });

        return count;
    }

    mlir::MLIRContext _context;
};

TEST_F(HoistHopConjunctsTest, movesEachConjunctToTheFirstStepBindingWhatItReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(lastStepProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths exploration = findExplorePaths(*module);
    ASSERT_TRUE(exploration);

    mlir::MutableArrayRef<mlir::Region> hops = exploration.getHops();
    ASSERT_FALSE(hops[0].empty());
    ASSERT_FALSE(hops[1].empty());

    EXPECT_EQ(countOps<mlir::db::GtOp>(hops[0]), 1u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(hops[0]), 1u);
    EXPECT_EQ(countOps<mlir::db::AndOp>(hops[0]), 1u);

    EXPECT_EQ(countOps<mlir::db::GtOp>(hops[1]), 0u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(hops[1]), 1u);
    EXPECT_EQ(countOps<mlir::db::AndOp>(hops[1]), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(hops[1]), 2u);
}

TEST_F(HoistHopConjunctsTest, dropsTheLastStepsRegionWhenEveryConjunctMoves) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(firstNodeProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths exploration = findExplorePaths(*module);
    ASSERT_TRUE(exploration);

    mlir::MutableArrayRef<mlir::Region> hops = exploration.getHops();
    ASSERT_FALSE(hops[0].empty());
    EXPECT_TRUE(hops[1].empty());
    EXPECT_EQ(countOps<mlir::db::GtOp>(hops[0]), 1u);
}
