#include <gtest/gtest.h>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "StorageDialect.h"

#include "IRTestOps.h"

using namespace turing::test;

// Which keys fuse_hash_join joins on. Each side hashes by its own runtime type and a probe
// key is compared to a build key as `=` compares them, so the two need not share a type -
// one the db level types none included. With no graph in the pass context, every match fuses.
class HashJoinKeyTypeTest : public ::testing::Test {
protected:
    HashJoinKeyTypeTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runFuseHashJoin(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseHashJoin(&_passContext));

        return mlir::succeeded(passManager.run(module));
    }

    mlir::MLIRContext _context;
    mlir::db::DBPassContext _passContext;
};

// MATCH (n), (m) WHERE n.name = m.dob RETURN n, m
const char* const twoStringProperties = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %n = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %n : !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %m : !db.column<!storage.node_id>
  }
  %name = db.get_node_properties(%p#0, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  %dob = db.get_node_properties(%p#1, "dob") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  %eq = db.eq %name, %dob : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>) -> !db.column<!storage.bool>
  %n, %m = db.filter(%eq, {%p#0, %p#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%n, %m) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// An integer property against a double one
const char* const propertiesOfTwoTypes = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %n = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %n : !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %m : !db.column<!storage.node_id>
  }
  %age = db.get_node_properties(%p#0, "age") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
  %ratio = db.get_node_properties(%p#1, "ratio") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<f64>>
  %eq = db.eq %age, %ratio : (!db.column<!storage.nullable<i64>>, !db.column<!storage.nullable<f64>>) -> !db.column<!storage.bool>
  %n, %m = db.filter(%eq, {%p#0, %p#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%n, %m) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The right factor yields m.dob itself, so its key is a product column and the left one a
// property read over the product
const char* const propertyReadAgainstAYieldedProperty = R"mlir(
func.func @main() {
  %p:3 = db.cross_product factor {
    %n = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %n : !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    %dob = db.get_node_properties(%m, "dob") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.yield %m, %dob : !db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>
  }
  %name = db.get_node_properties(%p#0, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  %eq = db.eq %name, %p#2 : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>) -> !db.column<!storage.bool>
  %n, %m = db.filter(%eq, {%p#0, %p#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%n, %m) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n), (m) WHERE (n.age > 30) = (m.age > 30) RETURN n, m: both keys declare a bool
// column, but lowering types a comparison from its operands, nullable or not
const char* const twoComparisons = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %n = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %n : !db.column<!storage.node_id>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %m : !db.column<!storage.node_id>
  }
  %nAge = db.get_node_properties(%p#0, "age") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
  %nLimit = db.constant(30 : i64)
  %nOld = db.gt %nAge, %nLimit : (!db.column<!storage.nullable<i64>>, !db.column<i64>) -> !db.column<!storage.bool>
  %mAge = db.get_node_properties(%p#1, "age") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
  %mLimit = db.constant(30 : i64)
  %mOld = db.gt %mAge, %mLimit : (!db.column<!storage.nullable<i64>>, !db.column<i64>) -> !db.column<!storage.bool>
  %eq = db.eq %nOld, %mOld : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %n, %m = db.filter(%eq, {%p#0, %p#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%n, %m) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// UNWIND [[32, 1], [20, 2]] AS p MATCH (n) WHERE n.age = p[0] RETURN n: the element is
// typed none until lowering reads the list it is taken from
const char* const listElementAgainstAProperty = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %pairs = db.unwind_const([[32, 1], [20, 2]]) : !db.column<!storage.list<i64>>
    db.yield %pairs : !db.column<!storage.list<i64>>
  } factor {
    %n = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %n : !db.column<!storage.node_id>
  }
  %age = db.get_node_properties(%p#1, "age") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
  %zero = db.constant(0 : i64)
  %first = db.list_index %p#0, %zero : (!db.column<!storage.list<i64>>, !db.column<i64>) -> !db.column<none>
  %eq = db.eq %age, %first : (!db.column<!storage.nullable<i64>>, !db.column<none>) -> !db.column<!storage.bool>
  %n = db.filter(%eq, {%p#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%n) : !db.column<!storage.node_id>
  return
}
)mlir";

// A path has no value to hash: it is compared as the nodes and edges it runs through
const char* const twoPaths = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %n = db.scan_nodes() : !db.column<!storage.node_id>
    %path = db.make_path(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.path>
    db.yield %path : !db.column<!storage.path>
  } factor {
    %m = db.scan_nodes() : !db.column<!storage.node_id>
    %path = db.make_path(%m) : (!db.column<!storage.node_id>) -> !db.column<!storage.path>
    db.yield %path : !db.column<!storage.path>
  }
  %eq = db.eq %p#0, %p#1 : (!db.column<!storage.path>, !db.column<!storage.path>) -> !db.column<!storage.bool>
  %n, %m = db.filter(%eq, {%p#0, %p#1}) : (!db.column<!storage.bool>, !db.column<!storage.path>, !db.column<!storage.path>) -> (!db.column<!storage.path>, !db.column<!storage.path>)
  db.output(%n, %m) : !db.column<!storage.path>, !db.column<!storage.path>
  return
}
)mlir";

TEST_F(HashJoinKeyTypeTest, fusesTwoPropertiesOfOneType) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoStringProperties);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuseHashJoin(*module));

    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
}

TEST_F(HashJoinKeyTypeTest, fusesPropertiesOfTwoTypes) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(propertiesOfTwoTypes);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuseHashJoin(*module));

    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
}

TEST_F(HashJoinKeyTypeTest, fusesAPropertyReadAgainstAYieldedPropertyOfItsType) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(propertyReadAgainstAYieldedProperty);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuseHashJoin(*module));

    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
}

TEST_F(HashJoinKeyTypeTest, fusesTwoComputedKeys) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoComparisons);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuseHashJoin(*module));

    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
}

TEST_F(HashJoinKeyTypeTest, fusesAKeyTypedNoneAgainstAProperty) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(listElementAgainstAProperty);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuseHashJoin(*module));

    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
}

TEST_F(HashJoinKeyTypeTest, keepsTheProductOverTwoPaths) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoPaths);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuseHashJoin(*module));

    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}
