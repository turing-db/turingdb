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

// Which keys fuse_hash_join trusts to serialize alike. The join matches keys by their
// bytes, so the two sides must carry one runtime column type: a key that is a column or a
// property read carries the type its db column declares, a computed one the type lowering
// derives from its operands. With no graph in the pass context, every match it trusts fuses.
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

TEST_F(HashJoinKeyTypeTest, fusesTwoPropertiesOfOneType) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoStringProperties);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuseHashJoin(*module));

    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
}

TEST_F(HashJoinKeyTypeTest, keepsTheProductOverPropertiesOfTwoTypes) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(propertiesOfTwoTypes);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuseHashJoin(*module));

    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}

TEST_F(HashJoinKeyTypeTest, fusesAPropertyReadAgainstAYieldedPropertyOfItsType) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(propertyReadAgainstAYieldedProperty);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuseHashJoin(*module));

    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
}

TEST_F(HashJoinKeyTypeTest, keepsTheProductOverTwoComputedKeys) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoComparisons);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuseHashJoin(*module));

    EXPECT_EQ(countOps<mlir::db::HashJoin>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}
