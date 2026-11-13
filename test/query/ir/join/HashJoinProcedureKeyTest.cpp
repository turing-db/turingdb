#include <gtest/gtest.h>

#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBOps.h"

#include "HashJoinQueryTest.h"
#include "IRTestOps.h"

using namespace db;
using namespace turing::test;

class HashJoinProcedureKeyTest : public HashJoinQueryTest {
};

TEST_F(HashJoinProcedureKeyTest, joinsTheNodesTwoCallsYield) {
    const std::string_view query = "CALL db.getNodes([0, 1]) YIELD id AS a "
                                   "CALL db.getNodes([1, 2]) YIELD id AS b "
                                   "WHERE a = b RETURN a";

    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(query, module);
    EXPECT_EQ(countOps<mlir::db::HashJoin>(module.get()), 1u);

    expectJoinedRows(query, {{"1"}});
}
