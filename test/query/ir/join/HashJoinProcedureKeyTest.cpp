#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBOps.h"

#include "HashJoinQueryTest.h"
#include "IRTestOps.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

// A join keyed on the nodes two procedure calls yield, on the shared SimpleGraph. db.getNodes
// yields each node it is listed, so the join keeps the node both lists name.
class HashJoinProcedureKeyTest : public HashJoinQueryTest {
};

TEST_F(HashJoinProcedureKeyTest, joinsTheNodesTwoCallsYield) {
    const std::string_view query = "CALL db.getNodes([0, 1]) YIELD id AS a "
                                   "CALL db.getNodes([1, 2]) YIELD id AS b "
                                   "WHERE a = b RETURN a";

    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(query, module);
    EXPECT_EQ(countOps<mlir::db::HashJoin>(module.get()), 1u);

    StringRowSink joined;
    runQuery(query, joined);
    const Rows expected {{"1"}};
    EXPECT_EQ(joined.getRows(), expected);

    StringRowSink product;
    runQuery(query, product, false);
    EXPECT_EQ(product.getRows(), expected);
}
