#include <gtest/gtest.h>

#include <stddef.h>
#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/StringRef.h"

#include "DBDialect.h"
#include "DBInterfaces.h"
#include "DBOps.h"
#include "StorageDialect.h"

// A db.cross_product and a db.hash_join hand out each factor's yield in turn, a returning
// db.call_subquery its body's yield after the inputs it hands back or from its first result
// when the body carries its scope, and an optional one none row for row
class YieldedResultsTest : public ::testing::Test {
protected:
    struct ExpectedPlace {
        bool _yielded {false};
        unsigned _region {0};
        size_t _position {0};
    };

    YieldedResultsTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::Operation* parseOp(const char* body, llvm::StringRef opName) {
        const std::string program = std::string(TYPES) + "func.func @main() {\n" + body + "  return\n}\n";
        _module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        if (!_module) {
            return nullptr;
        }

        mlir::Operation* found = nullptr;
        _module->walk([&found, opName](mlir::Operation* op) {
            if (op->getName().getStringRef() == opName) {
                found = op;
            }
        });

        return found;
    }

    void expectYieldedResults(mlir::Operation* op, llvm::ArrayRef<ExpectedPlace> expected) {
        ASSERT_EQ(op->getNumResults(), expected.size());

        for (size_t resultIndex = 0; resultIndex < expected.size(); resultIndex++) {
            const mlir::OpResult result = op->getResult(static_cast<unsigned>(resultIndex));
            const ExpectedPlace& place = expected[resultIndex];

            mlir::Region* region = nullptr;
            size_t position = 0;
            EXPECT_EQ(mlir::db::matchYieldedResult(result, region, position), place._yielded) << resultIndex;
            EXPECT_EQ(mlir::db::yieldingRegionOf(result), region) << resultIndex;

            if (!place._yielded) {
                EXPECT_FALSE(mlir::db::yieldedColumnOf(result)) << resultIndex;
                continue;
            }

            mlir::Block& body = region->front();
            const mlir::OperandRange yielded = mlir::db::yieldedColumnsOf(body);

            EXPECT_EQ(region, &op->getRegion(place._region)) << resultIndex;
            EXPECT_EQ(position, place._position) << resultIndex;
            EXPECT_EQ(mlir::db::yieldedResultsOf(*region)[position], result) << resultIndex;
            EXPECT_EQ(mlir::db::yieldedColumnOf(result), yielded[position]) << resultIndex;
        }
    }

    static constexpr const char* TYPES = R"mlir(
!nodes = !db.column<!storage.node_id>
!cells = !db.column<!storage.any>
)mlir";

    mlir::MLIRContext _context;
    mlir::OwningOpRef<mlir::ModuleOp> _module;
};

TEST_F(YieldedResultsTest, handsOutEachFactorOfAProductInTurn) {
    mlir::Operation* const product = parseOp(R"mlir(
  %pairs:5 = db.cross_product factor {
    %x = db.scan_nodes() : !nodes
    db.yield %x, %x : !nodes, !nodes
  } factor {
    %y = db.scan_nodes() : !nodes
    db.yield %y, %y, %y : !nodes, !nodes, !nodes
  }
)mlir", "db.cross_product");
    ASSERT_TRUE(product);

    expectYieldedResults(product, {{true, 0, 0}, {true, 0, 1}, {true, 1, 0}, {true, 1, 1}, {true, 1, 2}});
    EXPECT_EQ(mlir::db::yieldedResultsOf(product->getRegion(0)).size(), 2U);
    EXPECT_EQ(mlir::db::yieldedResultsOf(product->getRegion(1)).size(), 3U);
}

TEST_F(YieldedResultsTest, handsOutEachFactorOfAHashJoinInTurn) {
    mlir::Operation* const join = parseOp(R"mlir(
  %joined:3 = db.hash_join factor {
    %x = db.scan_nodes() : !nodes
    db.yield %x : !nodes
  } factor {
    %y = db.scan_nodes() : !nodes
    db.yield %y, %y : !nodes, !nodes
  } on 0, 1
)mlir", "db.hash_join");
    ASSERT_TRUE(join);

    expectYieldedResults(join, {{true, 0, 0}, {true, 1, 0}, {true, 1, 1}});
}

TEST_F(YieldedResultsTest, handsOutTheYieldOfACallCarryingItsScopeFromItsFirstResult) {
    mlir::Operation* const call = parseOp(R"mlir(
  %rows = db.unwind_const([1 : i64, 2 : i64]) : !cells
  %called:2 = db.call_subquery(%rows) carries_scope {
  ^bb0(%in: !cells):
    db.subquery_yield {%in, %in} : {!cells, !cells}
  } : (!cells) -> (!cells, !cells)
)mlir", "db.call_subquery");
    ASSERT_TRUE(call);

    expectYieldedResults(call, {{true, 0, 0}, {true, 0, 1}});
}

TEST_F(YieldedResultsTest, handsOutTheYieldOfACallRunPerRowAfterItsInputs) {
    mlir::Operation* const call = parseOp(R"mlir(
  %rows = db.unwind_const([1 : i64, 2 : i64]) : !cells
  %called:3 = db.call_subquery(%rows) {
  ^bb0(%in: !cells):
    %x = db.unwind_const([3 : i64, 4 : i64]) : !cells
    db.subquery_yield {%x, %x} : {!cells, !cells}
  } : (!cells) -> (!cells, !cells, !cells)
)mlir", "db.call_subquery");
    ASSERT_TRUE(call);

    expectYieldedResults(call, {{false}, {true, 0, 0}, {true, 0, 1}});
}

TEST_F(YieldedResultsTest, handsOutNoYieldOfAnOptionalCallRowForRow) {
    mlir::Operation* const call = parseOp(R"mlir(
  %rows = db.unwind_const([1 : i64, 2 : i64]) : !cells
  %called:2 = db.call_subquery(%rows) optional {
  ^bb0(%in: !cells):
    %x = db.unwind_const([3 : i64]) : !cells
    db.subquery_yield {%x} : {!cells}
  } : (!cells) -> (!cells, !cells)
)mlir", "db.call_subquery");
    ASSERT_TRUE(call);

    expectYieldedResults(call, {{false}, {false}});
    EXPECT_TRUE(mlir::db::yieldedResultsOf(call->getRegion(0)).empty());
}
