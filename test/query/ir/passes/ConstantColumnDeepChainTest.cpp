#include <gtest/gtest.h>

#include <stddef.h>
#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "IRConstantColumn.h"
#include "StorageDialect.h"

class ConstantColumnDeepChainTest : public ::testing::Test {
protected:
    static constexpr size_t CHAIN_LENGTH = 250000;

    ConstantColumnDeepChainTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parseDisjunction(const std::string& firstOperand, const std::string& firstOperandType) {
        std::string program = "func.func @main() {\n";
        program += "  %n = db.scan_nodes() : !db.column<!storage.node_id>\n";
        program += "  %age = db.get_node_properties(%n, \"age\") : (!db.column<!storage.node_id>) -> !db.column<none>\n";
        program += "  %k = db.constant(0 : i64)\n";
        program += "  %m0 = db.eq " + firstOperand + ", %k : (" + firstOperandType
                 + ", !db.column<i64>) -> !db.column<!storage.bool>\n";

        for (size_t link = 1; link <= CHAIN_LENGTH; link++) {
            const std::string index = std::to_string(link);
            const std::string previous = std::to_string(link - 1);

            program += "  %c" + index + " = db.constant(" + index + " : i64)\n";
            program += "  %e" + index + " = db.eq %k, %c" + index
                     + " : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>\n";
            program += "  %m" + index + " = db.or %m" + previous + ", %e" + index
                     + " : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>\n";
        }

        program += "  %nf = db.filter(%m" + std::to_string(CHAIN_LENGTH)
                 + ", {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n";
        program += "  db.output(%nf) : !db.column<!storage.node_id>\n  return\n}\n";

        return mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
    }

    mlir::Value getFilterPredicate(mlir::ModuleOp module) {
        mlir::db::FilterOp filter;
        module.walk([&filter](mlir::db::FilterOp op) { filter = op; });

        return filter.getMask();
    }

    mlir::MLIRContext _context;
};

TEST_F(ConstantColumnDeepChainTest, classifiesALongDisjunctionOverConstants) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseDisjunction("%k", "!db.column<i64>");
    ASSERT_TRUE(module);

    EXPECT_TRUE(db::yieldsConstantColumn(getFilterPredicate(*module)));
}

TEST_F(ConstantColumnDeepChainTest, classifiesALongDisjunctionOverAPropertyRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseDisjunction("%age", "!db.column<none>");
    ASSERT_TRUE(module);

    EXPECT_FALSE(db::yieldsConstantColumn(getFilterPredicate(*module)));
}
