#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "NLDialect.h"
#include "StorageDialect.h"

class ComputedColumnTypeVerifierTest : public ::testing::Test {
protected:
    ComputedColumnTypeVerifierTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    bool parses(std::string_view operation, std::string& diagnostics) {
        const std::string program = "module {\n"
                                    "  func.func @main(%integer: !db.column<i64>,\n"
                                    "                  %text: !db.column<!storage.string>,\n"
                                    "                  %unresolved: !db.column<none>) {\n"
                                    "    " + std::string(operation) + "\n"
                                    "    return\n"
                                    "  }\n"
                                    "}\n";

        diagnostics.clear();
        const mlir::ScopedDiagnosticHandler handler(&_context, [&diagnostics](mlir::Diagnostic& diagnostic) {
            diagnostics += diagnostic.str();
            diagnostics += '\n';
            return mlir::success();
        });

        const mlir::OwningOpRef<mlir::ModuleOp> module =
            mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));

        return static_cast<bool>(module);
    }

    mlir::MLIRContext _context;
};

TEST_F(ComputedColumnTypeVerifierTest, acceptsTheColumnItsOperandsGive) {
    std::string diagnostics;

    EXPECT_TRUE(parses("%r = db.add %integer, %integer : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.nullable<i64>>",
                       diagnostics)) << diagnostics;
    EXPECT_TRUE(parses("%r = db.to_upper(%text) : (!db.column<!storage.string>) -> !db.column<!storage.nullable<!storage.string>>",
                       diagnostics)) << diagnostics;
    EXPECT_TRUE(parses("%r = db.add %unresolved, %integer : (!db.column<none>, !db.column<i64>) -> !db.column<none>",
                       diagnostics)) << diagnostics;
}

TEST_F(ComputedColumnTypeVerifierTest, acceptsTheColumnWithoutItsNull) {
    std::string diagnostics;

    EXPECT_TRUE(parses("%r = db.add %integer, %integer : (!db.column<i64>, !db.column<i64>) -> !db.column<i64>",
                       diagnostics)) << diagnostics;
}

TEST_F(ComputedColumnTypeVerifierTest, refusesAColumnItsOperandsDoNotGive) {
    std::string diagnostics;

    EXPECT_FALSE(parses("%r = db.add %integer, %integer : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.string>",
                        diagnostics));
    EXPECT_NE(diagnostics.find("inferred type(s)"), std::string::npos) << diagnostics;

    EXPECT_FALSE(parses("%r = db.to_upper(%text) : (!db.column<!storage.string>) -> !db.column<i64>",
                        diagnostics));
    EXPECT_NE(diagnostics.find("inferred type(s)"), std::string::npos) << diagnostics;

    EXPECT_FALSE(parses("%r = db.add %unresolved, %integer : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.nullable<i64>>",
                        diagnostics));
    EXPECT_NE(diagnostics.find("inferred type(s)"), std::string::npos) << diagnostics;
}

TEST_F(ComputedColumnTypeVerifierTest, refusesNoTypeWhereTheOperandsGiveOne) {
    std::string diagnostics;

    EXPECT_FALSE(parses("%r = db.add %integer, %integer : (!db.column<i64>, !db.column<i64>) -> !db.column<none>",
                        diagnostics));
    EXPECT_NE(diagnostics.find("inferred type(s)"), std::string::npos) << diagnostics;
}
