#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "StorageDialect.h"

namespace {

// MATCH (n) RETURN CASE WHEN n.age = 0 THEN <value> ELSE <else> END, the value region ending in
// @param valueYield and the ELSE region in @param elseYield, with no ELSE where it is empty
void writeCaseProgram(std::string_view valueYield, std::string_view elseYield, std::string& program) {
    const std::string regionArguments = "  ^bb0(%ageIn: !db.column<!storage.nullable<i64>>):\n";

    program = "func.func @main(%x: i64) {\n"
              "  %n = db.scan_nodes() : !db.column<!storage.node_id>\n"
              "  %age = db.get_node_properties(%n, \"age\") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>\n"
              "  %name = db.get_node_properties(%n, \"name\") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>\n"
              "  %zero = db.constant(0 : i64)\n"
              "  %r = db.lazy_case({%age}) {\n"
              + regionArguments
              + "    %c = db.eq %ageIn, %zero : (!db.column<!storage.nullable<i64>>, !db.column<i64>) -> !db.column<!storage.bool>\n"
                "    db.case_yield %c : !db.column<!storage.bool>\n"
                "  }, {\n"
              + regionArguments + "    " + std::string(valueYield) + "\n";

    if (!elseYield.empty()) {
        program += "  }, {\n" + regionArguments + "    " + std::string(elseYield) + "\n";
    }

    program += "  } : (!db.column<!storage.nullable<i64>>) -> !db.column<!storage.nullable<i64>>\n"
               "  db.output(%r) : !db.column<!storage.nullable<i64>>\n"
               "  return\n"
               "}\n";
}

}

// A db.lazy_case selects the columns its regions yield: a db.case_yield holding no column is
// turned away before the case reads it, and a value of another type than the result by the case.
class LazyCaseYieldTest : public ::testing::Test {
protected:
    LazyCaseYieldTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    bool verifiesTheCase(std::string_view valueYield, std::string_view elseYield) {
        std::string program;
        writeCaseProgram(valueYield, elseYield, program);

        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        const mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program,
                                                                                                mlir::ParserConfig(&_context));

        return static_cast<bool>(module);
    }

    mlir::MLIRContext _context;
};

TEST_F(LazyCaseYieldTest, acceptsAYieldOfAColumn) {
    EXPECT_TRUE(verifiesTheCase("db.case_yield %zero : !db.column<i64>", ""));
}

TEST_F(LazyCaseYieldTest, rejectsAYieldOfNothing) {
    EXPECT_FALSE(verifiesTheCase("\"db.case_yield\"() : () -> ()", ""));
}

TEST_F(LazyCaseYieldTest, rejectsAYieldOfAValueNoColumnHolds) {
    EXPECT_FALSE(verifiesTheCase("\"db.case_yield\"(%x) : (i64) -> ()", ""));
}

TEST_F(LazyCaseYieldTest, rejectsAValueOfAnotherTypeThanTheResult) {
    EXPECT_FALSE(verifiesTheCase("db.case_yield %name : !db.column<!storage.nullable<!storage.string>>", ""));
}

TEST_F(LazyCaseYieldTest, acceptsAnElseOfTheResultType) {
    EXPECT_TRUE(verifiesTheCase("db.case_yield %zero : !db.column<i64>", "db.case_yield %zero : !db.column<i64>"));
}

TEST_F(LazyCaseYieldTest, rejectsAnElseYieldOfNothing) {
    EXPECT_FALSE(verifiesTheCase("db.case_yield %zero : !db.column<i64>", "\"db.case_yield\"() : () -> ()"));
}

TEST_F(LazyCaseYieldTest, rejectsAnElseOfAnotherTypeThanTheResult) {
    EXPECT_FALSE(verifiesTheCase("db.case_yield %zero : !db.column<i64>",
                                 "db.case_yield %name : !db.column<!storage.nullable<!storage.string>>"));
}
