#include <gtest/gtest.h>

#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "StorageDialect.h"

namespace {

// MATCH (n) RETURN CASE WHEN n.age > 30 THEN 1 ELSE 0 END, with the branch operands
// spelled by the caller so a well-formed selection and a malformed one differ only there.
std::string caseProgram(const char* branches) {
    return "func.func @main() {\n"
           "  %n = db.scan_nodes() : !db.column<!storage.node_id>\n"
           "  %age = db.get_node_properties(%n, \"age\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
           "  %limit = db.constant(30 : i64)\n"
           "  %old = db.gt %age, %limit : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>\n"
           "  %one = db.constant(1 : i64)\n"
           "  %zero = db.constant(0 : i64)\n"
           "  %pick = db.case("
           + std::string(branches)
           + "\n"
             "  db.output(%pick) : !db.column<none>\n"
             "  return\n"
             "}\n";
}

// The well-formed selection: one condition, one value, and a default
const char* const oneBranchWithDefault
    = "{%old}, {%one}, %zero) : (!db.column<!storage.bool>, !db.column<i64>, !db.column<i64>) "
      "-> !db.column<none>";

// The same without a default, which the optional operand allows
const char* const oneBranchNoDefault
    = "{%old}, {%one}) : (!db.column<!storage.bool>, !db.column<i64>) -> !db.column<none>";

// Two conditions against one value: the branches no longer pair up
const char* const twoConditionsOneValue
    = "{%old, %old}, {%one}) : (!db.column<!storage.bool>, !db.column<!storage.bool>, "
      "!db.column<i64>) -> !db.column<none>";

// No branch at all, which selects nothing
const char* const noBranch = "{}, {}, %zero) : (!db.column<i64>) -> !db.column<none>";

}

// db.case's operand groups and its optional default print and parse back, and its verifier
// holds the two invariants the selection rests on: one value per condition, and at least
// one branch to select between.
class CaseOpTest : public ::testing::Test {
protected:
    CaseOpTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // Parses a db-dialect module, returning a null module on failure. The verifier runs as
    // part of parsing, so an op the verifier rejects makes this null. The diagnostics are
    // swallowed so a deliberately malformed program does not print to the test log.
    mlir::OwningOpRef<mlir::ModuleOp> parse(const std::string& programText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    mlir::MLIRContext _context;
};

TEST_F(CaseOpTest, parsesABranchAndItsDefault) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(caseProgram(oneBranchWithDefault));
    ASSERT_TRUE(module);

    mlir::db::Case caseOp;
    module->walk([&caseOp](mlir::db::Case op) { caseOp = op; });

    ASSERT_TRUE(caseOp);
    EXPECT_EQ(caseOp.getConditions().size(), 1U);
    EXPECT_EQ(caseOp.getValues().size(), 1U);
    EXPECT_TRUE(caseOp.getDefaultValue());
}

// The default is the one optional operand, so a defaultless CASE is not a parse error
TEST_F(CaseOpTest, parsesABranchWithoutADefault) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(caseProgram(oneBranchNoDefault));
    ASSERT_TRUE(module);

    mlir::db::Case caseOp;
    module->walk([&caseOp](mlir::db::Case op) { caseOp = op; });

    ASSERT_TRUE(caseOp);
    EXPECT_FALSE(caseOp.getDefaultValue());
}

// The op prints in the form it parses, so a lowered module re-reads as itself
TEST_F(CaseOpTest, printsWhatItParses) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(caseProgram(oneBranchWithDefault));
    ASSERT_TRUE(module);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed);
    EXPECT_TRUE(reparsed) << printed;
}

TEST_F(CaseOpTest, verifierRejectsUnpairedBranches) {
    EXPECT_FALSE(parse(caseProgram(twoConditionsOneValue)));
}

TEST_F(CaseOpTest, verifierRejectsABranchlessSelection) {
    EXPECT_FALSE(parse(caseProgram(noBranch)));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
