#include <gtest/gtest.h>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "StorageDialect.h"

namespace {

class ParameterArgumentNameTest : public ::testing::Test {
protected:
    ParameterArgumentNameTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // Parses one db program and reports whether it verified
    bool parses(const char* programText) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText,
                                                                                                mlir::ParserConfig(&_context));

        return static_cast<bool>(module);
    }

    mlir::MLIRContext _context;
};

constexpr const char* twoNamedArguments = R"mlir(
func.func @main(%min: !db.column<i64> {storage.name = "min"}, %max: !db.column<i64> {storage.name = "max"}) {
  return
}
)mlir";

constexpr const char* twoArgumentsOfOneName = R"mlir(
func.func @main(%a: !db.column<i64> {storage.name = "min"}, %b: !db.column<f64> {storage.name = "min"}) {
  return
}
)mlir";

}

TEST_F(ParameterArgumentNameTest, eachArgumentNamesItsOwnParameter) {
    EXPECT_TRUE(parses(twoNamedArguments));
}

TEST_F(ParameterArgumentNameTest, twoArgumentsCannotNameOneParameter) {
    EXPECT_FALSE(parses(twoArgumentsOfOneName));
}
