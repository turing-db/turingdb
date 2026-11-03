#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"

#include "NLDialect.h"
#include "StorageDialect.h"

// nl.container_index answers tagged cells, or for a list base the element its list holds:
// a nullable value, a node or an edge
class ContainerIndexVerifierTest : public ::testing::Test {
protected:
    ContainerIndexVerifierTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    bool verifies(std::string_view baseType, std::string_view indexType, std::string_view resultType) {
        std::string program = "func.func @main(%base: !nl.chunk<";
        program += baseType;
        program += ">, %index: !nl.chunk<";
        program += indexType;
        program += ">) {\n  %r = nl.container_index %base, %index : (!nl.chunk<";
        program += baseType;
        program += ">, !nl.chunk<";
        program += indexType;
        program += ">) -> !nl.chunk<";
        program += resultType;
        program += ">\n  return\n}\n";

        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        const mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));

        return static_cast<bool>(module);
    }

    mlir::MLIRContext _context;
};

TEST_F(ContainerIndexVerifierTest, acceptsTheElementsOfAList) {
    EXPECT_TRUE(verifies("!storage.list<i64>", "!storage.any", "!storage.nullable<i64>"));
    EXPECT_TRUE(verifies("!storage.list<!storage.node_id>", "!storage.any", "!storage.node_id"));
    EXPECT_TRUE(verifies("!storage.list<!storage.any>", "!storage.nullable<!storage.any>", "!storage.nullable<!storage.any>"));
}

TEST_F(ContainerIndexVerifierTest, acceptsTaggedCellsOverAMapOrACell) {
    EXPECT_TRUE(verifies("!storage.map", "!storage.any", "!storage.any"));
    EXPECT_TRUE(verifies("!storage.any", "!storage.nullable<!storage.any>", "!storage.any"));
}

TEST_F(ContainerIndexVerifierTest, rejectsAListElementAnsweredAsAPlainValue) {
    EXPECT_FALSE(verifies("!storage.list<i64>", "!storage.any", "i64"));
}

TEST_F(ContainerIndexVerifierTest, rejectsATypedValueReadOffAMapOrACell) {
    EXPECT_FALSE(verifies("!storage.map", "!storage.any", "!storage.nullable<i64>"));
    EXPECT_FALSE(verifies("!storage.any", "!storage.any", "!storage.node_id"));
}

TEST_F(ContainerIndexVerifierTest, rejectsAnIndexOrABaseOfAnotherType) {
    EXPECT_FALSE(verifies("!storage.list<i64>", "i64", "!storage.nullable<i64>"));
    EXPECT_FALSE(verifies("i64", "!storage.any", "!storage.any"));
}
