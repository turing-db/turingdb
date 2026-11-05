#include <gtest/gtest.h>

#include <string_view>

#include "mlir/AsmParser/AsmParser.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"

#include "StorageDialect.h"

// A node or an edge column holds its own null as an invalid ID, so !storage.nullable wraps
// neither
class NullableEntityTypeTest : public ::testing::Test {
protected:
    NullableEntityTypeTest() {
        _context.getOrLoadDialect<mlir::storage::Storage>();
    }

    bool parses(std::string_view typeText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return static_cast<bool>(mlir::parseType(typeText, &_context));
    }

    mlir::MLIRContext _context;
};

TEST_F(NullableEntityTypeTest, rejectsANullableNode) {
    EXPECT_FALSE(parses("!storage.nullable<!storage.node_id>"));
}

TEST_F(NullableEntityTypeTest, rejectsANullableEdge) {
    EXPECT_FALSE(parses("!storage.nullable<!storage.edge_id>"));
}

TEST_F(NullableEntityTypeTest, acceptsANullableValue) {
    EXPECT_TRUE(parses("!storage.nullable<i64>"));
}

TEST_F(NullableEntityTypeTest, acceptsANullableLabel) {
    EXPECT_TRUE(parses("!storage.nullable<!storage.label_id>"));
}

TEST_F(NullableEntityTypeTest, acceptsANullableListOfNodes) {
    EXPECT_TRUE(parses("!storage.nullable<!storage.list<!storage.node_id>>"));
}
