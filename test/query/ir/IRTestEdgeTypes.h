#pragma once

#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <vector>

#include "mlir/IR/BuiltinAttributes.h"

#include "llvm/ADT/StringRef.h"

namespace turing::test {

// The one type name a by-type op carries, for the tests that fuse exactly one
inline llvm::StringRef onlyEdgeType(mlir::ArrayAttr edgeTypes) {
    EXPECT_EQ(edgeTypes.size(), 1u);
    return mlir::cast<mlir::StringAttr>(edgeTypes[0]).getValue();
}

// The type names a by-type op carries, in order
inline void expectEdgeTypes(mlir::ArrayAttr edgeTypes, const std::vector<std::string>& expected) {
    ASSERT_EQ(edgeTypes.size(), expected.size());
    for (size_t index = 0; index < expected.size(); index++) {
        EXPECT_EQ(mlir::cast<mlir::StringAttr>(edgeTypes[index]).getValue(), expected[index]);
    }
}

}
