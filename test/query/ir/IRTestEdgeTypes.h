#pragma once

#include <gtest/gtest.h>

#include "mlir/IR/BuiltinAttributes.h"

#include "llvm/ADT/StringRef.h"

namespace turing::test {

// The one type name a by-type op carries, for the tests that fuse exactly one
inline llvm::StringRef onlyEdgeType(mlir::ArrayAttr edgeTypes) {
    EXPECT_EQ(edgeTypes.size(), 1u);
    return mlir::cast<mlir::StringAttr>(edgeTypes[0]).getValue();
}

}
