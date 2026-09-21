#pragma once

#include <stddef.h>

#include "mlir/IR/BuiltinOps.h"

#include "llvm/ADT/SmallVector.h"

namespace turing::test {

// Every op of one kind in the module, in program order
template <typename OpType>
llvm::SmallVector<OpType> collect(mlir::ModuleOp module) {
    llvm::SmallVector<OpType> ops;
    module.walk([&](OpType op) {
        ops.push_back(op);
    });

    return ops;
}

template <typename OpType>
size_t countOps(mlir::ModuleOp module) {
    return collect<OpType>(module).size();
}

}
