#include "ProductPlacement.h"

#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"

using namespace turing::test;

namespace {

mlir::Value climbFilters(mlir::Value column) {
    while (mlir::db::FilterOp filter = column.getDefiningOp<mlir::db::FilterOp>()) {
        column = filter.getColumnsToFilter()[mlir::cast<mlir::OpResult>(column).getResultNumber()];
    }

    return column;
}

mlir::db::CrossProduct productOf(mlir::Value column) {
    return climbFilters(column).getDefiningOp<mlir::db::CrossProduct>();
}

size_t leftFactorWidth(mlir::db::CrossProduct product) {
    mlir::Block& leftBlock = product.getLeftFactor().front();

    return leftBlock.getTerminator()->getNumOperands();
}

bool readsOneFactor(mlir::db::FilterOp filter, mlir::db::CrossProduct product) {
    const size_t leftWidth = leftFactorWidth(product);

    bool readsTheLeft = false;
    bool readsTheRight = false;

    llvm::SmallVector<mlir::Value> pending {filter.getMask()};
    llvm::SmallPtrSet<void*, 16> visited;
    while (!pending.empty()) {
        const mlir::Value value = climbFilters(pending.pop_back_val());
        if (!visited.insert(value.getAsOpaquePointer()).second) {
            continue;
        }

        mlir::Operation* const def = value.getDefiningOp();
        if (def == product.getOperation()) {
            const size_t resultIndex = mlir::cast<mlir::OpResult>(value).getResultNumber();
            if (resultIndex < leftWidth) {
                readsTheLeft = true;
            } else {
                readsTheRight = true;
            }
        } else if (def && def->getBlock() == product->getBlock()) {
            for (const mlir::Value operand : def->getOperands()) {
                pending.push_back(operand);
            }
        }
    }

    return readsTheLeft != readsTheRight;
}

}

size_t turing::test::countFiltersOverOneFactor(mlir::ModuleOp module) {
    size_t count = 0;
    module.walk([&count](mlir::db::FilterOp filter) {
        for (const mlir::Value carried : filter.getColumnsToFilter()) {
            const mlir::db::CrossProduct product = productOf(carried);
            if (product) {
                count += readsOneFactor(filter, product) ? 1 : 0;
                return;
            }
        }
    });

    return count;
}

size_t turing::test::countUnwindsOverAProduct(mlir::ModuleOp module) {
    size_t count = 0;
    module.walk([&count](mlir::db::Unwind unwind) {
        for (const mlir::Value carried : unwind.getColumnsToFilter()) {
            if (productOf(carried)) {
                count++;
                return;
            }
        }
    });

    return count;
}
