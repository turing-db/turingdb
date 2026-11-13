#include "ProductPlacement.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/ADT/STLExtras.h"
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

bool carriesAProductColumn(mlir::OperandRange carriedColumns) {
    return llvm::any_of(carriedColumns, [](mlir::Value carried) {
        return static_cast<bool>(productOf(carried));
    });
}

bool readsEdges(mlir::Operation* op) {
    return mlir::isa<mlir::db::GetOutEdges,
                     mlir::db::GetInEdges,
                     mlir::db::GetEdges,
                     mlir::db::GetOutEdgesByType,
                     mlir::db::GetInEdgesByType,
                     mlir::db::GetOutEdgesByLabel,
                     mlir::db::GetInEdgesByLabel,
                     mlir::db::GetOutEdgesByTypeAndLabel,
                     mlir::db::GetInEdgesByTypeAndLabel,
                     mlir::db::ExplorePaths,
                     mlir::db::ScanEdges,
                     mlir::db::ScanEdgesByType,
                     mlir::db::ScanOutEdgesByLabelSrc,
                     mlir::db::ScanInEdgesByLabelTgt,
                     mlir::db::ScanOutEdgesByLabelTgt,
                     mlir::db::ScanInEdgesByLabelSrc,
                     mlir::db::ScanOutEdgesByLabelSrcTgt,
                     mlir::db::ScanInEdgesByLabelSrcTgt>(op);
}

bool joinsRows(mlir::Operation* op) {
    return mlir::isa<mlir::db::CrossProduct,
                     mlir::db::HashJoin,
                     mlir::db::Unwind,
                     mlir::db::UnwindRange,
                     mlir::db::UnwindComputed>(op);
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
        count += carriesAProductColumn(unwind.getColumnsToFilter()) ? 1 : 0;
    });

    module.walk([&count](mlir::db::UnwindRange unwindRange) {
        count += carriesAProductColumn(unwindRange.getColumnsToFilter()) ? 1 : 0;
    });

    module.walk([&count](mlir::db::UnwindComputed unwindComputed) {
        count += carriesAProductColumn(unwindComputed.getColumnsToFilter()) ? 1 : 0;
    });

    return count;
}

size_t turing::test::countEdgeReadsBeforeTheJoin(mlir::ModuleOp module) {
    mlir::func::FuncOp function = module.lookupSymbol<mlir::func::FuncOp>("main");

    size_t count = 0;
    bool joined = false;
    for (mlir::Operation& op : function.getBody().front()) {
        if (!joined || joinsRows(&op)) {
            op.walk([&count](mlir::Operation* nested) {
                count += readsEdges(nested) ? 1 : 0;
            });
        }

        joined = joined || joinsRows(&op);
    }

    return count;
}
