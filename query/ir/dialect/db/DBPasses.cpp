#include "DBPasses.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/Operation.h"

#include "llvm/ADT/SmallVector.h"

#include <optional>

#include "DBDialect.h"
#include "DBOps.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSESCANBYLABEL
#include "DBPasses.h.inc"

namespace {

struct LabelScanChain {
    ScanNodes scan;
    GetNodeLabelSet labelSet;
    CheckLabelConstraint check;
};

// Matches `filter` against that chain, returning the three producing ops, or
// nullopt when the filter is anything else - a different mask, more than one
// filtered column, or a source that is not a plain scan.
std::optional<LabelScanChain> matchLabelScanChain(FilterOp filter) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.size() != 1) {
        return std::nullopt;
    }

    const Value scanColumn = columns[0];

    auto check = filter.getMask().getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return std::nullopt;
    }

    auto labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet || labelSet.getInputNodes() != scanColumn) {
        return std::nullopt;
    }

    auto scan = scanColumn.getDefiningOp<ScanNodes>();
    if (!scan) {
        return std::nullopt;
    }

    return LabelScanChain {.scan = scan, .labelSet = labelSet, .check = check};
}

void eraseIfUnused(Operation* op) {
    if (op && op->use_empty()) {
        op->erase();
    }
}

struct FuseScanByLabel : public impl::FuseScanByLabelBase<FuseScanByLabel> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect matches first: fusing erases ops, which would invalidate the walk.
        llvm::SmallVector<FilterOp> filters;
        root->walk([&](FilterOp filter) {
            if (matchLabelScanChain(filter)) {
                filters.push_back(filter);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (FilterOp filter : filters) {
            std::optional<LabelScanChain> chain = matchLabelScanChain(filter);
            if (!chain) {
                continue;
            }

            builder.setInsertionPoint(filter);
            auto scanByLabel = builder.create<ScanNodesByLabel>(filter.getLoc(),
                                                                chain->scan.getResult().getType(),
                                                                chain->check.getLabels());

            Operation* const filterOp = filter.getOperation();
            filterOp->getResult(0).replaceAllUsesWith(scanByLabel.getResult());
            filterOp->erase();

            // Drop the now-dead chain, consumer to producer, each only if unused.
            eraseIfUnused(chain->check);
            eraseIfUnused(chain->labelSet);
            eraseIfUnused(chain->scan);
        }
    }
};

}

}
