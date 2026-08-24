#include "DBPasses.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include <optional>

#include "DBDialect.h"
#include "DBOps.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSESCANBYLABEL
#define GEN_PASS_DEF_PUSHDOWNFILTERS
#include "DBPasses.h.inc"

namespace {

struct LabelScanChain {
    ScanNodes scan;
    GetNodeLabelSet labelSet;
    CheckLabelConstraint check;
};

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

bool isNodeSource(Operation* op) {
    return isa<ScanNodes, ScanNodesByLabel, ConstScanNodes>(op);
}

bool isEdgeHop(Operation* op) {
    return isa<GetOutEdges, GetInEdges, GetEdges, GetOutEdgesByType, GetInEdgesByType>(op);
}

bool isReverseHop(Operation* op) {
    return isa<GetInEdges, GetInEdgesByType>(op);
}

// The columns a cross_product factor yields, in result order.
Operation::operand_range factorYieldColumns(mlir::Region& factor) {
    mlir::Block& factorBlock = factor.front();
    Yield yield = cast<Yield>(factorBlock.getTerminator());

    return yield.getColumns();
}

// Walk op chain until we reach a node/edge source or something we can't push down to
Value climbToLineageAnchor(Value column) {
    while (true) {
        Operation* const def = column.getDefiningOp();
        if (!def) {
            return {};
        }

        if (isNodeSource(def)) {
            return column;
        }

        if (FilterOp filter = dyn_cast<FilterOp>(def)) {
            if (filter.getColumnsToFilter().size() != 1) {
                return {};
            }

            return column;
        }

        if (CrossProduct product = dyn_cast<CrossProduct>(def)) {
            // Descend into the xprod factor which holds def
            const unsigned resultIndex = cast<OpResult>(column).getResultNumber();

            mlir::Region& leftFactor = product.getLeftFactor();
            const Operation::operand_range leftColumns = factorYieldColumns(leftFactor);
            const unsigned leftCount = leftColumns.size();

            if (resultIndex < leftCount) {
                column = leftColumns[resultIndex];
            } else {
                mlir::Region& rightFactor = product.getRightFactor();
                const Operation::operand_range rightColumns = factorYieldColumns(rightFactor);
                column = rightColumns[resultIndex - leftCount];
            }

            continue;
        }

        if (isEdgeHop(def)) {
            const unsigned resultIndex = cast<OpResult>(column).getResultNumber();
            const unsigned srcResultIndex = 0;
            const unsigned tgtResultIndex = 3;
            const unsigned fixedResultCount = 4;

            const unsigned inputResultIndex = isReverseHop(def) ? tgtResultIndex : srcResultIndex;

            if (resultIndex == inputResultIndex) {
                column = def->getOperand(0);
            } else if (resultIndex >= fixedResultCount) {
                // Carried columns follow input_nodes (operand 0) in operand order.
                column = def->getOperand(1 + (resultIndex - fixedResultCount));
            } else {
                return {};
            }
        } else {
            return {};
        }
    }
}

bool isMaskComputeOp(Operation* op) {
    return isa<EqOp, NeqOp, GtOp, LtOp, GteOp, LteOp,
               AndOp, OrOp, XorOp, NotOp,
               AddOp, SubOp, MulOp, DivOp, ModOp, PowOp,
               ConstantOp,
               GetNodeProperties, GetEdgeProperties>(op);
}

struct MaskCone {
    llvm::SmallVector<Operation*> _ops;
    llvm::SmallSetVector<Value, 4> _inputs;
};

// Gathers the mask's compute cone and the external columns feeding it. The cone ops
// come back in topological (block) order, ready to clone.
MaskCone collectMaskCone(Value mask) {
    MaskCone cone;

    llvm::SmallPtrSet<Operation*, 8> inCone;
    llvm::SmallVector<Value, 8> worklist {mask};
    while (!worklist.empty()) {
        const Value value = worklist.pop_back_val();
        Operation* const def = value.getDefiningOp();

        if (!def || !isMaskComputeOp(def)) {
            cone._inputs.insert(value);
            continue;
        }

        if (!inCone.insert(def).second) {
            continue;
        }

        cone._ops.push_back(def);
        for (const Value operand : def->getOperands()) {
            worklist.push_back(operand);
        }
    }

    llvm::sort(cone._ops, [](Operation* lhs, Operation* rhs) {
        return lhs->isBeforeInBlock(rhs);
    });

    return cone;
}

// A single-variable property-predicate filter and where its mask should be rebuilt:
// the lineage anchor of the one column its mask reads.
struct PushablePredicate {
    Value _anchor;
    MaskCone _cone;
};

std::optional<PushablePredicate> matchPushablePredicate(FilterOp filter) {
    Operation* const maskDef = filter.getMask().getDefiningOp();
    if (!maskDef || !isMaskComputeOp(maskDef)) {
        return std::nullopt;
    }

    MaskCone cone = collectMaskCone(filter.getMask());
    if (cone._inputs.empty()) {
        return std::nullopt;
    }

    Value anchor;
    bool reachedAnchor = true;
    for (const Value input : cone._inputs) {
        const Value inputAnchor = climbToLineageAnchor(input);
        if (!inputAnchor) {
            return std::nullopt;
        }

        if (!anchor) {
            anchor = inputAnchor;
        } else if (anchor != inputAnchor) {
            // More than one lineage feeds the mask: not a single-variable predicate.
            return std::nullopt;
        }

        if (input != inputAnchor) {
            reachedAnchor = false;
        }
    }

    // Filter already maximally pushed down
    if (reachedAnchor) {
        return std::nullopt;
    }

    return PushablePredicate {._anchor = anchor, ._cone = std::move(cone)};
}

void pushDownPredicate(FilterOp filter, const PushablePredicate& pushable, mlir::OpBuilder& builder) {
    Value anchor = pushable._anchor;
    const MaskCone& cone = pushable._cone;
    const mlir::Location loc = filter.getLoc();

    // Rebuild the mask over the anchor version of the column, right after it is bound.
    mlir::IRMapping mapping;
    for (const Value input : cone._inputs) {
        mapping.map(input, anchor);
    }

    builder.setInsertionPointAfter(anchor.getDefiningOp());

    llvm::SmallPtrSet<Operation*, 8> anchorReaders;
    for (Operation* const coneOp : cone._ops) {
        Operation* const cloned = builder.clone(*coneOp, mapping);
        anchorReaders.insert(cloned);
        builder.setInsertionPointAfter(cloned);
    }

    const Value clonedMask = mapping.lookup(filter.getMask());

    const llvm::SmallVector<mlir::Type, 1> resultTypes {anchor.getType()};
    FilterOp pushed = builder.create<FilterOp>(loc, resultTypes, clonedMask, mlir::ValueRange {anchor});
    const Value pushedColumn = pushed.getResult(0);
    anchorReaders.insert(pushed.getOperation());

    // Every downstream consumer of the anchor now reads the filtered column; the clones
    // and the pushed filter keep reading the anchor itself.
    anchor.replaceAllUsesExcept(pushedColumn, anchorReaders);

    // The original filter is now redundant - its rows were already dropped upstream - so
    // its outputs fold back onto its inputs and it, then its dead mask cone, are erased.
    const mlir::ResultRange filtered = filter.getFilteredColumns();
    const Operation::operand_range carried = filter.getColumnsToFilter();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(carried[index]);
    }
    filter.erase();

    for (Operation* const coneOp : llvm::reverse(cone._ops)) {
        eraseIfUnused(coneOp);
    }
}

struct PushDownFilters : public impl::PushDownFiltersBase<PushDownFilters> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        llvm::SmallVector<FilterOp> filters;
        root->walk([&](FilterOp filter) {
            filters.push_back(filter);
        });

        mlir::OpBuilder builder(&getContext());
        for (FilterOp filter : filters) {
            std::optional<PushablePredicate> pushable = matchPushablePredicate(filter);
            if (!pushable) {
                continue;
            }

            pushDownPredicate(filter, *pushable, builder);
        }
    }
};

}

}
