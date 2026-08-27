#include "DBPasses.h"

#include <optional>

#include "mlir/IR/Builders.h"
#include "mlir/IR/IRMapping.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

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
    for (;;) {
        Operation* const def = column.getDefiningOp();
        if (!def) {
            return {};
        }

        if (isNodeSource(def)) {
            return column;
        }

        if (isa<FilterOp>(def)) {
            // A filter passes each column through unchanged, so its result is the same
            // variable one step on - a valid boundary to sit a further filter right after.
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
            const size_t resultIndex = cast<OpResult>(column).getResultNumber();
            constexpr size_t srcResultIndex = 0;
            constexpr size_t tgtResultIndex = 3;
            constexpr size_t fixedResultCount = 4;

            // The input node re-surfaces as srcids (forward) or tgtids (reverse) and each
            // carried column passes through: those continue a variable that existed before
            // the hop, so keep climbing. The opposite node end and the eids/etypes are
            // bound here, so the variable is born at this hop - its earliest filter point.
            const size_t inputResultIndex = isReverseHop(def) ? tgtResultIndex : srcResultIndex;

            if (resultIndex == inputResultIndex) {
                column = def->getOperand(0);
            } else if (resultIndex >= fixedResultCount) {
                // Carried columns follow input_nodes (operand 0) in operand order.
                column = def->getOperand(1 + (resultIndex - fixedResultCount));
            } else {
                return column;
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

// Gathers the mask's compute cone and the external columns feeding it. Valid
// even when the cone spans blocks.
void collectConePostOrder(Value value, llvm::SmallPtrSet<Operation*, 8>& visited, MaskCone& cone) {
    Operation* const def = value.getDefiningOp();

    if (!def || !isMaskComputeOp(def)) {
        cone._inputs.insert(value);
        return;
    }

    if (!visited.insert(def).second) {
        return;
    }

    for (const Value operand : def->getOperands()) {
        collectConePostOrder(operand, visited, cone);
    }

    cone._ops.push_back(def);
}

MaskCone collectMaskCone(Value mask) {
    MaskCone cone;

    llvm::SmallPtrSet<Operation*, 8> visited;
    collectConePostOrder(mask, visited, cone);

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

    Operation* const anchorProducer = anchor.getDefiningOp();

    llvm::SmallVector<mlir::Value> liveColumns;
    for (const mlir::Value result : anchorProducer->getResults()) {
        if (!result.use_empty()) {
            liveColumns.push_back(result);
        }
    }

    // Rebuild the mask over the anchor version of the column, right after it is bound.
    mlir::IRMapping mapping;
    for (const Value input : cone._inputs) {
        mapping.map(input, anchor);
    }

    builder.setInsertionPointAfter(anchorProducer);

    llvm::SmallPtrSet<Operation*, 8> boundaryReaders;
    for (Operation* const coneOp : cone._ops) {
        Operation* const cloned = builder.clone(*coneOp, mapping);
        boundaryReaders.insert(cloned);
        builder.setInsertionPointAfter(cloned);
    }

    const Value clonedMask = mapping.lookup(filter.getMask());

    llvm::SmallVector<mlir::Type, 8> resultTypes;
    for (const mlir::Value column : liveColumns) {
        resultTypes.push_back(column.getType());
    }

    FilterOp pushed = builder.create<FilterOp>(loc, resultTypes, clonedMask, liveColumns);
    boundaryReaders.insert(pushed.getOperation());

    // Replace live columns with filtered versions
    for (size_t i {0}; mlir::Value liveCol : liveColumns) {
        liveCol.replaceAllUsesExcept(pushed.getResult(i++), boundaryReaders);
    }

    // Remove original filter
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
