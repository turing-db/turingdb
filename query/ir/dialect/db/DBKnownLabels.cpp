#include "DBKnownLabels.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBFilterUtils.h"
#include "DBPassUtils.h"

using namespace mlir;
using namespace mlir::db;

namespace {

// A label is known by its attribute: a name, or a parameter, which binds the same value
// wherever the program writes it
void addLabelNames(ArrayAttr names, llvm::DenseSet<Attribute>& labels) {
    for (const Attribute name : names) {
        labels.insert(name);
    }
}

void addFilterLabels(FilterOp filter, Value filtered, llvm::DenseSet<Attribute>& labels) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
        if (!check) {
            continue;
        }

        GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
        if (labelSet && labelSet.getInputNodes() == filtered) {
            addSharedLabelNames(check.getAlternatives(), labels);
        }
    }
}

}

bool mlir::db::readEdgeScanLabels(Operation* op, EdgeScanLabels& scanLabels) {
    if (isa<ScanEdges>(op)) {
        return true;
    } else if (isa<ScanOutEdgesByLabelSrc, ScanInEdgesByLabelSrc>(op)) {
        scanLabels._source = op->getAttrOfType<ArrayAttr>("labels");
        return true;
    } else if (isa<ScanOutEdgesByLabelTgt, ScanInEdgesByLabelTgt>(op)) {
        scanLabels._target = op->getAttrOfType<ArrayAttr>("labels");
        scanLabels._indexedByTarget = true;
        return true;
    } else if (ScanOutEdgesByLabelSrcTgt scan = dyn_cast<ScanOutEdgesByLabelSrcTgt>(op)) {
        scanLabels._source = scan.getSrcLabels();
        scanLabels._target = scan.getTgtLabels();
        return true;
    } else if (ScanInEdgesByLabelSrcTgt scan = dyn_cast<ScanInEdgesByLabelSrcTgt>(op)) {
        scanLabels._source = scan.getSrcLabels();
        scanLabels._target = scan.getTgtLabels();
        scanLabels._indexedByTarget = true;
        return true;
    }

    return false;
}

void mlir::db::addSharedLabelNames(ArrayAttr alternatives, llvm::DenseSet<Attribute>& labels) {
    for (const Attribute label : cast<ArrayAttr>(alternatives[0])) {
        const auto asksFor = [label](Attribute alternative) {
            return llvm::is_contained(cast<ArrayAttr>(alternative), label);
        };

        if (llvm::all_of(alternatives, asksFor)) {
            labels.insert(label);
        }
    }
}

void mlir::db::collectKnownLabels(Value column, llvm::DenseSet<Attribute>& labels) {
    constexpr size_t srcResultIndex = 0;
    constexpr size_t tgtResultIndex = 3;

    for (;;) {
        Operation* const def = column.getDefiningOp();
        if (!def) {
            return;
        }

        const size_t resultIndex = cast<OpResult>(column).getResultNumber();

        if (ScanNodesByLabel scan = dyn_cast<ScanNodesByLabel>(def)) {
            addLabelNames(scan.getLabels(), labels);
            return;
        } else if (ScanNodesByPropertyValue scan = dyn_cast<ScanNodesByPropertyValue>(def)) {
            if (const std::optional<ArrayAttr> scanLabels = scan.getLabels()) {
                addLabelNames(*scanLabels, labels);
            }
            return;
        } else if (EdgeScanLabels scanLabels; readEdgeScanLabels(def, scanLabels)) {
            if (resultIndex == srcResultIndex && scanLabels._source) {
                addLabelNames(scanLabels._source, labels);
            } else if (resultIndex == tgtResultIndex && scanLabels._target) {
                addLabelNames(scanLabels._target, labels);
            }
            return;
        } else if (FilterOp filter = dyn_cast<FilterOp>(def)) {
            const Value filtered = filter.getColumnsToFilter()[resultIndex];
            addFilterLabels(filter, filtered, labels);
            column = filtered;
        } else if (ExplorePaths exploration = dyn_cast<ExplorePaths>(def)) {
            const std::optional<ArrayAttr> endLabels = exploration.getEndLabels();
            if (column == exploration.getTgtids() && endLabels) {
                addLabelNames(*endLabels, labels);
            }

            const Value seed = seedColumnOf(exploration, column);
            if (!seed) {
                return;
            }

            column = seed;
        } else if (isEdgeHop(def)) {
            const size_t inputResultIndex = isReverseHop(def) ? tgtResultIndex : srcResultIndex;
            const size_t reachedResultIndex = isReverseHop(def) ? srcResultIndex : tgtResultIndex;

            if (resultIndex == inputResultIndex) {
                column = def->getOperand(0);
            } else if (resultIndex >= hopFixedResultCount) {
                column = def->getOperand(1 + (resultIndex - hopFixedResultCount));
            } else {
                const ArrayAttr hopLabels = def->getAttrOfType<ArrayAttr>("labels");
                if (resultIndex == reachedResultIndex && hopLabels) {
                    addLabelNames(hopLabels, labels);
                }
                return;
            }
        } else {
            return;
        }
    }
}
