#include "DBLineageClimbs.h"

#include "mlir/Interfaces/SideEffectInterfaces.h"

#include "DBFilterUtils.h"
#include "DBPassUtils.h"

using namespace mlir;
using namespace mlir::db;

namespace {

// A filter over the rows an op hands on can run on the rows it takes instead, unless the op
// has an effect a row it drops would miss, or keeps the rows at some positions
bool letsAFilterAcross(Operation* op) {
    return mlir::isMemoryEffectFree(op) && !op->hasTrait<OpTrait::KeepsRowsByPosition>();
}

}

void mlir::db::collectColumnEqualities(FilterOp filter, llvm::SmallVectorImpl<ColumnEquality>& equalities) {
    llvm::SmallVector<Value, 4> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    for (const Value conjunct : conjuncts) {
        EqOp equality = conjunct.getDefiningOp<EqOp>();
        if (!equality || !isNodeColumn(equality.getLhs()) || !isNodeColumn(equality.getRhs())) {
            continue;
        }

        equalities.push_back(ColumnEquality {equality.getLhs(), equality.getRhs()});
    }
}

void mlir::db::stepTowardLineageAnchor(Value column, LineageStep& step) {
    Operation* const def = column.getDefiningOp();
    if (!def) {
        return;
    } else if (isNodeSource(def)) {
        step._isAnchor = true;
        return;
    }

    ColumnCarrierOpInterface carrier = dyn_cast<ColumnCarrierOpInterface>(def);
    if (!carrier) {
        return;
    }

    CarrySet carrySet;
    const Value rowSource = carrier.getRowSource(cast<OpResult>(column));

    if (rowSource && letsAFilterAcross(def)) {
        step._next = rowSource;
        step._crossesRowOp = !isa<FilterOp>(def);
    } else if (!rowSource && carrier.getCarrySet(carrySet)) {
        step._isAnchor = true;
    }
}

Value LineageClimbs::anchorOf(Value column) {
    computeClimb(column);
    return _climbs.at(column)._anchor;
}

Value LineageClimbs::climb(Value column, bool& crossedRowOp, llvm::SmallVectorImpl<ColumnEquality>& anchorEqualities) {
    computeClimb(column);

    const Climb& climb = _climbs.at(column);
    crossedRowOp = crossedRowOp || climb._crossedRowOp;

    if (climb._equalities != NO_LIST) {
        llvm::append_range(anchorEqualities, _equalityLists[climb._equalities]);
    }

    return climb._anchor;
}

void LineageClimbs::forget(Operation* op) {
    for (const Value result : op->getResults()) {
        _climbs.erase(result);
    }
}

void LineageClimbs::clear() {
    _climbs.clear();
    _equalityLists.clear();
}

void LineageClimbs::computeClimb(Value column) {
    llvm::SmallVector<Value> worklist {column};
    llvm::SmallVector<ColumnEquality, 4> filterEqualities;

    while (!worklist.empty()) {
        const Value value = worklist.back();
        if (_climbs.contains(value)) {
            worklist.pop_back();
            continue;
        }

        LineageStep step;
        stepTowardLineageAnchor(value, step);

        filterEqualities.clear();
        if (FilterOp filter = value.getDefiningOp<FilterOp>()) {
            collectColumnEqualities(filter, filterEqualities);
        }

        bool dependenciesPending = false;
        const auto requireClimb = [this, &worklist, &dependenciesPending](Value dependency) {
            if (dependency && !_climbs.contains(dependency)) {
                worklist.push_back(dependency);
                dependenciesPending = true;
            }
        };

        requireClimb(step._next);
        for (const ColumnEquality& equality : filterEqualities) {
            requireClimb(equality.first);
            requireClimb(equality.second);
        }

        if (dependenciesPending) {
            continue;
        }

        Climb climb;
        if (step._isAnchor) {
            climb._anchor = value;
        } else if (step._next) {
            const Climb next = _climbs.at(step._next);
            climb._anchor = next._anchor;
            climb._crossedRowOp = next._crossedRowOp || step._crossesRowOp;
            climb._equalities = next._equalities;

            for (const ColumnEquality& equality : llvm::reverse(filterEqualities)) {
                const ColumnEquality anchors {_climbs.at(equality.first)._anchor, _climbs.at(equality.second)._anchor};
                const bool relatesTwoAnchors = anchors.first && anchors.second && anchors.first != anchors.second;
                if (!relatesTwoAnchors) {
                    continue;
                }

                llvm::SmallVector<ColumnEquality, 2> equalities {anchors};
                if (climb._equalities != NO_LIST) {
                    const llvm::SmallVector<ColumnEquality, 2>& held = _equalityLists[climb._equalities];
                    if (llvm::is_contained(held, anchors)) {
                        continue;
                    }

                    llvm::append_range(equalities, held);
                }

                _equalityLists.push_back(equalities);
                climb._equalities = _equalityLists.size() - 1;
            }
        }

        _climbs[value] = climb;
        worklist.pop_back();
    }
}
