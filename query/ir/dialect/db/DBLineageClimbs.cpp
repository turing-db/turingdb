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
