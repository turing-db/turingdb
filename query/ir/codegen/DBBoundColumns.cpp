#include "DBBoundColumns.h"

#include "NullLiteralType.h"

#include "BioAssert.h"

using namespace db;

bool db::boundAtOrAfter(mlir::Value column, mlir::Operation* reductionOp) {
    mlir::Operation* const definingOp = column.getDefiningOp();

    if (!definingOp || definingOp->getBlock() != reductionOp->getBlock()) {
        return true;
    }

    return definingOp == reductionOp || reductionOp->isBeforeInBlock(definingOp);
}

bool db::isUntypedNullColumnType(mlir::Type type) {
    const auto columnType = mlir::dyn_cast<mlir::db::ColumnType>(type);

    return columnType && mlir::isNullLiteralType(columnType.getType());
}

bool db::isUntypedNullColumn(mlir::Value column) {
    return isUntypedNullColumnType(column.getType());
}

bool db::isPathColumn(mlir::Value column) {
    const auto columnType = mlir::dyn_cast<mlir::db::ColumnType>(column.getType());
    return columnType && mlir::isa<mlir::storage::PathRefType>(columnType.getType());
}

mlir::Value db::findVarOrThrow(const DBProgramGenerator::VariableIdentityMap& map,
                            const VariableDependency* var) {
    const auto findIt = map.find(var);
    bioassert(findIt != map.end(), "Missing value for {}.", var->getName());
    const DBProgramGenerator::VariableIdentities& identities = findIt->second;
    bioassert(!identities.empty(), "Missing identity for {}.", var->getName());
    return identities.back();
}
