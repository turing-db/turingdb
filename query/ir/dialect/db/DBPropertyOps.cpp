#include "DBOps.h"

#include "llvm/ADT/STLExtras.h"

#include "DBRowCosts.h"

using namespace mlir;
using namespace mlir::db;

namespace {

LogicalResult verifyEdgeTypesNotEmpty(Operation* operation, ArrayAttr edgeTypes) {
    if (edgeTypes.empty()) {
        return operation->emitOpError("requires at least one edge type");
    }

    return success();
}

}

LogicalResult CheckLabelConstraint::verify() {
    const ArrayAttr alternatives = getAlternatives();
    const bool hasEmptyAlternative = llvm::any_of(alternatives, [](Attribute alternative) {
        return cast<ArrayAttr>(alternative).empty();
    });

    if (alternatives.empty() || hasEmptyAlternative) {
        return emitOpError("requires at least one label in every alternative");
    }

    return success();
}

ArrayAttr CheckLabelConstraint::getConjunction() {
    const ArrayAttr alternatives = getAlternatives();
    if (alternatives.size() != 1) {
        return nullptr;
    }

    return cast<ArrayAttr>(alternatives[0]);
}

LogicalResult CheckEdgeTypeConstraint::verify() {
    return verifyEdgeTypesNotEmpty(getOperation(), getEdgeTypes());
}

double GetNodeLabelSet::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double GetEdgeTypes::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double GetNodeProperties::getRowCost() {
    return STORAGE_READ_ROW_COST;
}

double GetEdgeProperties::getRowCost() {
    return STORAGE_READ_ROW_COST;
}
