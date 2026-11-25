#include "DBOps.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"

#include "DBColumnChecks.h"
#include "PropertyScanLiteral.h"
#include "BoundEntityIDs.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

// Ensures each variable has a numeric name
void ScanEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ConstScanEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void FetchEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanEdgesByType::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanOutEdgesByLabelSrc::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanInEdgesByLabelTgt::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanOutEdgesByLabelTgt::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanInEdgesByLabelSrc::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanOutEdgesByLabelSrcTgt::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanInEdgesByLabelSrcTgt::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

// A label scan must name at least one label to filter by; a label-free scan of
// every node is db.scan_nodes, so an empty label list is malformed IR here.
LogicalResult ScanNodesByLabel::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

// The edge sibling of ScanNodesByLabel::verify: a label-free scan of every edge is
// db.scan_edges, so an empty label list is malformed IR here too.
LogicalResult ScanOutEdgesByLabelSrc::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanInEdgesByLabelTgt::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanOutEdgesByLabelTgt::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanInEdgesByLabelSrc::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanOutEdgesByLabelSrcTgt::verify() {
    if (getSrcLabels().empty() || getTgtLabels().empty()) {
        return emitOpError("requires at least one label on each end");
    }

    return success();
}

LogicalResult ScanInEdgesByLabelSrcTgt::verify() {
    if (getSrcLabels().empty() || getTgtLabels().empty()) {
        return emitOpError("requires at least one label on each end");
    }

    return success();
}

LogicalResult ConstScanNodes::verify() {
    const Type node = columnElement(getResult());
    for (const Value bound : getBoundIDs()) {
        const Type element = columnElement(bound);
        if (!storage::isBoundEntityIDsType(element, node)) {
            return emitOpError("requires bound IDs of type i64, !storage.node_id or !storage.list, got ") << element;
        }
    }

    return success();
}

LogicalResult ConstScanEdges::verify() {
    const Type edge = columnElement(getEids());
    for (const Value bound : getBoundIDs()) {
        const Type element = columnElement(bound);
        if (!storage::isBoundEntityIDsType(element, edge)) {
            return emitOpError("requires bound IDs of type i64, !storage.edge_id or !storage.list, got ") << element;
        }
    }

    return success();
}

LogicalResult ScanNodesByPropertyValue::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    const std::optional<TypedAttr> literal = getValue();
    const Value bound = getBoundValue();
    if (literal.has_value() == static_cast<bool>(bound)) {
        return emitOpError("requires either a literal or a bound value");
    }

    const Type valueType = literal ? literal->getType() : columnElement(bound);
    if (!storage::isPropertyScanType(valueType)) {
        return emitOpError(storage::propertyScanLiteralKinds()) << valueType;
    }

    const std::optional<ArrayAttr> labels = getLabels();
    if (labels && labels->empty()) {
        return emitOpError("requires at least one label when labels are given");
    }

    return success();
}

bool FetchNodes::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._operandOffset = 1, ._resultOffset = 1, ._count = getColumnsToFilter().size()};
    return true;
}

bool FetchEdges::getCarrySet(CarrySet& carrySet) {
    carrySet = CarrySet {._operandOffset = 1, ._resultOffset = 4, ._count = getColumnsToFilter().size()};
    return true;
}
