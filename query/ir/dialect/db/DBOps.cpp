#include "DBOps.h"

#include "llvm/ADT/STLExtras.h"

#include "DBColumnElement.h"
#include "DBColumnChecks.h"
#include "DBSpeculation.h"
#include "IRValueTypes.h"
#include "StorageEnums.h"
#include "ColumnIndicesFormat.h"
#include "EdgeDirectionsFormat.h"
#include "PathStepsFormat.h"
#include "LabelAlternativesFormat.h"
#include "StorageAttributes.h"
#include "MergePatternShape.h"
#include "NullLiteralType.h"
#include "GroupAggregateKindsFormat.h"
#include "PropertyScanLiteral.h"
#include "BoundValueFormat.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

namespace {

bool writesAMapPerRow(Value column) {
    const Type declared = cast<ColumnType>(column.getType()).getType();

    return mayHoldAnyType(column) || isa<storage::MapType>(columnElement(column)) || isNullLiteralType(declared);
}

template <typename IDType>
bool holdsIDsOf(Value column) {
    return mayHoldAnyType(column) || isa<IDType>(columnElement(column));
}

template <typename... ElementTypes>
Speculation::Speculatability speculatableOverElement(Value input) {
    return speculatableWhen(isa<ElementTypes...>(columnElement(input)));
}

LogicalResult verifyStringEnd(Operation* op, Value string, Value length, Value result) {
    const bool readsAStringAndALength = isColumnOf<storage::StringType>(string.getType()) && isIntegerColumn(length);
    if (!readsAStringAndALength) {
        return op->emitOpError("operands must be a string column and an integer column");
    }

    return verifyStringResult(op, result);
}

}

#define GET_OP_CLASSES
#include "DBOps.cpp.inc"

LogicalResult CreateNode::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
    }

    return success();
}

LogicalResult CreateEdge::verify() {
    const StringAttr edgeType = dyn_cast<StringAttr>(getEdgeType());
    if (edgeType && edgeType.getValue().empty()) {
        return emitOpError("requires a non-empty edge type");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
    }

    return success();
}

LogicalResult Merge::verify() {
    const LogicalResult pattern = verifyMergePattern(getOperation(),
                                                     getNodeLabels(),
                                                     getNodePropNames(),
                                                     getEdgeTypes(),
                                                     getEdgePropNames(),
                                                     getEdgeDirectionsAttr(),
                                                     getPendingNodesAttr(),
                                                     getBoundNodes().size(),
                                                     getBoundPending().size(),
                                                     getNodePropValues().size(),
                                                     getEdgePropValues().size());
    if (failed(pattern)) {
        return pattern;
    }

    const size_t expectedResults = mergeResultCount(getNodeLabels(), getCarriedColumns().size());
    if (getResults().size() != expectedResults) {
        return emitOpError("must produce one column per chain entity, the created mask and one "
                           "per carried column - ")
               << expectedResults << " in all, but produces " << getResults().size();
    }

    return success();
}

LogicalResult SetNodeProperty::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    return success();
}

LogicalResult SetEdgeProperty::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    return success();
}

LogicalResult SetNodeProperties::verify() {
    if (!holdsIDsOf<storage::NodeIDType>(getInputNodes())) {
        return emitOpError("writes the properties of a column of node IDs");
    } else if (!writesAMapPerRow(getValue())) {
        return emitOpError("writes a column of maps or of tagged cells, optionally nullable, or the null literal");
    } else if (!::db::namesPropertyElementTypes(getCreatedTypesAttr())) {
        return emitOpError("creates each property with the element type of a stored value");
    }

    return success();
}

LogicalResult SetEdgeProperties::verify() {
    if (!holdsIDsOf<storage::EdgeIDType>(getInputEdges())) {
        return emitOpError("writes the properties of a column of edge IDs");
    } else if (!writesAMapPerRow(getValue())) {
        return emitOpError("writes a column of maps or of tagged cells, optionally nullable, or the null literal");
    } else if (!::db::namesPropertyElementTypes(getCreatedTypesAttr())) {
        return emitOpError("creates each property with the element type of a stored value");
    }

    return success();
}

LogicalResult ImportGraph::verify() {
    if (!::db::namesImportedPropertyTypes(getFormat(), getTypesAttr())) {
        return emitOpError("types each property as a datetime, duration or point a jsonl import reads, or a duration a parquet import reads");
    }

    return success();
}

LogicalResult CreateVectorIndex::verify() {
    if (getDimension() == 0) {
        return emitOpError("must hold vectors of at least one dimension");
    }

    return success();
}
