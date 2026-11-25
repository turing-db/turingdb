#include "NLOps.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "PropertyScanLiteral.h"
#include "BoundEntityIDs.h"
#include "NLChunkTypes.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

// A node scan always produces one chunk of node IDs per step
LogicalResult ScanNodes::inferReturnTypes(MLIRContext* context,
                                          std::optional<Location> location,
                                          ScanNodes::Adaptor adaptor,
                                          SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(IteratorType::get(context, {getNodeIDChunkType(context)}));
    return success();
}

// A label scan filters the rows a plain scan would produce, never their shape,
// so it produces the same single chunk of node IDs per step - the label list is
// a filter, not a result type.
LogicalResult ScanNodesByLabel::inferReturnTypes(MLIRContext* context,
                                                 std::optional<Location> location,
                                                 ScanNodesByLabel::Adaptor adaptor,
                                                 SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(IteratorType::get(context, {getNodeIDChunkType(context)}));
    return success();
}

// A const scan emits a fixed set of node IDs, but its row shape is a plain node
// scan's, so it produces the same single chunk of node IDs per step - the ID list
// is which rows are emitted, not a result type.
LogicalResult ConstScanNodes::inferReturnTypes(MLIRContext* context,
                                               std::optional<Location> location,
                                               ConstScanNodes::Adaptor adaptor,
                                               SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(IteratorType::get(context, {getNodeIDChunkType(context)}));
    return success();
}

LogicalResult ConstScanNodes::verify() {
    const Type node = storage::NodeIDType::get(getContext());
    for (const Value bound : getBoundIDs()) {
        const Type element = valueElementOf(bound.getType());
        if (!storage::isBoundEntityIDsType(element, node)) {
            return emitOpError("requires bound IDs of type i64, !storage.node_id or !storage.list, got ") << element;
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

    const Type valueType = literal ? literal->getType() : valueElementOf(bound.getType());
    if (!storage::isPropertyScanType(valueType)) {
        return emitOpError(storage::propertyScanLiteralKinds()) << valueType;
    }

    const std::optional<ArrayAttr> labels = getLabels();
    if (labels && labels->empty()) {
        return emitOpError("requires at least one label when labels are given");
    }

    return success();
}

// A property-value scan filters the rows a plain scan would produce, never their
// shape, so it produces the same single chunk of node IDs per step - the name and
// literal are a filter, not a result type.
LogicalResult ScanNodesByPropertyValue::inferReturnTypes(MLIRContext* context,
                                                         std::optional<Location> location,
                                                         ScanNodesByPropertyValue::Adaptor adaptor,
                                                         SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(IteratorType::get(context, {getNodeIDChunkType(context)}));
    return success();
}

// An edge scan produces the same four fixed edge chunks per step as an
// out-edges fetch, but reads no input and carries nothing, so the iterator has
// exactly those four chunks and no carried tail.
LogicalResult ScanEdges::inferReturnTypes(MLIRContext* context,
                                         std::optional<Location> location,
                                         ScanEdges::Adaptor adaptor,
                                         SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, {}));
    return success();
}

LogicalResult ConstScanEdges::inferReturnTypes(MLIRContext* context,
                                               std::optional<Location> location,
                                               ConstScanEdges::Adaptor adaptor,
                                               SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, {}));
    return success();
}

LogicalResult ConstScanEdges::verify() {
    const Type edge = storage::EdgeIDType::get(getContext());
    for (const Value bound : getBoundIDs()) {
        const Type element = valueElementOf(bound.getType());
        if (!storage::isBoundEntityIDsType(element, edge)) {
            return emitOpError("requires bound IDs of type i64, !storage.edge_id or !storage.list, got ") << element;
        }
    }

    return success();
}

// Keeping one edge type narrows only the rows, never their shape, so a by-type
// scan produces the same four-chunk edge iterator as ScanEdges.
LogicalResult ScanEdgesByType::inferReturnTypes(MLIRContext* context,
                                                std::optional<Location> location,
                                                ScanEdgesByType::Adaptor adaptor,
                                                SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, {}));
    return success();
}

// Keeping the edges of one label's nodes narrows only the rows, never their shape,
// so a by-label out-edge scan produces the same four-chunk edge iterator as ScanEdges.
LogicalResult ScanOutEdgesByLabelSrc::inferReturnTypes(MLIRContext* context,
                                                       std::optional<Location> location,
                                                       ScanOutEdgesByLabelSrc::Adaptor adaptor,
                                                       SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, {}));
    return success();
}

LogicalResult ScanInEdgesByLabelTgt::inferReturnTypes(MLIRContext* context,
                                                      std::optional<Location> location,
                                                      ScanInEdgesByLabelTgt::Adaptor adaptor,
                                                      SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, {}));
    return success();
}

LogicalResult ScanOutEdgesByLabelTgt::inferReturnTypes(MLIRContext* context,
                                                       std::optional<Location> location,
                                                       ScanOutEdgesByLabelTgt::Adaptor adaptor,
                                                       SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, {}));
    return success();
}

LogicalResult ScanInEdgesByLabelSrc::inferReturnTypes(MLIRContext* context,
                                                      std::optional<Location> location,
                                                      ScanInEdgesByLabelSrc::Adaptor adaptor,
                                                      SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, {}));
    return success();
}

LogicalResult ScanOutEdgesByLabelSrcTgt::inferReturnTypes(MLIRContext* context,
                                                          std::optional<Location> location,
                                                          ScanOutEdgesByLabelSrcTgt::Adaptor adaptor,
                                                          SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, {}));
    return success();
}

LogicalResult ScanInEdgesByLabelSrcTgt::inferReturnTypes(MLIRContext* context,
                                                         std::optional<Location> location,
                                                         ScanInEdgesByLabelSrcTgt::Adaptor adaptor,
                                                         SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, {}));
    return success();
}
