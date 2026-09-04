#include "MergePatternShape.h"

#include <optional>
#include <stdint.h>

#include "mlir/IR/Operation.h"

#include "StorageEnums.h"

using namespace mlir;

namespace storage = mlir::storage;

LogicalResult mlir::verifyMergePattern(Operation* op,
                                       ArrayAttr nodeLabels,
                                       ArrayAttr nodePropNames,
                                       ArrayAttr edgeTypes,
                                       ArrayAttr edgePropNames,
                                       DenseI64ArrayAttr edgeDirections,
                                       DenseI64ArrayAttr pendingNodes,
                                       size_t boundNodes,
                                       size_t boundPending,
                                       size_t nodePropValues,
                                       size_t edgePropValues) {
    const size_t nodeCount = nodeLabels.size();
    if (nodeCount == 0) {
        return op->emitOpError("requires at least one chain node");
    }

    if (nodePropNames.size() != nodeCount) {
        return op->emitOpError("props must give one name list per chain node, but has ")
               << nodePropNames.size() << " lists for " << nodeCount << " nodes";
    }

    const size_t hopCount = nodeCount - 1;
    if (edgeTypes.size() != hopCount) {
        return op->emitOpError("edges must give one type per hop, but has ")
               << edgeTypes.size() << " types for " << hopCount << " hops";
    }

    if (edgePropNames.size() != hopCount) {
        return op->emitOpError("props must give one name list per hop, but has ")
               << edgePropNames.size() << " lists for " << hopCount << " hops";
    }

    if (edgeDirections.size() != static_cast<int64_t>(hopCount)) {
        return op->emitOpError("dirs must give one direction per hop, but has ")
               << edgeDirections.size() << " directions for " << hopCount << " hops";
    }

    for (const int64_t raw : edgeDirections.asArrayRef()) {
        if (!storage::symbolizeEdgeDirection(static_cast<uint64_t>(raw))) {
            return op->emitOpError("carries an unknown edge direction ") << raw;
        }
    }

    size_t expectedBound = 0;
    size_t expectedNodeValues = 0;
    for (size_t node = 0; node < nodeCount; node++) {
        const size_t labels = cast<ArrayAttr>(nodeLabels[node]).size();
        const size_t props = cast<ArrayAttr>(nodePropNames[node]).size();

        if (labels == 0) {
            expectedBound++;

            if (props != 0) {
                return op->emitOpError("chain node ") << node << " is bound, so it cannot carry "
                       << props << " property names";
            }
        }

        expectedNodeValues += props;
    }

    if (boundNodes != expectedBound) {
        return op->emitOpError("bound must give one column per bound chain node, but has ")
               << boundNodes << " columns for " << expectedBound << " bound nodes";
    }

    if (pendingNodes.size() != static_cast<int64_t>(boundPending)) {
        return op->emitOpError("pending must name the chain node of each of its masks, but has ")
               << pendingNodes.size() << " names for " << boundPending << " masks";
    }

    int64_t previousPendingNode = -1;
    for (const int64_t pendingNode : pendingNodes.asArrayRef()) {
        if (pendingNode <= previousPendingNode) {
            return op->emitOpError("pending must name its chain nodes in increasing order, but "
                                   "names ")
                   << pendingNode << " after " << previousPendingNode;
        }

        if (pendingNode >= static_cast<int64_t>(nodeCount)) {
            return op->emitOpError("pending names chain node ") << pendingNode
                   << ", past the " << nodeCount << " the pattern has";
        }

        if (!cast<ArrayAttr>(nodeLabels[pendingNode]).empty()) {
            return op->emitOpError("pending names chain node ") << pendingNode
                   << ", which is not bound and so has no rows to be pending";
        }

        previousPendingNode = pendingNode;
    }

    if (nodePropValues != expectedNodeValues) {
        return op->emitOpError("values must give one node column per property name, but has ")
               << nodePropValues << " columns for " << expectedNodeValues << " names";
    }

    size_t expectedEdgeValues = 0;
    for (size_t hop = 0; hop < hopCount; hop++) {
        if (cast<StringAttr>(edgeTypes[hop]).getValue().empty()) {
            return op->emitOpError("hop ") << hop << " requires a non-empty edge type";
        }

        expectedEdgeValues += cast<ArrayAttr>(edgePropNames[hop]).size();
    }

    if (edgePropValues != expectedEdgeValues) {
        return op->emitOpError("values must give one hop column per property name, but has ")
               << edgePropValues << " columns for " << expectedEdgeValues << " names";
    }

    return success();
}

size_t mlir::mergeMatchedNodeCount(ArrayAttr nodeLabels) {
    size_t matched = 0;
    for (const Attribute labels : nodeLabels) {
        if (!cast<ArrayAttr>(labels).empty()) {
            matched++;
        }
    }

    return matched;
}

size_t mlir::mergeResultCount(ArrayAttr nodeLabels, size_t carriedColumns) {
    const size_t nodeCount = nodeLabels.size();
    const size_t hopCount = nodeCount > 0 ? nodeCount - 1 : 0;

    // An ID column and a pending mask per hop and per node the merge looks up, then the
    // created mask
    return 2 * (mergeMatchedNodeCount(nodeLabels) + hopCount) + 1 + carriedColumns;
}
