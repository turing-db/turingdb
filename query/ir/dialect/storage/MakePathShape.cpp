#include "MakePathShape.h"

#include <stddef.h>

#include "mlir/IR/Operation.h"
#include "llvm/ADT/SmallVector.h"

#include "StorageTypes.h"

using namespace mlir;

namespace storage = mlir::storage;

LogicalResult mlir::verifyMakePath(Operation* op,
                                   llvm::ArrayRef<Type> elementTypes,
                                   std::optional<ArrayAttr> reversedPaths) {
    if (elementTypes.empty()) {
        return op->emitOpError("a path runs through at least one node");
    }

    llvm::SmallVector<bool> reversed(elementTypes.size(), false);
    if (reversedPaths) {
        for (const Attribute entry : *reversedPaths) {
            const auto index = dyn_cast<IntegerAttr>(entry);
            if (!index || index.getInt() < 0 || static_cast<size_t>(index.getInt()) >= elementTypes.size()) {
                return op->emitOpError("reversed_paths names no operand of this path");
            }

            const size_t operandIndex = static_cast<size_t>(index.getInt());
            if (!isa_and_nonnull<storage::PathRefType>(elementTypes[operandIndex])) {
                return op->emitOpError("reversed_paths names operand ") << operandIndex << ", which is no path column";
            }

            reversed[operandIndex] = true;
        }
    }

    // A walk taken against the pattern opens on the node it ended at, so it may open the
    // path itself; one taken with the pattern opens on its first edge and may not
    const Type openingElement = elementTypes.front();
    const bool opensOnANode = isa_and_nonnull<storage::NodeIDType>(openingElement);
    const bool opensOnAReversedWalk = isa_and_nonnull<storage::PathRefType>(openingElement) && reversed.front();
    if (!opensOnANode && !opensOnAReversedWalk) {
        return op->emitOpError("a path opens on a node column");
    }

    size_t entityIndex = opensOnANode ? 1 : 0;
    while (entityIndex < elementTypes.size()) {
        const Type element = elementTypes[entityIndex];

        if (isa_and_nonnull<storage::PathRefType>(element)) {
            // A reversed walk closes on the edge it took first, so the node it seeded from
            // is written after it; a forward one closes on a node of its own
            if (!reversed[entityIndex]) {
                entityIndex++;
                continue;
            }

            const bool closesOnANode = entityIndex + 1 < elementTypes.size()
                                    && isa_and_nonnull<storage::NodeIDType>(elementTypes[entityIndex + 1]);
            if (!closesOnANode) {
                return op->emitOpError("the reversed path at operand ") << entityIndex << " closes on no node column";
            }

            entityIndex += 2;
            continue;
        }

        if (!isa_and_nonnull<storage::EdgeIDType>(element)) {
            return op->emitOpError("a hop of a path is an edge column or a path column, at operand ") << entityIndex;
        }

        const bool landsOnANode = entityIndex + 1 < elementTypes.size()
                               && isa_and_nonnull<storage::NodeIDType>(elementTypes[entityIndex + 1]);
        if (!landsOnANode) {
            return op->emitOpError("the edge column at operand ") << entityIndex << " lands on no node column";
        }

        entityIndex += 2;
    }

    return success();
}
