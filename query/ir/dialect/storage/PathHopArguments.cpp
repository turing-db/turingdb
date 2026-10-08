#include "PathHopArguments.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"

#include "StorageEnums.h"

using namespace mlir;

size_t mlir::hopWalkArgumentCount(size_t step) {
    return 2 * step + 3;
}

unsigned mlir::hopSourceArgument(size_t step) {
    return static_cast<unsigned>(2 * step);
}

unsigned mlir::hopEdgeArgument(size_t step) {
    return static_cast<unsigned>(2 * step + 1);
}

unsigned mlir::hopEndArgument(size_t step) {
    return static_cast<unsigned>(2 * step + 2);
}

size_t mlir::firstStepTakingHopArgument(size_t walkPosition) {
    return walkPosition == 0 ? 0 : (walkPosition - 1) / 2;
}

bool mlir::hasHopPredicate(MutableArrayRef<Region> hops) {
    return llvm::any_of(hops, [](Region& hop) { return !hop.empty(); });
}

void mlir::buildHopArgumentTypes(size_t step,
                                 Type nodeType,
                                 Type edgeType,
                                 ValueRange imports,
                                 llvm::SmallVectorImpl<Type>& argumentTypes) {
    const size_t walkArguments = hopWalkArgumentCount(step);
    for (size_t walkPosition = 0; walkPosition < walkArguments; walkPosition++) {
        argumentTypes.push_back(walkPosition % 2 == 0 ? nodeType : edgeType);
    }

    for (const Value import : imports) {
        argumentTypes.push_back(import.getType());
    }
}

LogicalResult mlir::verifyPathSteps(Operation* op, llvm::ArrayRef<int64_t> directions, size_t regionCount) {
    const size_t stepCount = directions.size();
    if (stepCount == 0) {
        return op->emitOpError("must take at least one step");
    }

    for (const int64_t direction : directions) {
        if (!storage::symbolizePathDirection(static_cast<uint64_t>(direction))) {
            return op->emitOpError("unknown path direction ") << direction;
        }
    }

    if (regionCount != stepCount) {
        return op->emitOpError("expects one hop region per step, but takes ") << stepCount
                                                                              << " steps and " << regionCount << " regions";
    }

    return success();
}

LogicalResult mlir::verifyHopArguments(Operation* op,
                                       Block& block,
                                       size_t step,
                                       Type nodeType,
                                       Type edgeType,
                                       ValueRange imports) {
    llvm::SmallVector<Type> expectedArguments;
    buildHopArgumentTypes(step, nodeType, edgeType, imports, expectedArguments);

    if (block.getNumArguments() != expectedArguments.size()) {
        return op->emitOpError("hop region ") << step << " must take the nodes and edges of its repetition up to "
                                                         "its end node, then one argument per hop import";
    }

    for (size_t argumentIndex = 0; argumentIndex < expectedArguments.size(); argumentIndex++) {
        if (block.getArgument(static_cast<unsigned>(argumentIndex)).getType() != expectedArguments[argumentIndex]) {
            return op->emitOpError("hop region argument ") << argumentIndex << " must be "
                                                           << expectedArguments[argumentIndex];
        }
    }

    return success();
}
