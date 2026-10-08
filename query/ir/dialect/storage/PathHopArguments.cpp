#include "PathHopArguments.h"

#include "llvm/ADT/STLExtras.h"

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
