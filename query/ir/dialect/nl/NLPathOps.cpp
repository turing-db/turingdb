#include "NLOps.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "StorageEnums.h"
#include "PathHopArguments.h"
#include "MakePathShape.h"
#include "NLChunkTypes.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

namespace {

// A path exploration exposes the seed node IDs, the end node IDs and the path handles,
// followed by one filtered chunk per carried column, mirroring db.explore_paths
Type getPathIteratorType(MLIRContext* context, TypeRange carriedChunkTypes) {
    const Type seeds = getNodeIDChunkType(context);
    const Type ends = getNodeIDChunkType(context);
    const Type paths = ChunkType::get(context, storage::PathRefType::get(context));

    llvm::SmallVector<Type> chunkTypes {seeds, ends, paths};
    chunkTypes.append(carriedChunkTypes.begin(), carriedChunkTypes.end());

    return IteratorType::get(context, chunkTypes);
}

}

LogicalResult ExplorePaths::inferReturnTypes(MLIRContext* context,
                                             std::optional<Location> location,
                                             ExplorePaths::Adaptor adaptor,
                                             SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getPathIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

storage::PathDirection ExplorePaths::getStepDirection(size_t step) {
    return static_cast<storage::PathDirection>(getDirections()[step]);
}

Value ExplorePaths::getStepEdgeTypes(size_t step) {
    const OperandRange edgeTypes = getEdgeTypes();
    const std::optional<llvm::ArrayRef<int64_t>> typedSteps = getEdgeTypeSteps();
    if (!typedSteps) {
        return edgeTypes.empty() ? Value() : edgeTypes[step];
    }

    const auto typed = llvm::find(*typedSteps, static_cast<int64_t>(step));

    return typed == typedSteps->end() ? Value() : edgeTypes[typed - typedSteps->begin()];
}

// The hop regions mirror the db op's: one block per step over the node and edge chunks of
// the repetition up to its hop, ending in an nl.yield of one mask chunk
LogicalResult ExplorePaths::verify() {
    const std::optional<uint64_t> maxHops = getMaxHops();
    if (maxHops && *maxHops < getMinHops()) {
        return emitOpError("max_hops must be at least min_hops");
    }

    MutableArrayRef<Region> hops = getHops();
    if (failed(verifyPathSteps(getOperation(), getDirections(), hops.size()))) {
        return failure();
    }

    const size_t stepCount = getStepCount();
    const size_t edgeTypeCount = getEdgeTypes().size();
    if (const std::optional<llvm::ArrayRef<int64_t>> typedSteps = getEdgeTypeSteps()) {
        if (typedSteps->size() != edgeTypeCount) {
            return emitOpError("edge_type_steps must name one step per edge type handle");
        }

        for (size_t index = 0; index < typedSteps->size(); index++) {
            const int64_t step = (*typedSteps)[index];
            const bool ascends = index == 0 || (*typedSteps)[index - 1] < step;
            const bool namesAStep = step >= 0 && static_cast<size_t>(step) < stepCount;
            if (!namesAStep || !ascends) {
                return emitOpError("edge_type_steps must name steps in ascending order");
            }
        }
    } else if (edgeTypeCount != 0 && edgeTypeCount != stepCount) {
        return emitOpError("expects one edge type handle per step, or edge_type_steps naming theirs");
    }

    const std::optional<ArrayAttr> endLabels = getEndLabels();
    if (endLabels && endLabels->empty()) {
        return emitOpError("end_labels must name at least one label");
    }

    const std::optional<ArrayAttr> hopLabels = getHopLabels();
    if (hopLabels && hopLabels->size() != stepCount) {
        return emitOpError("hop_labels must hold one list per step");
    }

    const std::optional<uint64_t> endColumn = getEndColumn();
    if (endColumn && *endColumn >= getColumnsToFilter().size()) {
        return emitOpError("end_column ") << *endColumn << " is not a carried column";
    }

    if (getEndsOnSeed() && endColumn) {
        return emitOpError("ends_on_seed names the end already named by end_column");
    }

    if (getEndNodes() && (endColumn || getEndsOnSeed())) {
        return emitOpError("end_nodes names the end already named by end_column or ends_on_seed");
    }

    const OperandRange imports = getHopImports();

    const bool hasPredicate = hasHopPredicate(hops);
    if (!hasPredicate) {
        if (!imports.empty()) {
            return emitOpError("hop imports without a hop region to read them");
        }

        return success();
    }

    if (getDistinct() && !imports.empty()) {
        return emitOpError("a distinct search expands many seeds at one level, so it cannot "
                           "read a chunk that holds one value per seed");
    }

    MLIRContext* const context = getContext();
    const Type nodeChunk = getNodeIDChunkType(context);
    const Type edgeChunk = getEdgeIDChunkType(context);

    for (size_t step = 0; step < stepCount; step++) {
        Region& hop = hops[step];
        if (hop.empty()) {
            continue;
        }

        Block& block = hop.front();
        if (failed(verifyHopArguments(getOperation(), block, step, nodeChunk, edgeChunk, imports))) {
            return failure();
        }

        Yield yield = dyn_cast_or_null<Yield>(block.empty() ? nullptr : &block.back());
        if (!yield || yield.getColumns().size() != 1) {
            return emitOpError("hop region must end with an nl.yield of one mask chunk");
        }
    }

    return success();
}

LogicalResult MakePath::verify() {
    llvm::SmallVector<Type> elementTypes;
    for (const Value entity : getEntities()) {
        const auto chunk = dyn_cast<ChunkType>(entity.getType());
        elementTypes.push_back(chunk ? chunk.getElementType() : Type());
    }

    return verifyMakePath(getOperation(), elementTypes, getReversedPaths());
}
