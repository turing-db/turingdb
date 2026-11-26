#include "DBLowering.h"

#include <mlir/IR/Location.h>
#include <optional>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"

#include "PathHopArguments.h"

#include "views/GraphView.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

void DBLowering::lowerShortestPath(mlir::db::ShortestPath shortestPath) {
    const mlir::Value sourceChunk = mapValue(shortestPath.getSource());
    const mlir::Value targetChunk = mapValue(shortestPath.getTarget());
    const llvm::StringRef edgeProperty = shortestPath.getEdgeProperty();

    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    // The search accumulator is hoisted above every loop, so its node-set buffers exist
    // before the producing loops fill them and the handle dominates both updates.
    _builder.setInsertionPointToStart(_rootBlock);
    const mlir::Value state = _builder.create<nl::ShortestPathBuffer>(loc).getState();

    // The source and target sets are filled by two updates, each spliced into the loop that
    // binds its node chunk. Disconnected patterns are cross-producted upstream, so in
    // practice both chunks are bound by the same loop and both updates land in it; placing
    // the drain after that loop guarantees each set is complete before the search runs.
    setInsertionInto(ownerBlock(sourceChunk));
    _builder.create<nl::ShortestPathUpdate>(loc, state, sourceChunk, /*target=*/false);

    setInsertionInto(ownerBlock(targetChunk));
    _builder.create<nl::ShortestPathUpdate>(loc, state, targetChunk, /*target=*/true);

    // The emit phase: an nl.shortest_path source runs the weighted search once, reading
    // the edge weight through the hoisted property handle, and yields the one result row.
    // The distance chunk is the weight property's value type (resolved here against the
    // schema, since the db distance column is a none placeholder), and it is plain, not
    // nullable - the one emitted row always carries a distance. The path chunk is a path.
    const mlir::Value handle = getOrCreatePropertyTypeHandle(edgeProperty);

    if (!_view) {
        throw IRException("Lowering a shortest path needs a graph to resolve the weight property '" + edgeProperty.str() + "'");
    }

    const std::optional<PropertyType> weightType = _view->metadata().propTypes().get(edgeProperty);
    if (!weightType) {
        throw IRException("Unknown property '" + edgeProperty.str() + "'");
    }

    const mlir::Type distanceElement = valueTypeToElementType(_builder, weightType->_valueType);

    llvm::SmallVector<mlir::Type, 2> chunkTypes;
    chunkTypes.push_back(nl::ChunkType::get(context, distanceElement));
    chunkTypes.push_back(nl::ChunkType::get(context, storage::PathType::get(context)));

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);

    setInsertionInto(_rootBlock);
    nl::ShortestPath searchOp = _builder.create<nl::ShortestPath>(loc, iteratorType, state, handle);
    buildLoopForSource(searchOp.getResult(), shortestPath.getOperation());
}

mlir::Value DBLowering::buildEndNodeSet(mlir::Value endNodeColumn, mlir::Value inputChunk) {
    const mlir::Value endChunk = mapValue(endNodeColumn);
    mlir::Block* const collectBlock = ownerBlock(endChunk);

    mlir::Operation* const collectNest = topLevelNestOf(collectBlock);
    mlir::Operation* const walkNest = topLevelNestOf(ownerBlock(inputChunk));
    const bool fillsBeforeTheWalk = collectNest && walkNest && collectNest->isBeforeInBlock(walkNest);
    if (!fillsBeforeTheWalk) {
        throw IRException("The end nodes of an exploration must be bound by a loop of their own, closing before the loop the walk runs in");
    }

    const mlir::Location loc = _builder.getUnknownLoc();

    _builder.setInsertionPointToStart(_entryBlock);
    const mlir::Value state = _builder.create<nl::NodeSetBuffer>(loc).getState();

    setInsertionInto(collectBlock);
    _builder.create<nl::NodeSetCollect>(loc, state, endChunk);

    return state;
}

void DBLowering::lowerExplorePaths(mlir::db::ExplorePaths explorePaths) {
    const mlir::Value inputChunk = mapValue(explorePaths.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : explorePaths.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    mlir::Value endNodeSet;
    if (const mlir::Value endNodeColumn = explorePaths.getEndNodes()) {
        endNodeSet = buildEndNodeSet(endNodeColumn, inputChunk);
    }

    const size_t stepCount = explorePaths.getStepCount();

    llvm::SmallVector<mlir::Value, 2> edgeTypeSets;
    llvm::SmallVector<int64_t, 2> typedSteps;
    for (size_t step = 0; step < stepCount; step++) {
        if (const mlir::ArrayAttr edgeTypes = explorePaths.getStepEdgeTypes(step)) {
            edgeTypeSets.push_back(getOrCreateEdgeTypeSetHandle(edgeTypes));
            typedSteps.push_back(static_cast<int64_t>(step));
        }
    }

    const bool typesEveryStep = typedSteps.size() == stepCount || typedSteps.empty();
    const mlir::DenseI64ArrayAttr edgeTypeSteps = typesEveryStep ? mlir::DenseI64ArrayAttr() : _builder.getDenseI64ArrayAttr(typedSteps);

    llvm::SmallVector<mlir::Value, 2> importChunks;
    for (const mlir::Value importColumn : explorePaths.getHopImports()) {
        importChunks.push_back(mapValue(importColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    nl::ExplorePaths exploration = _builder.create<nl::ExplorePaths>(_builder.getUnknownLoc(),
                                                                     inputChunk,
                                                                     carriedChunks,
                                                                     endNodeSet,
                                                                     edgeTypeSets,
                                                                     importChunks,
                                                                     explorePaths.getDirectionsAttr(),
                                                                     explorePaths.getMinHopsAttr(),
                                                                     explorePaths.getMaxHopsAttr(),
                                                                     edgeTypeSteps,
                                                                     explorePaths.getEndLabelsAttr(),
                                                                     explorePaths.getHopLabelsAttr(),
                                                                     explorePaths.getEndColumnAttr(),
                                                                     explorePaths.getEndsOnSeedAttr(),
                                                                     explorePaths.getDistinctAttr(),
                                                                     static_cast<unsigned>(stepCount));

    const mlir::MutableArrayRef<mlir::Region> dbHops = explorePaths.getHops();
    const mlir::MutableArrayRef<mlir::Region> nlHops = exploration.getHops();
    for (size_t step = 0; step < stepCount; step++) {
        mlir::Region& dbHop = dbHops[step];
        if (!dbHop.empty()) {
            const mlir::OpBuilder::InsertionGuard guard(_builder);
            lowerHopRegion(dbHop.front(), nlHops[step], step, importChunks);
        }
    }

    buildLoopForSource(exploration.getResult(), explorePaths.getOperation());
}

void DBLowering::lowerHopRegion(mlir::Block& dbHop, mlir::Region& nlHop, size_t step, mlir::ValueRange imports) {
    mlir::MLIRContext* context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    const mlir::Type nodeChunk = nl::ChunkType::get(context, storage::NodeIDType::get(context));
    const mlir::Type edgeChunk = nl::ChunkType::get(context, storage::EdgeIDType::get(context));
    llvm::SmallVector<mlir::Type> argumentTypes;
    mlir::buildHopArgumentTypes(step, nodeChunk, edgeChunk, imports, argumentTypes);

    const llvm::SmallVector<mlir::Location> argumentLocations(argumentTypes.size(), loc);

    mlir::Block* nlBlock = _builder.createBlock(&nlHop, nlHop.end(), argumentTypes, argumentLocations);
    for (unsigned argumentIndex = 0; argumentIndex < argumentTypes.size(); argumentIndex++) {
        _valueMap[dbHop.getArgument(argumentIndex)] = nlBlock->getArgument(argumentIndex);
    }

    // The ops lowered into the block insert ahead of its terminator, so the block gets one
    // before the mask that the real terminator yields exists
    nl::Yield placeholder = _builder.create<nl::Yield>(loc);

    // The rows the predicate computes over are the hop's edges, so a constant it reads is
    // laid out over those rather than over the rows the walk is read on
    const mlir::Value edges = nlBlock->getArgument(mlir::hopEdgeArgument(step));
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    mlir::Block* const previousInnermostRegionBody = _innermostRegionBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _innermostLoopBody = nlBlock;
    _innermostRegionBody = nlBlock;
    _innermostCardinality = edges;

    mlir::Value mask;
    for (mlir::Operation& operation : dbHop) {
        if (mlir::db::Yield yield = mlir::dyn_cast<mlir::db::Yield>(operation)) {
            mask = mapValue(yield.getColumns().front());
        } else {
            lowerOperation(operation);
        }
    }

    _innermostLoopBody = previousInnermostLoopBody;
    _innermostRegionBody = previousInnermostRegionBody;
    _innermostCardinality = previousInnermostCardinality;

    _builder.setInsertionPoint(placeholder);
    const mlir::Value maskChunk = rowAlignedChunk(mask, edges);
    _builder.create<nl::Yield>(loc, mlir::ValueRange {maskChunk});
    eraseOp(placeholder);
}

void DBLowering::lowerExpandPath(mlir::db::ExpandPath expandPath) {
    const mlir::Value pathsChunk = mapValue(expandPath.getPaths());
    const mlir::Value seedsChunk = expandPath.getSrcids() ? mapValue(expandPath.getSrcids()) : mlir::Value();

    setInsertionInto(ownerBlock(pathsChunk));

    const auto resultColumn = mlir::cast<mlir::db::ColumnType>(expandPath.getResult().getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultColumn.getType());

    nl::ExpandPath expansion = _builder.create<nl::ExpandPath>(_builder.getUnknownLoc(),
                                                               resultType,
                                                               pathsChunk,
                                                               seedsChunk,
                                                               expandPath.getKindAttr(),
                                                               expandPath.getReversedAttr(),
                                                               expandPath.getStepAttr(),
                                                               expandPath.getStepsAttr());
    _valueMap[expandPath.getResult()] = expansion.getResult();
}

void DBLowering::lowerPathLength(mlir::db::PathLength pathLength) {
    const mlir::Value pathsChunk = mapValue(pathLength.getPaths());

    setInsertionInto(ownerBlock(pathsChunk));

    mlir::MLIRContext* context = _builder.getContext();
    const nl::ChunkType resultType = nl::ChunkType::get(context, mlir::IntegerType::get(context, 64, mlir::IntegerType::Unsigned));

    nl::PathLength length = _builder.create<nl::PathLength>(_builder.getUnknownLoc(),
                                                            resultType,
                                                            pathsChunk,
                                                            pathLength.getStepsAttr(),
                                                            pathLength.getNodesAttr());
    _valueMap[pathLength.getResult()] = length.getResult();
}

void DBLowering::lowerPathElements(mlir::db::PathElements pathElements) {
    const mlir::Value pathChunk = mapValue(pathElements.getPath());

    setInsertionInto(ownerBlock(pathChunk));

    const auto resultColumn = mlir::cast<mlir::db::ColumnType>(pathElements.getResult().getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultColumn.getType());

    nl::PathElements elements = _builder.create<nl::PathElements>(_builder.getUnknownLoc(),
                                                                  resultType,
                                                                  pathChunk,
                                                                  pathElements.getKind());
    _valueMap[pathElements.getResult()] = elements.getResult();
}

void DBLowering::lowerMakePath(mlir::db::MakePath makePath) {
    llvm::SmallVector<mlir::Value, 4> entityChunks;
    for (const mlir::Value entityColumn : makePath.getEntities()) {
        entityChunks.push_back(mapValue(entityColumn));
    }

    setInsertionForNaryOp(entityChunks);

    const auto resultColumn = mlir::cast<mlir::db::ColumnType>(makePath.getResult().getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultColumn.getType());

    nl::MakePath path = _builder.create<nl::MakePath>(_builder.getUnknownLoc(),
                                                      resultType,
                                                      entityChunks,
                                                      makePath.getReversedPathsAttr());
    _valueMap[makePath.getResult()] = path.getResult();
}
