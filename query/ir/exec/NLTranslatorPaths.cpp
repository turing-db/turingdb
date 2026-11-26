#include "NLTranslator.h"

#include <optional>
#include <span>

#include "NLOps.h"
#include "StorageEnums.h"

#include "PathHopArguments.h"
#include "mlir/IR/Block.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "list/ListView.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"

#include "NLRuntimeProperty.h"
#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "LocalMemory.h"
#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

llvm::StringRef propertyTypeName(mlir::Value handle) {
    nl::GetPropertyType handleOp = handle.getDefiningOp<nl::GetPropertyType>();
    if (!handleOp) {
        throw IRException("edge_property operand must come from nl.get_property_type");
    }

    return handleOp.getName();
}

PathExplorationDir toPathExplorationDir(storage::PathDirection direction) {
    switch (direction) {
        case storage::PathDirection::Forward:
            return PathExplorationDir::FORWARD;
        break;

        case storage::PathDirection::Backward:
            return PathExplorationDir::BACKWARD;
        break;

        case storage::PathDirection::Both:
            return PathExplorationDir::BOTH;
        break;
    }

    throw IRException("Unknown path direction");
}

}

void NLTranslator::translateExploreStep(nl::ExplorePaths explorePaths,
                                        size_t step,
                                        llvm::ArrayRef<mlir::Value> hopImports,
                                        NLExplorePathsLoopData* loopData) {
    // The types are resolved as translateEdgeLoop resolves a by-type hop's: a name absent
    // from the schema drops out of the set, and a set left empty that way marks the step
    // unmatchable
    llvm::SmallVector<llvm::StringRef, 4> edgeTypeNamesOfStep;
    if (const mlir::Value handle = explorePaths.getStepEdgeTypes(step)) {
        resolveEdgeTypeNames(handle, edgeTypeNamesOfStep);
    }

    const bool filtersByType = !edgeTypeNamesOfStep.empty();
    llvm::SmallVector<EdgeTypeID, 4> edgeTypes;
    bool matchable = true;
    if (filtersByType) {
        resolveEdgeTypes(edgeTypeNamesOfStep, edgeTypes);
        matchable = !edgeTypes.empty();
    }

    const PathExplorationDir direction = toPathExplorationDir(explorePaths.getStepDirection(step));
    NLExploreStep* exploreStep = loopData->addStep(direction, filtersByType, edgeTypes, matchable);

    if (const std::optional<mlir::ArrayAttr> stepLabels = explorePaths.getHopLabels()) {
        const mlir::ArrayAttr labelsOfStep = mlir::cast<mlir::ArrayAttr>((*stepLabels)[step]);

        llvm::SmallVector<llvm::StringRef, 4> names;
        resolveLabelNames(labelsOfStep, names);

        if (!names.empty()) {
            LabelSet hopLabels;
            const bool hopLabelsMatchable = resolveLabelSet(names, hopLabels);
            exploreStep->setHopLabels(hopLabels, hopLabelsMatchable);
        }
    }

    mlir::Region& hop = explorePaths.getHops()[step];
    if (hop.empty()) {
        return;
    }

    mlir::Block& hopBlock = hop.front();
    const size_t chunkSize = _program->getChunkSize();

    // The hop's own source, edge and end are always filled; the nodes and edges the
    // repetition took before it only when the predicate reads them
    const size_t hopArguments = mlir::hopWalkArgumentCount(step);
    for (size_t argumentIndex = 0; argumentIndex < hopArguments; argumentIndex++) {
        const mlir::BlockArgument argument = hopBlock.getArgument(static_cast<unsigned>(argumentIndex));
        const bool ofTheHop = argumentIndex >= mlir::hopSourceArgument(step);
        const bool isEdge = argumentIndex % 2 == 1;

        if (!ofTheHop && argument.use_empty()) {
            if (isEdge) {
                exploreStep->addHopEdge(nullptr);
            } else {
                exploreStep->addHopNode(nullptr);
            }

            continue;
        }

        if (isEdge) {
            ColumnEdgeIDs* edges = _memory->alloc<ColumnEdgeIDs>();
            edges->reserve(chunkSize);
            _valueSlots[argument] = edges;
            exploreStep->addHopEdge(edges);
        } else {
            ColumnNodeIDs* nodes = _memory->alloc<ColumnNodeIDs>();
            nodes->reserve(chunkSize);
            _valueSlots[argument] = nodes;
            exploreStep->addHopNode(nodes);
        }
    }

    for (size_t importIndex = 0; importIndex < hopImports.size(); importIndex++) {
        const mlir::BlockArgument importArgument = hopBlock.getArgument(static_cast<unsigned>(hopArguments + importIndex));
        if (importArgument.use_empty()) {
            continue;
        }

        const mlir::Value importValue = hopImports[importIndex];
        const Column* importColumn = getColumn(importValue);

        Column* importChunk = nullptr;
        NLGatherFunction gather = nullptr;
        if (isConstantColumn(importColumn)) {
            importChunk = _memory->allocSame(importColumn);
            gather = NLExecutor::selectConstGatherFunction();
        } else {
            importChunk = allocColumnForChunkType(importArgument.getType());
            gather = selectGatherForChunkType(importValue.getType());
        }
        _valueSlots[importArgument] = importChunk;

        exploreStep->addHopImport(NLHopImport {importColumn, importChunk, gather});
    }

    translateBlock(hopBlock, exploreStep->getHopStmts());

    nl::Yield yield = mlir::cast<nl::Yield>(hopBlock.getTerminator());
    const mlir::Value mask = yield.getColumns().front();
    const Column* maskColumn = getColumn(mask);

    exploreStep->setHopFilter(maskColumn, NLExecutor::selectMaskSurvivorFunction(truthColumnKindOf(mask.getType(), maskColumn)));
}

void NLTranslator::translateExpandPath(nl::ExpandPath expand, NLStmtContainer* body) {
    const ColumnVector<PathRef>* paths = static_cast<const ColumnVector<PathRef>*>(getColumn(expand.getPaths()));
    const ColumnNodeIDs* seeds = nullptr;
    if (const mlir::Value seedsValue = expand.getSrcids()) {
        seeds = static_cast<const ColumnNodeIDs*>(getColumn(seedsValue));
    }

    ColumnVector<ListView>* output = static_cast<ColumnVector<ListView>*>(allocListColumn());
    _valueSlots[expand.getResult()] = output;

    PathExpansionKind kind = PathExpansionKind::Edges;
    switch (expand.getKind()) {
        case storage::PathExpansionKind::Edges:
            kind = PathExpansionKind::Edges;
        break;

        case storage::PathExpansionKind::Sources:
            kind = PathExpansionKind::Sources;
        break;

        case storage::PathExpansionKind::Ends:
            kind = PathExpansionKind::Ends;
        break;

        case storage::PathExpansionKind::Nodes:
            kind = PathExpansionKind::Nodes;
        break;
    }

    const PathHopStride hops {._offset = expand.getStep().value_or(0), ._stride = expand.getSteps().value_or(1)};

    NLExpandPathData* data = _program->allocFunctionData<NLExpandPathData>(paths,
                                                                           seeds,
                                                                           output,
                                                                           kind,
                                                                           expand.getReversed(),
                                                                           hops,
                                                                           &_memory->pathTrie(),
                                                                           &_program->allocStepBuffers()->listBuffer());
    body->emplaceStmt(&NLExecutor::runExpandPath, data);
}

void NLTranslator::translatePathLength(nl::PathLength length, NLStmtContainer* body) {
    const ColumnVector<PathRef>* paths = static_cast<const ColumnVector<PathRef>*>(getColumn(length.getPaths()));

    ColumnVector<uint64_t>* output = allocCountColumn();
    _valueSlots[length.getResult()] = output;

    const size_t steps = length.getSteps().value_or(1);
    NLPathLengthData* data = _program->allocFunctionData<NLPathLengthData>(paths,
                                                                           output,
                                                                           steps,
                                                                           length.getNodes(),
                                                                           &_memory->pathTrie());
    body->emplaceStmt(&NLExecutor::runPathLength, data);
}

void NLTranslator::translatePathElements(nl::PathElements elements, NLStmtContainer* body) {
    const Column* paths = getColumn(elements.getPath());

    PathElementsKind kind = PathElementsKind::Nodes;
    Column* output = nullptr;
    switch (elements.getKind()) {
        case storage::PathElementsKind::Nodes:
            kind = PathElementsKind::Nodes;
            output = allocOptListColumn();
        break;

        case storage::PathElementsKind::Relationships:
            kind = PathElementsKind::Relationships;
            output = allocOptListColumn();
        break;

        case storage::PathElementsKind::Length:
            kind = PathElementsKind::Length;
            output = allocOptColumnForValueType(ValueType::UInt64);
        break;
    }

    _valueSlots[elements.getResult()] = output;

    NLPathElementsData* data = _program->allocFunctionData<NLPathElementsData>(paths,
                                                                               NLExecutor::selectPathRead(paths),
                                                                               output,
                                                                               kind,
                                                                               &_program->allocStepBuffers()->listBuffer());
    body->emplaceStmt(&NLExecutor::runPathElements, data);
}

void NLTranslator::translateMakePath(nl::MakePath makePath, NLStmtContainer* body) {
    ColumnVector<GraphPath>* output = static_cast<ColumnVector<GraphPath>*>(allocColumnForChunkType(makePath.getResult().getType()));
    _valueSlots[makePath.getResult()] = output;

    NLMakePathData* data = _program->allocFunctionData<NLMakePathData>(output, &_memory->pathTrie());

    llvm::SmallVector<bool> reversedPaths(makePath.getEntities().size(), false);
    if (const std::optional<mlir::ArrayAttr> reversed = makePath.getReversedPaths()) {
        for (const mlir::Attribute entry : *reversed) {
            reversedPaths[mlir::cast<mlir::IntegerAttr>(entry).getInt()] = true;
        }
    }

    const mlir::OperandRange entities = makePath.getEntities();
    size_t entityIndex = 0;
    while (entityIndex < entities.size()) {
        const mlir::Value entityValue = entities[entityIndex];
        NLPathStep step;

        switch (getChunkKind(entityValue.getType())) {
            case NLChunkKind::NodeID:
                step._kind = NLPathStep::Kind::Start;
                step._nodes = static_cast<const ColumnNodeIDs*>(getColumn(entityValue));
                entityIndex += 1;
            break;

            case NLChunkKind::EdgeID:
                step._kind = NLPathStep::Kind::Hop;
                step._edges = static_cast<const ColumnEdgeIDs*>(getColumn(entityValue));
                step._nodes = static_cast<const ColumnNodeIDs*>(getColumn(entities[entityIndex + 1]));
                entityIndex += 2;
            break;

            case NLChunkKind::PathRef:
                step._walks = static_cast<const ColumnVector<PathRef>*>(getColumn(entityValue));
                if (reversedPaths[entityIndex]) {
                    step._kind = NLPathStep::Kind::ReversedWalk;
                    step._nodes = static_cast<const ColumnNodeIDs*>(getColumn(entities[entityIndex + 1]));
                    entityIndex += 2;
                } else {
                    step._kind = NLPathStep::Kind::Walk;
                    entityIndex += 1;
                }
            break;

            default:
                throw IRException("nl.make_path runs through nodes, edges and paths alone");
            break;
        }

        data->addStep(step);
    }

    body->emplaceStmt(&NLExecutor::runMakePath, data);
}

PropertyType NLTranslator::resolveShortestPathWeight(mlir::Value stateHandle) const {
    nl::ShortestPath drain;
    for (mlir::Operation* user : stateHandle.getUsers()) {
        if (nl::ShortestPath found = mlir::dyn_cast<nl::ShortestPath>(user)) {
            drain = found;
            break;
        }
    }

    if (!drain) {
        throw IRException("nl.shortest_path_buffer must feed an nl.shortest_path drain");
    }

    const llvm::StringRef name = propertyTypeName(drain.getEdgeProperty());
    const std::optional<PropertyType> weight = _view->metadata().propTypes().get(name);
    if (!weight) {
        throw IRException("Unknown property '" + name.str() + "'");
    }

    return *weight;
}

NLShortestPathState* NLTranslator::allocShortestPathStateFor(const PropertyType& weight) {
    switch (weight._valueType) {
        case ValueType::Double:
            return _program->allocShortestPathState<types::Double>(weight._valueType, weight._id);
        break;

        case ValueType::Int64:
            return _program->allocShortestPathState<types::Int64>(weight._valueType, weight._id);
        break;

        case ValueType::UInt64:
            return _program->allocShortestPathState<types::UInt64>(weight._valueType, weight._id);
        break;

        default:
            throw IRException("SHORTESTPATH weight must be an Int64, UInt64 or Double property");
        break;
    }
}

void NLTranslator::translateNodeSetBuffer(nl::NodeSetBuffer buffer, NLStmtContainer* body) {
    NLNodeSetState* state = _program->allocNodeSetState();
    _nodeSetStates[buffer.getState()] = state;

    // The reset empties the set each time the block holding this nl.node_set_buffer runs
    NLNodeSetResetData* resetData = _program->allocFunctionData<NLNodeSetResetData>(state);
    body->emplaceStmt(&NLExecutor::runNodeSetReset, resetData);
}

void NLTranslator::translateNodeSetCollect(nl::NodeSetCollect collect, NLStmtContainer* body) {
    NLNodeSetState* state = nodeSetStateFor(collect.getState());

    const ColumnNodeIDs* nodes = static_cast<const ColumnNodeIDs*>(getColumn(collect.getNodes()));

    NLNodeSetCollectData* data = _program->allocFunctionData<NLNodeSetCollectData>(state, nodes);
    body->emplaceStmt(&NLExecutor::runNodeSetCollect, data);
}

NLNodeSetState* NLTranslator::nodeSetStateFor(mlir::Value handle) const {
    const auto stateIt = _nodeSetStates.find(handle);
    if (stateIt == _nodeSetStates.end()) {
        throw IRException("node set handle must be produced by an nl.node_set_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateShortestPathBuffer(nl::ShortestPathBuffer buffer, NLStmtContainer* body) {
    // Resolve the weight before the accumulator is built, so it is typed by the weight and the
    // updates can seed the frontier directly. The weight type is chosen once, here.
    const PropertyType weight = resolveShortestPathWeight(buffer.getState());
    NLShortestPathState* state = allocShortestPathStateFor(weight);
    _shortestPathStates[buffer.getState()] = state;

    // The reset empties the accumulator each time the block holding this
    // nl.shortest_path_buffer runs: once at function scope.
    NLShortestPathResetData* resetData = _program->allocFunctionData<NLShortestPathResetData>(state);
    body->emplaceStmt(&NLExecutor::runShortestPathReset, resetData);
}

void NLTranslator::translateShortestPathUpdate(nl::ShortestPathUpdate update, NLStmtContainer* body) {
    NLShortestPathState* state = shortestPathStateFor(update.getState());

    const ColumnNodeIDs* nodes = static_cast<const ColumnNodeIDs*>(getColumn(update.getNodes()));

    NLShortestPathUpdateData* data = _program->allocFunctionData<NLShortestPathUpdateData>(state, nodes, update.getTarget());
    body->emplaceStmt(&NLExecutor::runShortestPathUpdate, data);
}

NLShortestPathState* NLTranslator::shortestPathStateFor(mlir::Value handle) const {
    const auto stateIt = _shortestPathStates.find(handle);
    if (stateIt == _shortestPathStates.end()) {
        throw IRException("shortest path handle must be produced by an nl.shortest_path_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateShortestPathLoop(const IteratorConfig& config,
                                             mlir::Block& loopBody,
                                             NLStmtContainer* body) {
    NLShortestPathState* state = config._shortestPathState;
    if (!state) {
        throw IRException("nl.shortest_path iterator must carry a shortest-path accumulator");
    }

    if (loopBody.getNumArguments() != 2) {
        throw IRException("nl.shortest_path loop must bind a distance and a path variable");
    }

    NLShortestPathLoopData* loopData = _program->allocFunctionData<NLShortestPathLoopData>(state);

    const mlir::Value distanceVariable = loopBody.getArgument(0);
    const mlir::Value pathVariable = loopBody.getArgument(1);

    Column* distanceOutput = allocColumnForChunkType(distanceVariable.getType());
    _valueSlots[distanceVariable] = distanceOutput;
    loopData->setDistanceOutput(distanceOutput);

    ColumnVector<GraphPath>* pathOutput = static_cast<ColumnVector<GraphPath>*>(allocColumnForChunkType(pathVariable.getType()));
    _valueSlots[pathVariable] = pathOutput;
    loopData->setPathOutput(pathOutput);

    ColumnNodeIDs* expansionInput = _memory->alloc<ColumnNodeIDs>();
    ColumnEdgeIDs* expandedEdges = _memory->alloc<ColumnEdgeIDs>();
    ColumnNodeIDs* expandedTargets = _memory->alloc<ColumnNodeIDs>();
    Column* weightValues = allocOptColumnForValueType(state->getValueType());
    loopData->setExpansionScratch(expansionInput, expandedEdges, expandedTargets, weightValues);

    body->emplaceStmt(&NLExecutor::runShortestPathLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}
