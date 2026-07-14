#include "NLTranslator.h"

#include <optional>

#include <spdlog/fmt/bundled/format.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Verifier.h"

#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"

#include "NLExecutor.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// The edge type name carried by the nl.get_edge_type handle a by-type hop's
// edge_type operand names. The name lives on the handle op, not the hop, so it is
// resolved once above the loops; a hop reads it back through its operand here (the
// same way translatePropertyFetch reads a property name off nl.get_property_type).
llvm::StringRef edgeTypeName(mlir::Value handle) {
    nl::GetEdgeType handleOp = handle.getDefiningOp<nl::GetEdgeType>();
    if (!handleOp) {
        throw IRException("edge_type operand must come from nl.get_edge_type");
    }

    return handleOp.getName();
}

// The with-null fetch handler for a property's value type, on the node side
// when isNode is true and the edge side otherwise. Selecting it here keeps the
// value-type dispatch with the rest of translation; the handler bodies live in
// NLExecutor.
NLHandlerFunction selectPropertyFetchHandler(bool isNode, ValueType valueType) {
    // The handler is the runPropertyFetch instantiation for the node or edge ID
    // type and the property's value type; ValueTypeDispatcher maps the runtime
    // value type to that compile-time type, so the per-type fan-out lives in one
    // shared place rather than a switch repeated across the lowering.
    NLHandlerFunction handler = nullptr;
    const auto select = [&]<SupportedType T>() {
        handler = isNode ? &NLExecutor::runPropertyFetch<NodeID, T>
                         : &NLExecutor::runPropertyFetch<EdgeID, T>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return handler;
}

// Reverse of DBLowering's valueTypeToElementType: recover the storage value
// type baked into a nullable value chunk's element type. A cross product result
// keeps its input chunk's element type, so a crossed property column carries the
// same !storage.nullable<...> the fetch produced, and this maps it back to allocate
// the matching nullable value column and pick the broadcast.
ValueType valueTypeFromElementType(mlir::Type elementType) {
    if (mlir::isa<storage::StringType>(elementType)) {
        return ValueType::String;
    } else if (mlir::isa<storage::EmbeddingType>(elementType)) {
        return ValueType::Embedding;
    } else if (mlir::isa<mlir::Float64Type>(elementType)) {
        return ValueType::Double;
    } else if (const auto intType = mlir::dyn_cast<mlir::IntegerType>(elementType)) {
        const bool isBool = intType.getWidth() == 1;
        const bool isUnsigned = intType.isUnsigned();
        if (isBool) {
            return ValueType::Bool;
        } else if (isUnsigned) {
            return ValueType::UInt64;
        } else {
            return ValueType::Int64;
        }
    }

    throw IRException("Unsupported nullable value chunk element type");
}

template <SupportedType T>
typename T::Primitive constantValueAs(mlir::TypedAttr value) {
    if constexpr (std::same_as<T, types::Int64>) {
        return mlir::cast<mlir::IntegerAttr>(value).getInt();
    } else if constexpr (std::same_as<T, types::UInt64>) {
        return mlir::cast<mlir::IntegerAttr>(value).getValue().getZExtValue();
    } else if constexpr (std::same_as<T, types::Double>) {
        return mlir::cast<mlir::FloatAttr>(value).getValueAsDouble();
    } else if constexpr (std::same_as<T, types::Bool>) {
        return CustomBool(mlir::cast<mlir::IntegerAttr>(value).getInt() != 0);
    } else { // string and embedding not yet supported
        throw IRException("Unsupported constant value type");
    }
}

// The runtime reduction the interpreter dispatches on, from the MLIR one the op
// carries. The two enums are kept separate so the runtime (NLProgram / NLExecutor)
// stays free of the MLIR dialect headers; this is the one place they meet.
AggregateKind toRuntimeAggregateKind(storage::AggregateKind kind) {
    switch (kind) {
        case storage::AggregateKind::Sum:
            return AggregateKind::Sum;
        break;

        case storage::AggregateKind::Min:
            return AggregateKind::Min;
        break;

        case storage::AggregateKind::Max:
            return AggregateKind::Max;
        break;

        case storage::AggregateKind::Avg:
            return AggregateKind::Avg;
        break;
    }

    throw IRException("Unhandled aggregate kind");
}

// The value type wrapped by a nullable value chunk (!nl.chunk<!storage.nullable<T>>).
// An aggregate reduces property values, so its input and result are always such
// chunks; a chunk that is not a nullable value chunk (an ID chunk) is rejected.
ValueType nullableChunkValueType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const auto nullableType = mlir::dyn_cast<storage::NullableType>(chunk.getElementType());
    if (!nullableType) {
        throw IRException("aggregate requires a nullable value chunk");
    }

    return valueTypeFromElementType(nullableType.getValueType());
}

bool isConstantLike(mlir::Value value) {
    mlir::Operation* definingOp = value.getDefiningOp();
    if (!definingOp) {
        return false;
    }

    if (mlir::isa<nl::Constant>(definingOp)) {
        return true;
    }

    const bool isArith = mlir::isa<nl::Add, nl::Sub, nl::Mul>(definingOp);
    if (!isArith) {
        return false;
    }

    for (const mlir::Value operand : definingOp->getOperands()) {
        if (!isConstantLike(operand)) {
            return false;
        }
    }

    return true;
}

}

NLTranslator::NLTranslator(NLProgram* program, LocalMemory* memory, const GraphView* view)
    : _program(program),
    _memory(memory),
    _view(view)
{
}

NLTranslator::~NLTranslator() {
}

void NLTranslator::translate(const mlir::func::FuncOp& function) {
    // Check MLIR verifier on the function
    if (mlir::failed(mlir::verify(function))) {
        throw IRException("nl function failed MLIR verification");
    }

    mlir::Region& bodyRegion = function->getRegion(0);
    if (!bodyRegion.hasOneBlock()) {
        throw IRException("NLTranslator expects a function with a single block");
    }

    translateBlock(bodyRegion.front(), _program->getStmts());
}

void NLTranslator::translateBlock(mlir::Block& block, NLStmtContainer* body) {
    for (mlir::Operation& operation : block) {
        if (nl::ScanNodes scanNodes = mlir::dyn_cast<nl::ScanNodes>(operation)) {
            _iteratorConfigs[scanNodes.getResult()] = IteratorConfig {IteratorKind::ScanNodes, {}, {}};
        } else if (nl::ScanNodesByLabel scanNodesByLabel = mlir::dyn_cast<nl::ScanNodesByLabel>(operation)) {
            IteratorConfig config {IteratorKind::ScanNodesByLabel, {}, {}};
            for (const mlir::Attribute label : scanNodesByLabel.getLabels()) {
                config._labels.emplace_back(mlir::cast<mlir::StringAttr>(label).getValue());
            }
            _iteratorConfigs[scanNodesByLabel.getResult()] = config;
        } else if (nl::ScanEdges scanEdges = mlir::dyn_cast<nl::ScanEdges>(operation)) {
            _iteratorConfigs[scanEdges.getResult()] = IteratorConfig {IteratorKind::ScanEdges, {}, {}};
        } else if (nl::GetOutEdges getOutEdges = mlir::dyn_cast<nl::GetOutEdges>(operation)) {
            IteratorConfig config {IteratorKind::GetOutEdges, getOutEdges.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getOutEdges.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            _iteratorConfigs[getOutEdges.getResult()] = config;
        } else if (nl::GetInEdges getInEdges = mlir::dyn_cast<nl::GetInEdges>(operation)) {
            IteratorConfig config {IteratorKind::GetInEdges, getInEdges.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getInEdges.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            _iteratorConfigs[getInEdges.getResult()] = config;
        } else if (nl::GetOutEdgesByType getOutEdgesByType = mlir::dyn_cast<nl::GetOutEdgesByType>(operation)) {
            IteratorConfig config {IteratorKind::GetOutEdgesByType, getOutEdgesByType.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getOutEdgesByType.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            config._edgeType = edgeTypeName(getOutEdgesByType.getEdgeType());
            _iteratorConfigs[getOutEdgesByType.getResult()] = config;
        } else if (nl::GetInEdgesByType getInEdgesByType = mlir::dyn_cast<nl::GetInEdgesByType>(operation)) {
            IteratorConfig config {IteratorKind::GetInEdgesByType, getInEdgesByType.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getInEdgesByType.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            config._edgeType = edgeTypeName(getInEdgesByType.getEdgeType());
            _iteratorConfigs[getInEdgesByType.getResult()] = config;
        } else if (nl::Sort sort = mlir::dyn_cast<nl::Sort>(operation)) {
            _iteratorConfigs[sort.getResult()] = IteratorConfig {IteratorKind::Sort, {}, {}, sortStateFor(sort.getState())};
        } else if (nl::For forLoop = mlir::dyn_cast<nl::For>(operation)) {
            translateFor(forLoop, body);
        } else if (mlir::isa<nl::GetPropertyType, nl::GetEdgeType>(operation)) {
            // The handle carries only a name; a fetch/hop resolves it on consumption
        } else if (nl::Constant constant = mlir::dyn_cast<nl::Constant>(operation)) {
            translateConstant(constant);
        } else if (nl::Add add = mlir::dyn_cast<nl::Add>(operation)) {
            translateAdd(add, body);
        } else if (nl::Sub sub = mlir::dyn_cast<nl::Sub>(operation)) {
            translateSub(sub, body);
        } else if (nl::Mul mul = mlir::dyn_cast<nl::Mul>(operation)) {
            translateMul(mul, body);
        } else if (nl::Eq eq = mlir::dyn_cast<nl::Eq>(operation)) {
            translateEq(eq, body);
        } else if (nl::And andOp = mlir::dyn_cast<nl::And>(operation)) {
            translateAnd(andOp, body);
        } else if (nl::Or orOp = mlir::dyn_cast<nl::Or>(operation)) {
            translateOr(orOp, body);
        } else if (nl::Not notOp = mlir::dyn_cast<nl::Not>(operation)) {
            translateNot(notOp, body);
        } else if (nl::Filter filter = mlir::dyn_cast<nl::Filter>(operation)) {
            translateFilter(filter, body);
        } else if (nl::GetNodeProperties getNodeProperties = mlir::dyn_cast<nl::GetNodeProperties>(operation)) {
            translatePropertyFetch(getNodeProperties.getInputNodes(),
                                   getNodeProperties.getPropertyType(),
                                   getNodeProperties.getValues(),
                                   /*isNode=*/true,
                                   body);
        } else if (nl::GetEdgeProperties getEdgeProperties = mlir::dyn_cast<nl::GetEdgeProperties>(operation)) {
            translatePropertyFetch(getEdgeProperties.getInputEdges(),
                                   getEdgeProperties.getPropertyType(),
                                   getEdgeProperties.getValues(),
                                   /*isNode=*/false,
                                   body);
        } else if (nl::CrossProduct crossProduct = mlir::dyn_cast<nl::CrossProduct>(operation)) {
            translateCrossProduct(crossProduct, body);
        } else if (nl::Limit limit = mlir::dyn_cast<nl::Limit>(operation)) {
            translateLimit(limit, body);
        } else if (nl::LimitUpdate update = mlir::dyn_cast<nl::LimitUpdate>(operation)) {
            translateLimitUpdate(update, body);
        } else if (nl::LimitTruncate truncate = mlir::dyn_cast<nl::LimitTruncate>(operation)) {
            translateLimitTruncate(truncate, body);
        } else if (nl::Skip skip = mlir::dyn_cast<nl::Skip>(operation)) {
            translateSkip(skip, body);
        } else if (nl::SkipUpdate update = mlir::dyn_cast<nl::SkipUpdate>(operation)) {
            translateSkipUpdate(update, body);
        } else if (nl::SkipTruncate truncate = mlir::dyn_cast<nl::SkipTruncate>(operation)) {
            translateSkipTruncate(truncate, body);
        } else if (nl::SortBuffer sortBuffer = mlir::dyn_cast<nl::SortBuffer>(operation)) {
            translateSortBuffer(sortBuffer, body);
        } else if (nl::SortCollect sortCollect = mlir::dyn_cast<nl::SortCollect>(operation)) {
            translateSortCollect(sortCollect, body);
        } else if (nl::Distinct distinct = mlir::dyn_cast<nl::Distinct>(operation)) {
            translateDistinctState(distinct, body);
        } else if (nl::DistinctFilter distinctFilter = mlir::dyn_cast<nl::DistinctFilter>(operation)) {
            translateDistinctFilter(distinctFilter, body);
        } else if (nl::Count count = mlir::dyn_cast<nl::Count>(operation)) {
            translateCountState(count, body);
        } else if (nl::CountUpdate countUpdate = mlir::dyn_cast<nl::CountUpdate>(operation)) {
            translateCountUpdate(countUpdate, body);
        } else if (nl::CountResult countResult = mlir::dyn_cast<nl::CountResult>(operation)) {
            translateCountResult(countResult, body);
        } else if (nl::Aggregate aggregate = mlir::dyn_cast<nl::Aggregate>(operation)) {
            translateAggregateState(aggregate, body);
        } else if (nl::AggregateUpdate aggregateUpdate = mlir::dyn_cast<nl::AggregateUpdate>(operation)) {
            translateAggregateUpdate(aggregateUpdate, body);
        } else if (nl::AggregateResult aggregateResult = mlir::dyn_cast<nl::AggregateResult>(operation)) {
            translateAggregateResult(aggregateResult, body);
        } else if (nl::Output output = mlir::dyn_cast<nl::Output>(operation)) {
            translateOutput(output, body);
        } else if (mlir::isa<nl::Yield, mlir::func::ReturnOp>(operation)) {
            // Structural terminators carry no behavior
        } else {
            throw IRException(fmt::format("NLTranslator cannot translate operation '{}'",
                                          operation.getName().getStringRef().str()));
        }
    }
}

void NLTranslator::translateFor(nl::For forLoop, NLStmtContainer* body) {
    // Fetch the iterator associated to this loop
    const auto configIt = _iteratorConfigs.find(forLoop->getOperand(0));
    if (configIt == _iteratorConfigs.end()) {
        throw IRException("nl.for iterator must be produced by an nl source operation");
    }

    const IteratorConfig& config = configIt->second;
    mlir::Block& loopBody = forLoop->getRegion(0).front();

    // A loop that names a limit handle stops once that counter is spent; the
    // handle was produced by an nl.limit translated earlier, so its counter is
    // already mapped.
    NLLimitState* limit = limitStateFor(forLoop.getLimit());

    // Translate the loop differently depending on the kind of iterator associated
    if (config._kind == IteratorKind::ScanNodes) {
        translateScanLoop(loopBody, limit, body);
    } else if (config._kind == IteratorKind::ScanNodesByLabel) {
        translateScanByLabelLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::ScanEdges) {
        translateScanEdgesLoop(loopBody, limit, body);
    } else if (config._kind == IteratorKind::Sort) {
        // A sort emit loop is never limit-bounded: ORDER BY must see every row.
        translateSortLoop(config, loopBody, body);
    } else {
        translateEdgeLoop(config, loopBody, limit, body);
    }
}

void NLTranslator::translateScanLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body) {
    // For::verify guarantees one block argument per iterator chunk, and a
    // node scan iterator has exactly one chunk of node IDs
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));

    NLScanLoopData* loopData = _program->allocFunctionData<NLScanLoopData>(nodeIDs);
    loopData->setLimit(limit);

    body->emplaceStmt(&NLExecutor::runScanNodesLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanByLabelLoop(const IteratorConfig& config,
                                            mlir::Block& loopBody,
                                            NLLimitState* limit,
                                            NLStmtContainer* body) {
    // A label scan binds the same single node ID chunk as a plain scan.
    const mlir::Value nodeChunk = loopBody.getArgument(0);
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(nodeChunk));

    // Resolve the label names into the LabelSet the scan filters by. A node
    // matches when its label set is a superset of this one, so the requested
    // labels are ANDed. If any name is absent from the schema, no node can
    // carry it: the conjunction is unsatisfiable and the scan is marked
    // unmatchable (it emits nothing) rather than dropping the name and matching
    // a weaker set.
    LabelSet labelset;
    bool matchable = true;
    const LabelMap& labels = _view->metadata().labels();
    for (const llvm::StringRef label : config._labels) {
        const std::optional<LabelID> id = labels.get(label);
        if (!id) {
            matchable = false;
            break;
        }

        labelset.set(*id);
    }

    NLScanByLabelLoopData* loopData = _program->allocFunctionData<NLScanByLabelLoopData>(nodeIDs, labelset, matchable);
    loopData->setLimit(limit);

    body->emplaceStmt(&NLExecutor::runScanNodesByLabelLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanEdgesLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body) {
    // For::verify guarantees one block argument per iterator chunk, and an edge
    // scan iterator has exactly four chunks in the order getEdgeIteratorType
    // establishes: sources, edge IDs, edge type IDs, targets.
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumn(loopBody.getArgument(1)));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumn(loopBody.getArgument(2)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(3)));

    NLScanEdgesLoopData* loopData = _program->allocFunctionData<NLScanEdgesLoopData>(sources,
                                                                                     edgeIDs,
                                                                                     edgeTypes,
                                                                                     targets);
    loopData->setLimit(limit);

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runScanEdgesLoop, loopData});

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateEdgeLoop(const IteratorConfig& config,
                                     mlir::Block& loopBody,
                                     NLLimitState* limit,
                                     NLStmtContainer* body) {
    // The four fixed chunks of an edge iterator step, in the block-argument
    // order established by getEdgeIteratorType: sources, edge IDs, edge type
    // IDs, targets
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumn(loopBody.getArgument(1)));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumn(loopBody.getArgument(2)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(3)));

    const ColumnNodeIDs* inputNodeIDs = static_cast<const ColumnNodeIDs*>(getColumn(config._inputNodes));

    const bool byType = config._kind == IteratorKind::GetOutEdgesByType
                        || config._kind == IteratorKind::GetInEdgesByType;

    // A by-type hop carries the resolved edge type in an NLEdgeByTypeLoopData; a
    // plain hop uses the base NLEdgeLoopData. The shared driver below (reserve,
    // carried columns, body) works through the base pointer either way.
    NLEdgeLoopData* loopData = nullptr;
    if (byType) {
        // Resolve the edge type name against the schema, exactly as
        // translateScanByLabelLoop resolves its labels. A name absent from the
        // schema matches no edge, so the loop is marked unmatchable and emits
        // nothing rather than filtering against a bogus type.
        const std::optional<EdgeTypeID> edgeTypeID = _view->metadata().edgeTypes().get(config._edgeType);
        const bool matchable = edgeTypeID.has_value();
        const EdgeTypeID resolvedType = matchable ? *edgeTypeID : EdgeTypeID();

        loopData = _program->allocFunctionData<NLEdgeByTypeLoopData>(inputNodeIDs,
                                                                     sources,
                                                                     edgeIDs,
                                                                     edgeTypes,
                                                                     targets,
                                                                     resolvedType,
                                                                     matchable);
    } else {
        loopData = _program->allocFunctionData<NLEdgeLoopData>(inputNodeIDs,
                                                               sources,
                                                               edgeIDs,
                                                               edgeTypes,
                                                               targets);
    }
    loopData->setLimit(limit);

    // Reserve scratch indices column
    loopData->getIndices()->reserve(_program->getChunkSize());

    // Allocate carried columns in the carried set
    const auto inputArgument = mlir::dyn_cast<mlir::BlockArgument>(config._inputNodes);
    const size_t carriedCount = config._carriedColumns.size();
    for (size_t carriedIndex = 0; carriedIndex < carriedCount; carriedIndex++) {
        const mlir::Value carriedValue = config._carriedColumns[carriedIndex];

        // A carried chunk is filtered through the same indices as the input,
        // so its rows must belong to the same loop step: it must be a loop
        // variable of the nl.for that binds input_nodes. The ops constrain
        // only types, so a cross-loop carry passes MLIR verification and has
        // to be rejected here, before it can misalign the gathers at runtime.
        const auto carriedArgument = mlir::dyn_cast<mlir::BlockArgument>(carriedValue);
        const bool boundBySameLoop = inputArgument && carriedArgument
                                     && carriedArgument.getOwner() == inputArgument.getOwner();
        if (!boundBySameLoop) {
            throw IRException("Carried columns must be loop variables of the same nl.for as the input chunk");
        }

        const NLChunkKind kind = getChunkKind(carriedValue.getType());
        Column* carriedOutput = allocColumn(loopBody.getArgument(static_cast<unsigned>(4 + carriedIndex)));

        const NLCarriedColumn carriedColumn(getColumn(carriedValue),
                                            carriedOutput,
                                            NLExecutor::selectGatherFunction(kind));
        loopData->addCarriedColumn(carriedColumn);
    }

    NLHandlerFunction handler = nullptr;
    if (config._kind == IteratorKind::GetOutEdges) {
        handler = &NLExecutor::runGetOutEdgesLoop;
    } else if (config._kind == IteratorKind::GetInEdges) {
        handler = &NLExecutor::runGetInEdgesLoop;
    } else if (config._kind == IteratorKind::GetOutEdgesByType) {
        handler = &NLExecutor::runGetOutEdgesByTypeLoop;
    } else {
        handler = &NLExecutor::runGetInEdgesByTypeLoop;
    }
    body->emplaceStmt(handler, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translatePropertyFetch(mlir::Value inputValue,
                                          mlir::Value propertyTypeValue,
                                          mlir::Value resultValue,
                                          bool isNode,
                                          NLStmtContainer* body) {
    // The property name lives on the nl.get_property_type that produced the handle
    nl::GetPropertyType handleOp = propertyTypeValue.getDefiningOp<nl::GetPropertyType>();
    if (!handleOp) {
        throw IRException("property_type operand must come from nl.get_property_type");
    }

    const llvm::StringRef name = handleOp.getName();

    // Resolve the name against the schema once, here, so execution works from a
    // PropertyTypeID and value type and never sees the name again
    const std::optional<PropertyType> propertyType = _view->metadata().propTypes().get(std::string_view(name.data(), name.size()));
    if (!propertyType) {
        throw IRException("Unknown property '" + name.str() + "'");
    }

    const ValueType valueType = propertyType->_valueType;

    const Column* input = getColumn(inputValue);
    Column* output = allocOptColumnForValueType(valueType);
    _valueSlots[resultValue] = output;

    NLPropertyFetchData* fetchData = _program->allocFunctionData<NLPropertyFetchData>(input,
                                                                                      output,
                                                                                      propertyType->_id);

    const NLHandlerFunction handler = selectPropertyFetchHandler(isNode, valueType);
    body->emplaceStmt(handler, fetchData);
}

void NLTranslator::translateConstant(nl::Constant constant) {
    const mlir::TypedAttr value = mlir::cast<mlir::TypedAttr>(constant.getValue());
    const mlir::TypedValue<mlir::nl::ChunkType> res = constant.getResult();
    const auto chunkType = mlir::cast<nl::ChunkType>(res.getType());
    const ValueType valueType = valueTypeFromElementType(chunkType.getElementType());

    Column* column = nullptr;
    const auto materialize = [&]<SupportedType T>() {
        auto* typed = _memory->alloc<ColumnConst<typename T::Primitive>>();
        typed->set(constantValueAs<T>(value));
        column = typed;
    };
    ValueTypeDispatcher(valueType).execute(materialize);
    bioassert(column, "Failed to allocate column.");

    _valueSlots[res] = column;
}

void NLTranslator::translateAdd(nl::Add add, NLStmtContainer* body) {
    const Column* lhs = getColumn(add.getLhs());
    const Column* rhs = getColumn(add.getRhs());

    Column* result = nullptr;
    const NLBinaryFn fn = NLExecutor::selectBinary<OP_ADD>(lhs, rhs, _memory, result);
    bioassert(result, "Failed to translate ADD result.");

    _valueSlots[add.getResult()] = result;

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn);
    body->emplaceStmt(&NLExecutor::runBinary, data);
}

void NLTranslator::translateSub(nl::Sub sub, NLStmtContainer* body) {
    const Column* lhs = getColumn(sub.getLhs());
    const Column* rhs = getColumn(sub.getRhs());

    Column* result = nullptr;
    const NLBinaryFn fn = NLExecutor::selectBinary<OP_SUB>(lhs, rhs, _memory, result);
    bioassert(result, "Failed to translate SUB result.");

    _valueSlots[sub.getResult()] = result;

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn);
    body->emplaceStmt(&NLExecutor::runBinary, data);
}

void NLTranslator::translateMul(nl::Mul mul, NLStmtContainer* body) {
    const Column* lhs = getColumn(mul.getLhs());
    const Column* rhs = getColumn(mul.getRhs());

    Column* result = nullptr;
    const NLBinaryFn fn = NLExecutor::selectBinary<OP_MUL>(lhs, rhs, _memory, result);
    bioassert(result, "Failed to translate MUL result.");

    _valueSlots[mul.getResult()] = result;

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn);
    body->emplaceStmt(&NLExecutor::runBinary, data);
}

void NLTranslator::translateEq(nl::Eq eq, NLStmtContainer* body) {
    const Column* lhs = getColumn(eq.getLhs());
    const Column* rhs = getColumn(eq.getRhs());

    Column* result = nullptr;
    const NLBinaryFn fn = NLExecutor::selectBinary<OP_EQUAL>(lhs, rhs, _memory, result);
    bioassert(result, "Failed to translate EQ result.");

    _valueSlots[eq.getResult()] = result;

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn);
    body->emplaceStmt(&NLExecutor::runBinary, data);
}

void NLTranslator::translateAnd(nl::And andOp, NLStmtContainer* body) {
    const Column* lhs = getColumn(andOp.getLhs());
    const Column* rhs = getColumn(andOp.getRhs());

    Column* result = nullptr;
    const NLBinaryFn fn = NLExecutor::selectBinary<OP_AND>(lhs, rhs, _memory, result);
    bioassert(result, "Failed to translate AND result.");

    _valueSlots[andOp.getResult()] = result;

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn);
    body->emplaceStmt(&NLExecutor::runBinary, data);
}

void NLTranslator::translateOr(nl::Or orOp, NLStmtContainer* body) {
    const Column* lhs = getColumn(orOp.getLhs());
    const Column* rhs = getColumn(orOp.getRhs());

    Column* result = nullptr;
    const NLBinaryFn fn = NLExecutor::selectBinary<OP_OR>(lhs, rhs, _memory, result);
    bioassert(result, "Failed to translate OR result.");

    _valueSlots[orOp.getResult()] = result;

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn);
    body->emplaceStmt(&NLExecutor::runBinary, data);
}

void NLTranslator::translateNot(nl::Not notOp, NLStmtContainer* body) {
    const Column* operand = getColumn(notOp.getOperand());

    Column* result = nullptr;
    const NLUnaryFn fn = NLExecutor::selectNot(operand, _memory, result);
    bioassert(result, "Failed to allocate NOT result column.");

    _valueSlots[notOp.getResult()] = result;

    NLUnaryData* data = _program->allocFunctionData<NLUnaryData>(operand, result, fn);
    body->emplaceStmt(&NLExecutor::runUnary, data);
}

void NLTranslator::translateFilter(nl::Filter filter, NLStmtContainer* body) {
    const Column* mask = getColumn(filter.getMask());

    const auto maskChunk = mlir::cast<nl::ChunkType>(filter.getMask().getType());
    const bool maskNullable = mlir::isa<storage::NullableType>(maskChunk.getElementType());
    const NLMaskSurvivorFunction filterFunction =
        NLExecutor::selectMaskSurvivorFunction(maskNullable);


    NLFilterData* data = _program->allocFunctionData<NLFilterData>(mask, filterFunction);

    // Reserve to avoid execution time allocations
    data->getIndices()->reserve(_program->getChunkSize());

    // New column for each result output, same types
    const mlir::OperandRange columns = filter.getColumns();
    const mlir::ResultRange results = filter.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Value colVal = columns[columnIndex];
        const mlir::Type colType = colVal.getType();

        const Column* input = getColumn(colVal);
        Column* output = allocColumnForChunkType(colType);
        _valueSlots[results[columnIndex]] = output;

        const NLGatherFunction gatherType = selectGatherForChunkType(colType);

        const NLFilterData::FilterColumn column {input, output, gatherType};

        data->addColumn(column);
    }

    body->emplaceStmt(&NLExecutor::runFilter, data);
}

void NLTranslator::translateOutput(nl::Output output, NLStmtContainer* body) {
    // The optional limit handle is a separate operand, so read just the columns;
    // including it in this list would treat the handle as a chunk.
    const mlir::OperandRange columns = output.getColumns();
    if (columns.empty()) {
        throw IRException("nl.output requires at least one column");
    }

    // Output is either the per-step sink inside an nl.for body, or - for an
    // aggregate like COUNT that collapses to one row - a one-shot emit at function
    // scope. Either way its per-row columns must be bound in the output's own block,
    // which the per-column check below enforces: a loop variable of this loop, or a
    // chunk materialized in this block (a property fetch, or nl.count_result at
    // function scope). The sole cross-scope exception is a constant, which is
    // loop-invariant and broadcasts; any other chunk from an outer or sibling loop
    // fails the check.
    mlir::Block* outputBlock = output->getBlock();

    NLOutputData* outputData = _program->allocFunctionData<NLOutputData>();
    outputData->setLimit(limitStateFor(output.getLimit()));
    outputData->setSkip(skipStateFor(output.getSkip()));

    if (const mlir::Value cardinality = output.getCardinality()) {
        outputData->setCardinality(getColumn(cardinality));
    }

    for (const mlir::Value column : columns) {
        const auto columnArgument = mlir::dyn_cast<mlir::BlockArgument>(column);
        const bool isInnermostLoopVariable = columnArgument && columnArgument.getOwner() == outputBlock;

        // A property fetch result is not a loop variable but an op result
        // produced in this same loop body; it is equally available to output.
        mlir::Operation* definingOp = column.getDefiningOp();
        const bool isProducedInThisBlock = definingOp && definingOp->getBlock() == outputBlock;

        if (!isInnermostLoopVariable && !isProducedInThisBlock && !isConstantLike(column)) {
            throw IRException("nl.output columns must be a loop variable of the enclosing "
                              "nl.for, produced in this block, or a constant");
        }

        outputData->addOutputColumn(getColumn(column));
    }

    body->emplaceStmt(&NLExecutor::runOutput, outputData);
}

void NLTranslator::translateLimit(nl::Limit limit, NLStmtContainer* body) {
    // Allocate the runtime counter and map the handle to it, so the loops, the
    // update and the output that name the handle all find the same counter.
    NLLimitState* state = _program->allocLimitState();
    _limitStates[limit.getState()] = state;

    // The reset runs each time the block holding this nl.limit runs: once at
    // function scope for a top-level LIMIT, per enclosing step for a nested one.
    const size_t count = limit.getCount();
    NLLimitInitData* initData = _program->allocFunctionData<NLLimitInitData>(state, count);

    body->emplaceStmt(&NLExecutor::runLimitInit, initData);
}

void NLTranslator::translateLimitUpdate(nl::LimitUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so limitStateFor returns its counter or
    // throws if it was not produced by an nl.limit.
    NLLimitState* state = limitStateFor(update.getState());

    const Column* representative = getColumn(update.getRows());
    NLLimitUpdateData* updateData = _program->allocFunctionData<NLLimitUpdateData>(state, representative);

    body->emplaceStmt(&NLExecutor::runLimitUpdate, updateData);
}

void NLTranslator::translateLimitTruncate(nl::LimitTruncate truncate, NLStmtContainer* body) {
    // The handle is a required operand, so limitStateFor returns its counter or
    // throws if it was not produced by an nl.limit.
    NLLimitState* state = limitStateFor(truncate.getState());

    NLLimitTruncateData* data = _program->allocFunctionData<NLLimitTruncateData>(state);

    // One fresh output column per input, walked in step with the results the
    // downstream consumers are mapped to.
    const mlir::OperandRange columns = truncate.getColumns();
    const mlir::ResultRange results = truncate.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        addTruncateColumn(columns[columnIndex], results[columnIndex], data);
    }

    body->emplaceStmt(&NLExecutor::runLimitTruncate, data);
}

void NLTranslator::addTruncateColumn(mlir::Value inputValue,
                                     mlir::Value resultValue,
                                     NLLimitTruncateData* data) {
    const Column* input = getColumn(inputValue);

    const auto chunkType = mlir::cast<nl::ChunkType>(inputValue.getType());
    const mlir::Type elementType = chunkType.getElementType();

    // The output keeps the input's element type; only the row count is cut. The
    // copy is a block-repeat with factor 1 (each input row once, stopping at
    // emitThisStep), so it reuses the block-repeat broadcast families the cross
    // product uses - by value type for a nullable value chunk, by chunk kind for
    // an ID chunk.
    Column* output = nullptr;
    NLBroadcastFunction copyPrefix = nullptr;

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        output = allocOptColumnForValueType(valueType);
        copyPrefix = NLExecutor::selectOptBlockRepeatFunction(valueType);
    } else {
        const NLChunkKind kind = getChunkKind(chunkType);
        output = allocColumnForKind(kind);
        copyPrefix = NLExecutor::selectBlockRepeatFunction(kind);
    }

    _valueSlots[resultValue] = output;

    const NLCrossColumn truncateColumn(input, output, copyPrefix);
    data->addColumn(truncateColumn);
}

NLLimitState* NLTranslator::limitStateFor(mlir::Value handle) const {
    if (!handle) {
        return nullptr;
    }

    const auto stateIt = _limitStates.find(handle);
    if (stateIt == _limitStates.end()) {
        throw IRException("limit handle must be produced by an nl.limit");
    }

    return stateIt->second;
}

void NLTranslator::translateSkip(nl::Skip skip, NLStmtContainer* body) {
    // Allocate the runtime counter and map the handle to it, so the update and the
    // truncate that name the handle both find the same counter.
    NLSkipState* state = _program->allocSkipState();
    _skipStates[skip.getState()] = state;

    // The reset runs each time the block holding this nl.skip runs: once at
    // function scope for a top-level SKIP.
    const size_t count = skip.getCount();
    NLSkipInitData* initData = _program->allocFunctionData<NLSkipInitData>(state, count);

    body->emplaceStmt(&NLExecutor::runSkipInit, initData);
}

void NLTranslator::translateSkipUpdate(nl::SkipUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so skipStateFor returns its counter or
    // throws if it was not produced by an nl.skip.
    NLSkipState* state = skipStateFor(update.getState());

    const Column* representative = getColumn(update.getRows());
    NLSkipUpdateData* updateData = _program->allocFunctionData<NLSkipUpdateData>(state, representative);

    body->emplaceStmt(&NLExecutor::runSkipUpdate, updateData);
}

void NLTranslator::translateSkipTruncate(nl::SkipTruncate truncate, NLStmtContainer* body) {
    // The handle is a required operand, so skipStateFor returns its counter or
    // throws if it was not produced by an nl.skip.
    NLSkipState* state = skipStateFor(truncate.getState());

    NLSkipTruncateData* data = _program->allocFunctionData<NLSkipTruncateData>(state);

    // One fresh output column per input, walked in step with the results the
    // downstream consumers are mapped to.
    const mlir::OperandRange columns = truncate.getColumns();
    const mlir::ResultRange results = truncate.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        addSkipColumn(columns[columnIndex], results[columnIndex], data);
    }

    body->emplaceStmt(&NLExecutor::runSkipTruncate, data);
}

void NLTranslator::addSkipColumn(mlir::Value inputValue,
                                 mlir::Value resultValue,
                                 NLSkipTruncateData* data) {
    const Column* input = getColumn(inputValue);

    const auto chunkType = mlir::cast<nl::ChunkType>(inputValue.getType());
    const mlir::Type elementType = chunkType.getElementType();

    // The output keeps the input's element type; only the dropped prefix is
    // removed. The copy is a range copy of the surviving suffix to the front of a
    // fresh chunk, so it uses the copy families - by value type for a nullable
    // value chunk, by chunk kind for an ID chunk.
    Column* output = nullptr;
    NLCopyFunction copySuffix = nullptr;

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        output = allocOptColumnForValueType(valueType);
        copySuffix = NLExecutor::selectOptCopyFunction(valueType);
    } else {
        const NLChunkKind kind = getChunkKind(chunkType);
        output = allocColumnForKind(kind);
        copySuffix = NLExecutor::selectCopyFunction(kind);
    }

    _valueSlots[resultValue] = output;

    const NLSkipColumn skipColumn(input, output, copySuffix);
    data->addColumn(skipColumn);
}

NLSkipState* NLTranslator::skipStateFor(mlir::Value handle) const {
    if (!handle) {
        return nullptr;
    }

    const auto stateIt = _skipStates.find(handle);
    if (stateIt == _skipStates.end()) {
        throw IRException("skip handle must be produced by an nl.skip");
    }

    return stateIt->second;
}

void NLTranslator::translateSortBuffer(nl::SortBuffer buffer, NLStmtContainer* body) {
    // Allocate the runtime accumulator and map the handle to it, so the collect
    // and the emit loop that name the handle share the same buffers. The buffer
    // columns themselves are allocated by the collect, which knows their types.
    NLSortState* state = _program->allocSortState();
    _sortStates[buffer.getState()] = state;

    // A fused ORDER BY ... LIMIT carries the bound as the top-K count, so the
    // accumulator keeps only the best k rows. Absent, it keeps every row.
    if (const std::optional<uint64_t> topK = buffer.getTopK()) {
        state->setTopK(*topK);
    }

    // The reset empties the buffers each time the block holding this nl.sort_buffer
    // runs: once at function scope for a top-level ORDER BY.
    NLSortResetData* resetData = _program->allocFunctionData<NLSortResetData>(state);
    body->emplaceStmt(&NLExecutor::runSortReset, resetData);
}

void NLTranslator::translateSortCollect(nl::SortCollect collect, NLStmtContainer* body) {
    const mlir::Value collectState = collect.getState();
    NLSortState* state = sortStateFor(collectState);

    // The sort spec lives on the nl.sort_buffer that produced the handle.
    nl::SortBuffer buffer = collectState.getDefiningOp<nl::SortBuffer>();
    if (!buffer) {
        throw IRException("sort_collect state must come from nl.sort_buffer");
    }

    // The buffers are allocated once, by the single collect feeding a buffer.
    // Generated IR has exactly one collect per accumulator; a second would append
    // to the same buffers and double the rows, so it is rejected here.
    if (!state->buffers().empty()) {
        throw IRException("an nl.sort_buffer must be fed by a single nl.sort_collect");
    }

    NLSortCollectData* data = _program->allocFunctionData<NLSortCollectData>(state);

    // One growing buffer per collected column, row-aligned. The buffer keeps the
    // column's element type; the append copies a chunk's rows onto its tail. A
    // bounded accumulator also gets a compaction scratch column and the gather
    // that fills it, so the trim can drop all but the best k rows.
    const bool bounded = state->isBounded();
    const mlir::OperandRange columns = collect.getColumns();
    for (const mlir::Value column : columns) {
        Column* bufferColumn = allocColumnForChunkType(column.getType());

        Column* tempColumn = bounded ? allocColumnForChunkType(column.getType()) : nullptr;
        const NLGatherFunction gather = bounded ? selectGatherForChunkType(column.getType()) : nullptr;
        state->addColumnBuffer(bufferColumn, tempColumn, gather);

        const NLSortCollectData::Append append {getColumn(column),
                                                bufferColumn,
                                                selectAppendForChunkType(column.getType())};
        data->addAppend(append);
    }

    // Build the comparators from the spec, most significant key first. Each key
    // indexes a collected column, so the index must be in range, and the column's
    // element type must have an order (an embedding key is rejected by the
    // selector). The buffer column is the one the comparator reads at run time.
    const llvm::ArrayRef<int64_t> keyColumns = buffer.getKeyColumns();
    const llvm::ArrayRef<bool> keyAscending = buffer.getKeyAscending();
    for (size_t keyIndex = 0; keyIndex < keyColumns.size(); keyIndex++) {
        const int64_t keyColumn = keyColumns[keyIndex];
        const bool inRange = keyColumn >= 0 && static_cast<size_t>(keyColumn) < columns.size();
        if (!inRange) {
            throw IRException("sort key column index is out of range");
        }

        const mlir::Type keyType = columns[static_cast<size_t>(keyColumn)].getType();
        const NLSortState::Key key {state->buffer(static_cast<size_t>(keyColumn)),
                                    keyAscending[keyIndex],
                                    selectCompareForChunkType(keyType)};
        state->addKey(key);
    }

    body->emplaceStmt(&NLExecutor::runSortCollect, data);
}

void NLTranslator::translateSortLoop(const IteratorConfig& config,
                                     mlir::Block& loopBody,
                                     NLStmtContainer* body) {
    NLSortState* state = config._sortState;
    if (!state) {
        throw IRException("nl.sort iterator must carry a sort accumulator");
    }

    // For::verify binds one loop variable per iterator chunk, and the sort
    // iterator's chunk types are the collected column types, so the loop must
    // take exactly one variable per buffer.
    const size_t bufferCount = state->buffers().size();
    if (loopBody.getNumArguments() != bufferCount) {
        throw IRException("nl.sort loop must bind one variable per collected column");
    }

    NLSortLoopData* loopData = _program->allocFunctionData<NLSortLoopData>(state);

    // Reserve the permutation-slice scratch so the per-step gather stays
    // allocation-free, the same as the edge loop's indices column.
    loopData->getIndices()->reserve(_program->getChunkSize());

    // Each loop variable is filled from its buffer by gathering the rows the
    // current emit step covers, in permutation order. The buffer is the read
    // side, the loop variable the write side - the NLCarriedColumn (input,
    // output, gather) shape the edge loops already use.
    for (size_t columnIndex = 0; columnIndex < bufferCount; columnIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(columnIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;

        const NLCarriedColumn column(state->buffer(columnIndex),
                                     output,
                                     selectGatherForChunkType(loopVariable.getType()));
        loopData->addColumn(column);
    }

    body->emplaceStmt(&NLExecutor::runSortLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

NLSortState* NLTranslator::sortStateFor(mlir::Value handle) const {
    const auto stateIt = _sortStates.find(handle);
    if (stateIt == _sortStates.end()) {
        throw IRException("sort handle must be produced by an nl.sort_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateDistinctState(nl::Distinct distinct, NLStmtContainer* body) {
    // Allocate the runtime seen-set and map the handle to it, so the filter that
    // names the handle finds the same set.
    NLDistinctState* state = _program->allocDistinctState();
    _distinctStates[distinct.getState()] = state;

    // The reset empties the set each time the block holding this nl.distinct runs:
    // once at function scope for a top-level / mid-query DISTINCT.
    NLDistinctResetData* resetData = _program->allocFunctionData<NLDistinctResetData>(state);
    body->emplaceStmt(&NLExecutor::runDistinctReset, resetData);
}

void NLTranslator::translateDistinctFilter(nl::DistinctFilter filter, NLStmtContainer* body) {
    // The handle is a required operand, so distinctStateFor returns its set or
    // throws if it was not produced by an nl.distinct.
    NLDistinctState* state = distinctStateFor(filter.getState());

    NLDistinctFilterData* data = _program->allocFunctionData<NLDistinctFilterData>(state);

    // Reserve the surviving-indices scratch so the per-step gather stays
    // allocation-free, the same as the edge and sort loops' indices column.
    data->getIndices()->reserve(_program->getChunkSize());

    // One fresh output column per input, walked in step with the results the
    // downstream consumers are mapped to.
    const mlir::OperandRange columns = filter.getColumns();
    const mlir::ResultRange results = filter.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        addDistinctColumn(columns[columnIndex], results[columnIndex], data);
    }

    body->emplaceStmt(&NLExecutor::runDistinctFilter, data);
}

void NLTranslator::addDistinctColumn(mlir::Value inputValue,
                                     mlir::Value resultValue,
                                     NLDistinctFilterData* data) {
    const Column* input = getColumn(inputValue);

    // The output keeps the input's element type; only duplicate rows are removed.
    // The key-append serializes one of its rows into the shared row key, and the
    // gather copies the survivors into the fresh output - the same gather family
    // the edge and sort loops use, selected by chunk type.
    Column* output = allocColumnForChunkType(inputValue.getType());
    _valueSlots[resultValue] = output;

    const NLDistinctFilterData::FilterColumn column {input,
                                                     output,
                                                     selectKeyAppendForChunkType(inputValue.getType()),
                                                     selectGatherForChunkType(inputValue.getType())};
    data->addColumn(column);
}

NLDistinctState* NLTranslator::distinctStateFor(mlir::Value handle) const {
    const auto stateIt = _distinctStates.find(handle);
    if (stateIt == _distinctStates.end()) {
        throw IRException("distinct handle must be produced by an nl.distinct");
    }

    return stateIt->second;
}

void NLTranslator::translateCountState(nl::Count count, NLStmtContainer* body) {
    // Allocate the runtime tally and map the handle to it, so the update and the
    // emit loop that name the handle share the same counter.
    NLCountState* state = _program->allocCountState();
    _countStates[count.getState()] = state;

    // The reset zeroes the tally each time the block holding this nl.count runs:
    // once at function scope for a top-level / mid-query COUNT.
    NLCountResetData* resetData = _program->allocFunctionData<NLCountResetData>(state);
    body->emplaceStmt(&NLExecutor::runCountReset, resetData);
}

void NLTranslator::translateCountUpdate(nl::CountUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so countStateFor returns its tally or
    // throws if it was not produced by an nl.count.
    NLCountState* state = countStateFor(update.getState());

    // Measure the chunk's non-null rows the way its type demands: all rows for an
    // ID chunk, the present values for a nullable value chunk.
    const mlir::Value rows = update.getRows();
    const Column* input = getColumn(rows);
    const NLCountFunction count = selectCountForChunkType(rows.getType());

    NLCountUpdateData* data = _program->allocFunctionData<NLCountUpdateData>(state, input, count);
    body->emplaceStmt(&NLExecutor::runCountUpdate, data);
}

void NLTranslator::translateCountResult(nl::CountResult result, NLStmtContainer* body) {
    // The handle is a required operand, so countStateFor returns its tally or
    // throws if it was not produced by an nl.count.
    NLCountState* state = countStateFor(result.getState());

    // The result is the unsigned i64 count chunk (!nl.chunk<ui64>) runCountResult
    // fills with the single tally row. It is the one non-nullable value chunk in the
    // pipeline, so allocate its ColumnVector<uint64_t> directly - the shared
    // allocColumnForChunkType only knows ID chunks and nullable value chunks.
    ColumnVector<uint64_t>* output = _memory->alloc<ColumnVector<uint64_t>>();
    output->reserve(_program->getChunkSize());
    _valueSlots[result.getResult()] = output;

    NLCountResultData* data = _program->allocFunctionData<NLCountResultData>(state, output);
    body->emplaceStmt(&NLExecutor::runCountResult, data);
}

NLCountState* NLTranslator::countStateFor(mlir::Value handle) const {
    const auto stateIt = _countStates.find(handle);
    if (stateIt == _countStates.end()) {
        throw IRException("count handle must be produced by an nl.count");
    }

    return stateIt->second;
}

void NLTranslator::translateAggregateState(nl::Aggregate aggregate, NLStmtContainer* body) {
    // Allocate the runtime accumulator and map the handle to it, so the update and
    // the emit that name the handle share the same accumulator.
    NLAggregateState* state = _program->allocAggregateState();
    _aggregateStates[aggregate.getState()] = state;

    // The state handle's element type is the accumulator's value type (f64 for an
    // avg, the input type otherwise), baked during lowering. Allocate the single-row
    // nullable value column the reduction folds into.
    const auto stateType = mlir::cast<nl::AggregateStateType>(aggregate.getState().getType());
    const ValueType accumulatorType = valueTypeFromElementType(stateType.getElementType());
    Column* accumulator = allocSingleRowOptColumnForValueType(accumulatorType);
    state->setAccumulator(accumulator);

    // The reset re-initializes the accumulator each time the block holding this
    // nl.aggregate runs (once at function scope for a top-level aggregate): a present
    // zero for sum/avg, null for min/max.
    const AggregateKind kind = toRuntimeAggregateKind(aggregate.getKind());
    const NLAggregateResetFunction reset = NLExecutor::selectAggregateReset(kind, accumulatorType);

    NLAggregateResetData* resetData = _program->allocFunctionData<NLAggregateResetData>(state, reset);
    body->emplaceStmt(&NLExecutor::runAggregateReset, resetData);
}

void NLTranslator::translateAggregateUpdate(nl::AggregateUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so aggregateStateFor returns its accumulator
    // or throws if it was not produced by an nl.aggregate.
    NLAggregateState* state = aggregateStateFor(update.getState());

    // Fold the chunk's non-null values the way its kind and value type demand. The
    // input value type may differ from the accumulator's (an avg of i64 folds into
    // an f64 accumulator), so the handler is selected from the input type here.
    const mlir::Value rows = update.getRows();
    const Column* input = getColumn(rows);
    const AggregateKind kind = toRuntimeAggregateKind(update.getKind());
    const ValueType inputType = nullableChunkValueType(rows.getType());
    const NLAggregateUpdateFunction fold = NLExecutor::selectAggregateUpdate(kind, inputType);

    NLAggregateUpdateData* data = _program->allocFunctionData<NLAggregateUpdateData>(state, input, fold);
    body->emplaceStmt(&NLExecutor::runAggregateUpdate, data);
}

void NLTranslator::translateAggregateResult(nl::AggregateResult result, NLStmtContainer* body) {
    // The handle is a required operand, so aggregateStateFor returns its accumulator
    // or throws if it was not produced by an nl.aggregate.
    NLAggregateState* state = aggregateStateFor(result.getState());

    // The result is a single-row nullable value chunk runAggregateResult fills with
    // the reduced value. Allocate its ColumnOptVector on the result value type and
    // map the op result to it.
    const mlir::Value resultChunk = result.getResult();
    const ValueType resultType = nullableChunkValueType(resultChunk.getType());
    Column* output = allocOptColumnForValueType(resultType);
    _valueSlots[resultChunk] = output;

    const AggregateKind kind = toRuntimeAggregateKind(result.getKind());
    const NLAggregateResultFunction emit = NLExecutor::selectAggregateResult(kind, resultType);

    NLAggregateResultData* data = _program->allocFunctionData<NLAggregateResultData>(state, output, emit);
    body->emplaceStmt(&NLExecutor::runAggregateResult, data);
}

NLAggregateState* NLTranslator::aggregateStateFor(mlir::Value handle) const {
    const auto stateIt = _aggregateStates.find(handle);
    if (stateIt == _aggregateStates.end()) {
        throw IRException("aggregate handle must be produced by an nl.aggregate");
    }

    return stateIt->second;
}

// An ID chunk allocates an ID column on its kind; a !storage.nullable<...> chunk
// allocates a ColumnOptVector on its value type. Mirrors addCrossColumn's split.
Column* NLTranslator::allocColumnForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return allocOptColumnForValueType(valueType);
    }

    return allocColumnForKind(chunkKindFromElementType(elementType));
}

NLAppendFunction NLTranslator::selectAppendForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptAppendFunction(valueType);
    }

    return NLExecutor::selectAppendFunction(chunkKindFromElementType(elementType));
}

NLGatherFunction NLTranslator::selectGatherForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptGatherFunction(valueType);
    }

    return NLExecutor::selectGatherFunction(chunkKindFromElementType(elementType));
}

NLCompareFunction NLTranslator::selectCompareForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptCompareFunction(valueType);
    }

    return NLExecutor::selectCompareFunction(chunkKindFromElementType(elementType));
}

NLKeyAppendFunction NLTranslator::selectKeyAppendForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptKeyAppendFunction(valueType);
    }

    return NLExecutor::selectKeyAppendFunction(chunkKindFromElementType(elementType));
}

NLCountFunction NLTranslator::selectCountForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptCountFunction(valueType);
    }

    // A non-nullable chunk must be an ID chunk (node/edge/edge-type IDs), which has
    // no null rows, so every row counts. chunkKindFromElementType rejects any other
    // element type, so its result is discarded - it validates, nothing more.
    chunkKindFromElementType(elementType);
    return &NLExecutor::countAllRows;
}

void NLTranslator::translateCrossProduct(nl::CrossProduct cross, NLStmtContainer* body) {
    const mlir::OperandRange outerColumns = cross.getOuterColumns();
    const mlir::OperandRange innerColumns = cross.getInnerColumns();

    // At run time runCrossProduct takes N from the first outer column and M from
    // the first inner column, so a side with no column cannot be sized. Reject
    // that here: DBLowering never emits it, but the nl IR may come from elsewhere.
    if (outerColumns.empty() || innerColumns.empty()) {
        throw IRException("nl.cross_product needs at least one column on each side");
    }

    NLCrossProductData* data = _program->allocFunctionData<NLCrossProductData>();

    // The optional limit handle is a separate operand group, so it never appears
    // among the columns; null leaves the product unbounded.
    data->setLimit(limitStateFor(cross.getLimit()));

    // The results are the outer columns followed by the inner, the order
    // inferReturnTypes lays them out, so walk the result list in step.
    const mlir::ResultRange results = cross.getResults();
    size_t resultIndex = 0;

    for (const mlir::Value column : outerColumns) {
        addCrossColumn(column, results[resultIndex], /*isOuter=*/true, data);
        resultIndex++;
    }

    for (const mlir::Value column : innerColumns) {
        addCrossColumn(column, results[resultIndex], /*isOuter=*/false, data);
        resultIndex++;
    }

    body->emplaceStmt(&NLExecutor::runCrossProduct, data);
}

void NLTranslator::addCrossColumn(mlir::Value inputValue,
                                  mlir::Value resultValue,
                                  bool isOuter,
                                  NLCrossProductData* data) {
    const Column* input = getColumn(inputValue);

    const auto chunkType = mlir::cast<nl::ChunkType>(inputValue.getType());
    const mlir::Type elementType = chunkType.getElementType();

    // The output keeps the input's element type, only the row count changes. A
    // nullable value chunk allocates a ColumnOptVector and broadcasts on its
    // value type; an ID chunk allocates on its chunk kind.
    Column* output = nullptr;
    NLBroadcastFunction broadcast = nullptr;

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        output = allocOptColumnForValueType(valueType);
        broadcast = isOuter ? NLExecutor::selectOptBlockRepeatFunction(valueType)
                            : NLExecutor::selectOptTileFunction(valueType);
    } else {
        const NLChunkKind kind = getChunkKind(chunkType);
        output = allocColumnForKind(kind);
        broadcast = isOuter ? NLExecutor::selectBlockRepeatFunction(kind)
                            : NLExecutor::selectTileFunction(kind);
    }

    _valueSlots[resultValue] = output;

    const NLCrossColumn crossColumn(input, output, broadcast);
    if (isOuter) {
        data->addOuterColumn(crossColumn);
    } else {
        data->addInnerColumn(crossColumn);
    }
}

Column* NLTranslator::allocColumn(mlir::Value chunkValue) {
    const NLChunkKind kind = getChunkKind(chunkValue.getType());

    Column* column = allocColumnForKind(kind);
    _valueSlots[chunkValue] = column;
    return column;
}

// Pool-allocate a chunk column of the right concrete type from the external
// arena, reserving a full chunk so execution stays allocation-free
Column* NLTranslator::allocColumnForKind(NLChunkKind kind) {
    const size_t chunkSize = _program->getChunkSize();

    switch (kind) {
        case NLChunkKind::NodeID: {
            ColumnNodeIDs* column = _memory->alloc<ColumnNodeIDs>();
            column->reserve(chunkSize);
            return column;
        }
        break;

        case NLChunkKind::EdgeID: {
            ColumnEdgeIDs* column = _memory->alloc<ColumnEdgeIDs>();
            column->reserve(chunkSize);
            return column;
        }
        break;

        case NLChunkKind::EdgeTypeID: {
            ColumnEdgeTypes* column = _memory->alloc<ColumnEdgeTypes>();
            column->reserve(chunkSize);
            return column;
        }
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// Pool-allocate a nullable value column (ColumnOptVector) of the right primitive
// for a property's value type, reserving a full chunk so execution stays
// allocation-free. The value type was baked into the chunk during lowering and
// re-resolved from the schema here.
Column* NLTranslator::allocOptColumnForValueType(ValueType valueType) {
    // Per-step loop columns reserve a full chunk so execution stays
    // allocation-free.
    return allocOptColumn(valueType, _program->getChunkSize());
}

Column* NLTranslator::allocSingleRowOptColumnForValueType(ValueType valueType) {
    // The aggregate accumulator only ever holds one row (reset assign(1, ...),
    // update rewrites .front()), so reserve a single element rather than a chunk
    // it would never fill.
    return allocOptColumn(valueType, 1);
}

Column* NLTranslator::allocOptColumn(ValueType valueType, size_t reserveSize) {
    // Allocate a ColumnOptVector for the value type's primitive, reserving
    // reserveSize optionals up front; ValueTypeDispatcher maps the runtime value
    // type to that compile-time primitive.
    Column* column = nullptr;
    const auto allocate = [&]<SupportedType T>() {
        ColumnOptVector<typename T::Primitive>* typed = _memory->alloc<ColumnOptVector<typename T::Primitive>>();
        typed->reserve(reserveSize);
        column = typed;
    };
    ValueTypeDispatcher(valueType).execute(allocate);

    return column;
}

Column* NLTranslator::getColumn(mlir::Value chunkValue) const {
    // Get allocated column for a given MLIR Value
    const auto slotIt = _valueSlots.find(chunkValue);
    if (slotIt == _valueSlots.end()) {
        throw IRException("Chunk value must be a loop variable of an enclosing nl.for");
    }

    return slotIt->second;
}

NLChunkKind NLTranslator::getChunkKind(mlir::Type chunkType) {
    const auto chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        throw IRException("Expected an !nl.chunk type");
    }

    return chunkKindFromElementType(chunk.getElementType());
}

NLChunkKind NLTranslator::chunkKindFromElementType(mlir::Type elementType) {
    if (mlir::isa<storage::NodeIDType>(elementType)) {
        return NLChunkKind::NodeID;
    } else if (mlir::isa<storage::EdgeIDType>(elementType)) {
        return NLChunkKind::EdgeID;
    } else if (mlir::isa<storage::EdgeTypeIDType>(elementType)) {
        return NLChunkKind::EdgeTypeID;
    }

    throw IRException("Unsupported chunk element type");
}
