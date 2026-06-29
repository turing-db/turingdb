#include "NLTranslator.h"

#include <optional>

#include <spdlog/fmt/bundled/format.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Verifier.h"

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
// same !nl.nullable<...> the fetch produced, and this maps it back to allocate
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
        } else if (nl::For forLoop = mlir::dyn_cast<nl::For>(operation)) {
            translateFor(forLoop, body);
        } else if (mlir::isa<nl::GetPropertyType>(operation)) {
            // The handle carries only a name; a fetch resolves it on consumption
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

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runScanNodesLoop, loopData});

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

    NLEdgeLoopData* loopData = _program->allocFunctionData<NLEdgeLoopData>(inputNodeIDs,
                                                                           sources,
                                                                           edgeIDs,
                                                                           edgeTypes,
                                                                           targets);
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

    const bool isOutEdges = config._kind == IteratorKind::GetOutEdges;
    const NLHandlerFunction handler = isOutEdges ? &NLExecutor::runGetOutEdgesLoop
                                                 : &NLExecutor::runGetInEdgesLoop;
    body->addStmt(NLFunctionDescriptor {handler, loopData});

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
    body->addStmt(NLFunctionDescriptor {handler, fetchData});
}

void NLTranslator::translateOutput(nl::Output output, NLStmtContainer* body) {
    // The optional limit handle is a separate operand, so read just the columns;
    // including it in this list would treat the handle as a chunk.
    const mlir::OperandRange columns = output.getColumns();
    if (columns.empty()) {
        throw IRException("nl.output requires at least one column");
    }

    if (!mlir::isa<nl::For>(output->getParentOp())) {
        throw IRException("nl.output must appear inside an nl.for body");
    }

    // Check that the columns passed to output are all variables of the innermost loop
    mlir::Block* outputBlock = output->getBlock();

    NLOutputData* outputData = _program->allocFunctionData<NLOutputData>();
    outputData->setLimit(limitStateFor(output.getLimit()));
    for (const mlir::Value column : columns) {
        const auto columnArgument = mlir::dyn_cast<mlir::BlockArgument>(column);
        const bool isInnermostLoopVariable = columnArgument && columnArgument.getOwner() == outputBlock;

        // A property fetch result is not a loop variable but an op result
        // produced in this same loop body; it is equally available to output.
        mlir::Operation* definingOp = column.getDefiningOp();
        const bool isProducedInThisBlock = definingOp && definingOp->getBlock() == outputBlock;

        if (!isInnermostLoopVariable && !isProducedInThisBlock) {
            throw IRException("nl.output columns must belong to the innermost enclosing nl.for body");
        }

        outputData->addOutputColumn(getColumn(column));
    }

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runOutput, outputData});
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

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runLimitInit, initData});
}

void NLTranslator::translateLimitUpdate(nl::LimitUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so limitStateFor returns its counter or
    // throws if it was not produced by an nl.limit.
    NLLimitState* state = limitStateFor(update.getState());

    const Column* representative = getColumn(update.getRows());
    NLLimitUpdateData* updateData = _program->allocFunctionData<NLLimitUpdateData>(state, representative);

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runLimitUpdate, updateData});
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

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runLimitTruncate, data});
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

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runSkipInit, initData});
}

void NLTranslator::translateSkipUpdate(nl::SkipUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so skipStateFor returns its counter or
    // throws if it was not produced by an nl.skip.
    NLSkipState* state = skipStateFor(update.getState());

    const Column* representative = getColumn(update.getRows());
    NLSkipUpdateData* updateData = _program->allocFunctionData<NLSkipUpdateData>(state, representative);

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runSkipUpdate, updateData});
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

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runSkipTruncate, data});
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

    if (const auto nullableType = mlir::dyn_cast<nl::NullableType>(elementType)) {
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

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runCrossProduct, data});
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
    const size_t chunkSize = _program->getChunkSize();

    // Allocate a ColumnOptVector for the value type's primitive, reserving a
    // full chunk so execution stays allocation-free; ValueTypeDispatcher maps
    // the runtime value type to that compile-time primitive.
    Column* column = nullptr;
    const auto allocate = [&]<SupportedType T>() {
        ColumnOptVector<typename T::Primitive>* typed = _memory->alloc<ColumnOptVector<typename T::Primitive>>();
        typed->reserve(chunkSize);
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

    const mlir::Type elementType = chunk.getElementType();
    if (mlir::isa<storage::NodeIDType>(elementType)) {
        return NLChunkKind::NodeID;
    } else if (mlir::isa<storage::EdgeIDType>(elementType)) {
        return NLChunkKind::EdgeID;
    } else if (mlir::isa<storage::EdgeTypeIDType>(elementType)) {
        return NLChunkKind::EdgeTypeID;
    }

    throw IRException("Unsupported chunk element type");
}
