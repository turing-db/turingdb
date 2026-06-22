#include "NLTranslator.h"

#include <optional>

#include <spdlog/fmt/bundled/format.h>

#include "mlir/IR/Block.h"
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

namespace {

// The with-null fetch handler for a property's value type, on the node side
// when isNode is true and the edge side otherwise. Selecting it here keeps the
// value-type dispatch with the rest of translation; the handler bodies live in
// NLExecutor.
NLHandlerFunction selectPropertyFetchHandler(bool isNode, ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return isNode ? &NLExecutor::runPropertyFetch<NodeID, types::Int64>
                          : &NLExecutor::runPropertyFetch<EdgeID, types::Int64>;
        break;

        case ValueType::UInt64:
            return isNode ? &NLExecutor::runPropertyFetch<NodeID, types::UInt64>
                          : &NLExecutor::runPropertyFetch<EdgeID, types::UInt64>;
        break;

        case ValueType::Double:
            return isNode ? &NLExecutor::runPropertyFetch<NodeID, types::Double>
                          : &NLExecutor::runPropertyFetch<EdgeID, types::Double>;
        break;

        case ValueType::Bool:
            return isNode ? &NLExecutor::runPropertyFetch<NodeID, types::Bool>
                          : &NLExecutor::runPropertyFetch<EdgeID, types::Bool>;
        break;

        case ValueType::String:
            return isNode ? &NLExecutor::runPropertyFetch<NodeID, types::String>
                          : &NLExecutor::runPropertyFetch<EdgeID, types::String>;
        break;

        case ValueType::Embedding:
            return isNode ? &NLExecutor::runPropertyFetch<NodeID, types::Embedding>
                          : &NLExecutor::runPropertyFetch<EdgeID, types::Embedding>;
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("Invalid property value type");
        break;
    }

    throw IRException("Unhandled property value type");
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

void NLTranslator::translateFor(const nl::For& forLoop, NLStmtContainer* body) {
    // Fetch the iterator associated to this loop
    const auto configIt = _iteratorConfigs.find(forLoop->getOperand(0));
    if (configIt == _iteratorConfigs.end()) {
        throw IRException("nl.for iterator must be produced by an nl source operation");
    }

    const IteratorConfig& config = configIt->second;
    mlir::Block& loopBody = forLoop->getRegion(0).front();

    // Translate the loop differently depending on the kind of iterator associated
    if (config._kind == IteratorKind::ScanNodes) {
        translateScanLoop(loopBody, body);
    } else {
        translateEdgeLoop(config, loopBody, body);
    }
}

void NLTranslator::translateScanLoop(mlir::Block& loopBody, NLStmtContainer* body) {
    // For::verify guarantees one block argument per iterator chunk, and a
    // node scan iterator has exactly one chunk of node IDs
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));

    NLScanLoopData* loopData = _program->allocFunctionData<NLScanLoopData>(nodeIDs);

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runScanNodesLoop, loopData});

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateEdgeLoop(const IteratorConfig& config,
                                     mlir::Block& loopBody,
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

void NLTranslator::translateOutput(const nl::Output& output, NLStmtContainer* body) {
    const mlir::OperandRange columns = output->getOperands();
    if (columns.empty()) {
        throw IRException("nl.output requires at least one column");
    }

    if (!mlir::isa<nl::For>(output->getParentOp())) {
        throw IRException("nl.output must appear inside an nl.for body");
    }

    // Check that the columns passed to output are all variables of the innermost loop
    mlir::Block* outputBlock = output->getBlock();

    NLOutputData* outputData = _program->allocFunctionData<NLOutputData>();
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

    switch (valueType) {
        case ValueType::Int64: {
            ColumnOptVector<types::Int64::Primitive>* column = _memory->alloc<ColumnOptVector<types::Int64::Primitive>>();
            column->reserve(chunkSize);
            return column;
        }
        break;

        case ValueType::UInt64: {
            ColumnOptVector<types::UInt64::Primitive>* column = _memory->alloc<ColumnOptVector<types::UInt64::Primitive>>();
            column->reserve(chunkSize);
            return column;
        }
        break;

        case ValueType::Double: {
            ColumnOptVector<types::Double::Primitive>* column = _memory->alloc<ColumnOptVector<types::Double::Primitive>>();
            column->reserve(chunkSize);
            return column;
        }
        break;

        case ValueType::Bool: {
            ColumnOptVector<types::Bool::Primitive>* column = _memory->alloc<ColumnOptVector<types::Bool::Primitive>>();
            column->reserve(chunkSize);
            return column;
        }
        break;

        case ValueType::String: {
            ColumnOptVector<types::String::Primitive>* column = _memory->alloc<ColumnOptVector<types::String::Primitive>>();
            column->reserve(chunkSize);
            return column;
        }
        break;

        case ValueType::Embedding: {
            ColumnOptVector<types::Embedding::Primitive>* column = _memory->alloc<ColumnOptVector<types::Embedding::Primitive>>();
            column->reserve(chunkSize);
            return column;
        }
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("Invalid property value type");
        break;
    }

    bioassert(false, "Unhandled property value type");
    return nullptr;
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
    if (mlir::isa<nl::NodeIDType>(elementType)) {
        return NLChunkKind::NodeID;
    } else if (mlir::isa<nl::EdgeIDType>(elementType)) {
        return NLChunkKind::EdgeID;
    } else if (mlir::isa<nl::EdgeTypeIDType>(elementType)) {
        return NLChunkKind::EdgeTypeID;
    }

    throw IRException("Unsupported chunk element type");
}
