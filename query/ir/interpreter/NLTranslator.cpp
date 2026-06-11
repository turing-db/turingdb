#include "NLTranslator.h"

#include <spdlog/fmt/bundled/format.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/Verifier.h"

#include "NLInterpreter.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;

NLTranslator::NLTranslator(NLProgram& program)
    : _program(program)
{
}

NLTranslator::~NLTranslator() {
}

void NLTranslator::translate(mlir::func::FuncOp function) {
    // The op verifiers carry the type guarantees the slot casts below rely on
    if (mlir::failed(mlir::verify(function.getOperation()))) {
        throw IRException("nl function failed MLIR verification");
    }

    mlir::Region& bodyRegion = function.getBody();
    if (!bodyRegion.hasOneBlock()) {
        throw IRException("NLTranslator expects a function with a single block");
    }

    translateBlock(bodyRegion.front(), _program.getTopLevel());
}

void NLTranslator::translateBlock(mlir::Block& block, std::vector<NLFunctionDescriptor>& body) {
    for (mlir::Operation& operation : block) {
        if (auto scanNodes = mlir::dyn_cast<nl::ScanNodes>(operation)) {
            _iteratorConfigs[scanNodes.getResult()] = IteratorConfig {IteratorKind::ScanNodes, {}, {}};
        } else if (auto getOutEdges = mlir::dyn_cast<nl::GetOutEdges>(operation)) {
            IteratorConfig config {IteratorKind::GetOutEdges, getOutEdges.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getOutEdges.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            _iteratorConfigs[getOutEdges.getResult()] = config;
        } else if (auto getInEdges = mlir::dyn_cast<nl::GetInEdges>(operation)) {
            IteratorConfig config {IteratorKind::GetInEdges, getInEdges.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getInEdges.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            _iteratorConfigs[getInEdges.getResult()] = config;
        } else if (auto forLoop = mlir::dyn_cast<nl::For>(operation)) {
            translateFor(forLoop, body);
        } else if (auto output = mlir::dyn_cast<nl::Output>(operation)) {
            translateOutput(output, body);
        } else if (mlir::isa<nl::Yield, mlir::func::ReturnOp>(operation)) {
            // Structural terminators carry no behavior
        } else {
            throw IRException(fmt::format("NLTranslator cannot translate operation '{}'",
                                          operation.getName().getStringRef().str()));
        }
    }
}

void NLTranslator::translateFor(nl::For forLoop, std::vector<NLFunctionDescriptor>& body) {
    const auto configIt = _iteratorConfigs.find(forLoop.getIterator());
    if (configIt == _iteratorConfigs.end()) {
        throw IRException("nl.for iterator must be produced by an nl source operation");
    }

    const IteratorConfig& config = configIt->second;
    mlir::Block* loopBody = forLoop.getBody();

    if (config._kind == IteratorKind::ScanNodes) {
        translateScanLoop(loopBody, body);
    } else {
        translateEdgeLoop(config, loopBody, body);
    }
}

void NLTranslator::translateScanLoop(mlir::Block* loopBody, std::vector<NLFunctionDescriptor>& body) {
    // For::verify guarantees one block argument per iterator chunk, and a
    // node scan iterator has exactly one chunk of node IDs
    Column* nodeIDsSlot = _program.addChunkSlot(NLChunkKind::NodeID);
    _valueSlots[loopBody->getArgument(0)] = nodeIDsSlot;

    NLScanLoopData* loopData = _program.addFunctionData<NLScanLoopData>();
    loopData->_nodeIDs = static_cast<ColumnNodeIDs*>(nodeIDsSlot);

    body.push_back(NLFunctionDescriptor {&NLInterpreter::runScanNodesLoop, loopData});

    translateBlock(*loopBody, loopData->_body);
}

void NLTranslator::translateEdgeLoop(const IteratorConfig& config,
                                     mlir::Block* loopBody,
                                     std::vector<NLFunctionDescriptor>& body) {
    NLEdgeLoopData* loopData = _program.addFunctionData<NLEdgeLoopData>();
    loopData->_inputNodeIDs = static_cast<const ColumnNodeIDs*>(resolveChunkSlot(config._inputNodes));

    // The scratch indices column gets the same up-front reservation as the
    // slots, keeping execution allocation-free
    loopData->_indices.reserve(_program.getChunkSize());

    // The four fixed chunks of an edge iterator step, in the block-argument
    // order established by getEdgeIteratorType: sources, edge IDs, edge type
    // IDs, targets
    Column* sourcesSlot = _program.addChunkSlot(NLChunkKind::NodeID);
    Column* edgeIDsSlot = _program.addChunkSlot(NLChunkKind::EdgeID);
    Column* edgeTypesSlot = _program.addChunkSlot(NLChunkKind::EdgeTypeID);
    Column* targetsSlot = _program.addChunkSlot(NLChunkKind::NodeID);

    loopData->_sources = static_cast<ColumnNodeIDs*>(sourcesSlot);
    loopData->_edgeIDs = static_cast<ColumnEdgeIDs*>(edgeIDsSlot);
    loopData->_edgeTypes = static_cast<ColumnEdgeTypes*>(edgeTypesSlot);
    loopData->_targets = static_cast<ColumnNodeIDs*>(targetsSlot);

    _valueSlots[loopBody->getArgument(0)] = sourcesSlot;
    _valueSlots[loopBody->getArgument(1)] = edgeIDsSlot;
    _valueSlots[loopBody->getArgument(2)] = edgeTypesSlot;
    _valueSlots[loopBody->getArgument(3)] = targetsSlot;

    // One carried chunk per columns_to_filter entry, bound as trailing loop
    // variables after the four fixed chunks
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

        const NLChunkKind kind = chunkKindOf(carriedValue.getType());

        Column* carriedOutSlot = _program.addChunkSlot(kind);
        _valueSlots[loopBody->getArgument(static_cast<unsigned>(4 + carriedIndex))] = carriedOutSlot;

        NLCarriedColumn carriedColumn;
        carriedColumn._input = resolveChunkSlot(carriedValue);
        carriedColumn._output = carriedOutSlot;
        carriedColumn._gather = NLInterpreter::selectGatherFunction(kind);
        loopData->_carriedColumns.push_back(carriedColumn);
    }

    const bool isOutEdges = config._kind == IteratorKind::GetOutEdges;
    const NLHandlerFunction handler = isOutEdges ? &NLInterpreter::runGetOutEdgesLoop
                                                 : &NLInterpreter::runGetInEdgesLoop;
    body.push_back(NLFunctionDescriptor {handler, loopData});

    translateBlock(*loopBody, loopData->_body);
}

void NLTranslator::translateOutput(nl::Output output, std::vector<NLFunctionDescriptor>& body) {
    const mlir::OperandRange columns = output.getColumns();
    if (columns.empty()) {
        throw IRException("nl.output requires at least one column");
    }

    if (!mlir::isa<nl::For>(output->getParentOp())) {
        throw IRException("nl.output must appear inside an nl.for body");
    }

    // The output chunks are zipped into rows, so they must all describe the
    // same loop step: block arguments of the innermost enclosing nl.for. An
    // outer loop variable has no per-row correspondence to the current step -
    // it must be carried through columns_to_filter to reach this depth. The
    // op constrains only types, so a cross-loop operand passes MLIR
    // verification and has to be rejected here.
    mlir::Block* outputBlock = output->getBlock();

    NLOutputData* outputData = _program.addFunctionData<NLOutputData>();
    for (const mlir::Value column : columns) {
        const auto columnArgument = mlir::dyn_cast<mlir::BlockArgument>(column);
        const bool isInnermostLoopVariable = columnArgument && columnArgument.getOwner() == outputBlock;
        if (!isInnermostLoopVariable) {
            throw IRException("nl.output columns must be loop variables of the innermost enclosing nl.for");
        }

        outputData->_columns.push_back(resolveChunkSlot(column));
    }

    body.push_back(NLFunctionDescriptor {&NLInterpreter::runOutput, outputData});
}

Column* NLTranslator::resolveChunkSlot(mlir::Value chunkValue) {
    // Chunk SSA values only exist as nl.for block arguments, each of which
    // was registered when its loop was translated
    const auto slotIt = _valueSlots.find(chunkValue);
    if (slotIt == _valueSlots.end()) {
        throw IRException("Chunk value must be a loop variable of an enclosing nl.for");
    }

    return slotIt->second;
}

NLChunkKind NLTranslator::chunkKindOf(mlir::Type chunkType) {
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
