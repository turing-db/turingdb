#include "NLTranslator.h"

#include <spdlog/fmt/bundled/format.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/Verifier.h"

#include "NLInterpreter.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;

NLTranslator::NLTranslator(NLProgram* program)
    : _program(program)
{
}

NLTranslator::~NLTranslator() {
}

void NLTranslator::translate(const mlir::func::FuncOp& function) {
    // The op verifiers carry the type guarantees the column casts below rely
    // on. The const handle converts to Operation* through its const
    // conversion operator.
    if (mlir::failed(mlir::verify(function))) {
        throw IRException("nl function failed MLIR verification");
    }

    // The generated accessors are non-const, so the const handle is read
    // through its const operator-> into the generic Operation API: region 0
    // of a func.func is its body
    mlir::Region& bodyRegion = function->getRegion(0);
    if (!bodyRegion.hasOneBlock()) {
        throw IRException("NLTranslator expects a function with a single block");
    }

    translateBlock(bodyRegion.front(), _program->getStmts());
}

void NLTranslator::translateBlock(mlir::Block& block, NLStmtContainer* body) {
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

void NLTranslator::translateFor(const nl::For& forLoop, NLStmtContainer* body) {
    // The iterator is the only operand of nl.for and the loop body is the
    // single block of its only region, both read through the const operator->
    const auto configIt = _iteratorConfigs.find(forLoop->getOperand(0));
    if (configIt == _iteratorConfigs.end()) {
        throw IRException("nl.for iterator must be produced by an nl source operation");
    }

    const IteratorConfig& config = configIt->second;
    mlir::Block& loopBody = forLoop->getRegion(0).front();

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

    body->addStmt(NLFunctionDescriptor {&NLInterpreter::runScanNodesLoop, loopData});

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

    // The scratch indices column gets the same up-front reservation as the
    // other columns, keeping execution allocation-free
    loopData->getIndices()->reserve(_program->getChunkSize());

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

        const NLChunkKind kind = getChunkKind(carriedValue.getType());
        Column* carriedOutput = allocColumn(loopBody.getArgument(static_cast<unsigned>(4 + carriedIndex)));

        const NLCarriedColumn carriedColumn(getColumn(carriedValue),
                                            carriedOutput,
                                            NLInterpreter::selectGatherFunction(kind));
        loopData->addCarriedColumn(carriedColumn);
    }

    const bool isOutEdges = config._kind == IteratorKind::GetOutEdges;
    const NLHandlerFunction handler = isOutEdges ? &NLInterpreter::runGetOutEdgesLoop
                                                 : &NLInterpreter::runGetInEdgesLoop;
    body->addStmt(NLFunctionDescriptor {handler, loopData});

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateOutput(const nl::Output& output, NLStmtContainer* body) {
    // The columns are the only operands of nl.output, read through the const
    // operator-> since the generated accessors are non-const
    const mlir::OperandRange columns = output->getOperands();
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

    NLOutputData* outputData = _program->allocFunctionData<NLOutputData>();
    for (const mlir::Value column : columns) {
        const auto columnArgument = mlir::dyn_cast<mlir::BlockArgument>(column);
        const bool isInnermostLoopVariable = columnArgument && columnArgument.getOwner() == outputBlock;
        if (!isInnermostLoopVariable) {
            throw IRException("nl.output columns must be loop variables of the innermost enclosing nl.for");
        }

        outputData->addOutputColumn(getColumn(column));
    }

    body->addStmt(NLFunctionDescriptor {&NLInterpreter::runOutput, outputData});
}

Column* NLTranslator::allocColumn(mlir::Value chunkValue) {
    const NLChunkKind kind = getChunkKind(chunkValue.getType());

    Column* column = _program->allocColumn(kind);
    _valueSlots[chunkValue] = column;
    return column;
}

Column* NLTranslator::getColumn(mlir::Value chunkValue) const {
    // Chunk SSA values only exist as nl.for block arguments, each of which
    // was registered when its loop was translated
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
