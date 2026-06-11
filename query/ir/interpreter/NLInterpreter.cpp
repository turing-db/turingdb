#include "NLInterpreter.h"

#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/ScanNodesIterator.h"

#include "NLOutputSink.h"

#include "BioAssert.h"

using namespace db;

namespace {

void runBody(NLExecutionContext& context, const std::vector<NLFunctionDescriptor>& body) {
    for (const NLFunctionDescriptor& descriptor : body) {
        descriptor._function(context, descriptor._data);
    }
}

// The kernel behind NLCarriedColumn::_gather, instantiated per chunk element
// type and selected at translation time. The casts are safe because slots are
// allocated from the same NLChunkKind the gather is selected from.
template <typename ElementType>
void gatherColumn(const Column& input, const ColumnVector<size_t>& indices, Column& output) {
    const auto& typedInput = static_cast<const ColumnVector<ElementType>&>(input);
    auto& typedOutput = static_cast<ColumnVector<ElementType>&>(output);

    typedOutput.clear();
    for (const size_t index : indices.getRaw()) {
        typedOutput.push_back(typedInput[index]);
    }
}

void checkCarriedColumnSizes(const NLEdgeLoopData& loop) {
    const size_t inputSize = loop._inputNodeIDs->size();
    for (const NLCarriedColumn& carriedColumn : loop._carriedColumns) {
        bioassert(carriedColumn._input->size() == inputSize,
                  "Carried chunk must have one row per input node");
    }
}

// The native chunk-stepping loop shared by the two edge-loop handlers. The
// writer fills its bound chunks plus the indices column, where indices[i] is
// the input-chunk row that produced output row i; that one map drives both
// the reconstruction of the gathered side (sources for out-edges, targets for
// in-edges) and the carry-set filtering.
template <typename WriterType>
void runEdgeLoopSteps(NLExecutionContext& context,
                      NLEdgeLoopData& loop,
                      WriterType& writer,
                      ColumnNodeIDs& gatheredNodeIDs) {
    while (writer.isValid()) {
        // fill clears the bound columns before writing the next chunk
        writer.fill(context._chunkSize);

        // Every row of this step may have been tombstone-filtered away
        if (loop._indices.empty()) {
            continue;
        }

        gatherColumn<NodeID>(*loop._inputNodeIDs, loop._indices, gatheredNodeIDs);

        for (const NLCarriedColumn& carriedColumn : loop._carriedColumns) {
            carriedColumn._gather(*carriedColumn._input, loop._indices, *carriedColumn._output);
        }

        runBody(context, loop._body);
    }
}

}

void NLInterpreter::run(const GraphView& view, NLProgram& program, NLOutputSink& sink) {
    NLExecutionContext context {view, &sink, program.getChunkSize()};
    runBody(context, program.getTopLevel());
}

void NLInterpreter::runScanNodesLoop(NLExecutionContext& context, NLFunctionData* data) {
    auto* loop = static_cast<NLScanLoopData*>(data);
    ColumnNodeIDs* nodeIDs = loop->_nodeIDs;

    // The writer lives on the handler's own stack: loop state on the native C
    // stack is the point of the subroutine-threaded design
    ScanNodesChunkWriter writer(context._view);
    writer.setNodeIDs(nodeIDs);

    while (writer.isValid()) {
        writer.fill(context._chunkSize);

        if (nodeIDs->empty()) {
            continue;
        }

        runBody(context, loop->_body);
    }
}

void NLInterpreter::runGetOutEdgesLoop(NLExecutionContext& context, NLFunctionData* data) {
    auto* loop = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loop->_inputNodeIDs;

    if (inputNodeIDs->empty()) {
        return;
    }

    checkCarriedColumnSizes(*loop);

    // Constructed at handler entry, after the enclosing loop filled the input
    // slot: the writer captures the input column's iterators on construction
    GetOutEdgesChunkWriter writer(context._view, inputNodeIDs);
    writer.setIndices(&loop->_indices);
    writer.setEdgeIDs(loop->_edgeIDs);
    writer.setEdgeTypes(loop->_edgeTypes);
    writer.setTgtIDs(loop->_targets);

    // Out-edges walk the successors of the input nodes: the writer fills the
    // target side and the source side is gathered from the input
    runEdgeLoopSteps(context, *loop, writer, *loop->_sources);
}

void NLInterpreter::runGetInEdgesLoop(NLExecutionContext& context, NLFunctionData* data) {
    auto* loop = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loop->_inputNodeIDs;

    if (inputNodeIDs->empty()) {
        return;
    }

    checkCarriedColumnSizes(*loop);

    GetInEdgesChunkWriter writer(context._view, inputNodeIDs);
    writer.setIndices(&loop->_indices);
    writer.setEdgeIDs(loop->_edgeIDs);
    writer.setEdgeTypes(loop->_edgeTypes);
    writer.setSrcIDs(loop->_sources);

    // In-edges walk the predecessors of the input nodes: the writer fills the
    // source side and the target side is gathered from the input
    runEdgeLoopSteps(context, *loop, writer, *loop->_targets);
}

void NLInterpreter::runOutput(NLExecutionContext& context, NLFunctionData* data) {
    const auto* output = static_cast<NLOutputData*>(data);
    bioassert(!output->_columns.empty(), "nl.output requires at least one column");

    const size_t rowCount = output->_columns.front()->size();
    for (const Column* column : output->_columns) {
        bioassert(column->size() == rowCount, "nl.output columns must have the same length");
    }

    context._sink->appendChunks(output->_columns);
}

NLGatherFunction NLInterpreter::selectGatherFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &gatherColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &gatherColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &gatherColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}
