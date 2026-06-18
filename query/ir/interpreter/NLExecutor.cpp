#include "NLExecutor.h"

#include <type_traits>

#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetPropertiesWithNullIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLOutputSink.h"

#include "BioAssert.h"

using namespace db;

namespace {

// Execute a body of statements
void runBody(NLExecutionContext* context, const NLStmtContainer* body) {
    for (const NLFunctionDescriptor& descriptor : body->stmts()) {
        const auto func = descriptor.getFunction();
        NLFunctionData* funcData = descriptor.getData();
        func(context, funcData);
    }
}

// Gather rows of a carried column by applying indices
template <typename ElementType>
void gatherColumn(const Column* input,
                  const ColumnVector<size_t>* indices,
                  Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    typedOutput->clear();
    typedOutput->reserve(indices->size());
    const auto& indicesRaw = indices->getRaw();
    auto& typedInputRaw = typedInput->getRaw();

    for (const size_t index : indicesRaw) {
        typedOutput->push_back(typedInputRaw[index]);
    }
}

// Execute a get_out_edges/get_in_edges loop
template <typename ChunkWriterType>
void runEdgeLoopSteps(NLExecutionContext* context,
                      NLEdgeLoopData* loopData,
                      ChunkWriterType* chunkWriter,
                      ColumnNodeIDs* gatheredNodeIDs) {
    const NLStmtContainer* loopBody = loopData->getStmts();

    while (chunkWriter->isValid()) {
        chunkWriter->fill(context->getChunkSize());

        const ColumnVector<size_t>* indices = loopData->getIndices();
        if (indices->empty()) {
            continue;
        }

        // Gather either source or target if we are get_out or get_in (the source side)
        gatherColumn<NodeID>(loopData->getInput(), indices, gatheredNodeIDs);

        // Transform all the columns in the carried set according to indices
        for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), indices, carriedColumn.getOutput());
        }

        runBody(context, loopBody);
    }
}

}

NLExecutor::NLExecutor(const GraphView* view,
                             const NLProgram* prog,
                             NLOutputSink* sink)
    : _ctxt(view, sink, prog->getChunkSize()),
    _prog(prog)
{
}

NLExecutor::~NLExecutor() {
}

void NLExecutor::run() {
    runBody(&_ctxt, _prog->getStmts());
}

void NLExecutor::runScanNodesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanLoopData* loopData = static_cast<NLScanLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();
    const size_t chunkSize = context->getChunkSize();

    ScanNodesChunkWriter chunkWriter(*context->getView());
    chunkWriter.setNodeIDs(nodeIDs);

    while (chunkWriter.isValid()) {
        chunkWriter.fill(chunkSize);

        if (nodeIDs->empty()) {
            continue;
        }

        runBody(context, loopBody);
    }
}

void NLExecutor::runGetOutEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetOutEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    runEdgeLoopSteps(context, loopData, &chunkWriter, loopData->getSources());
}

void NLExecutor::runGetInEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetInEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    runEdgeLoopSteps(context, loopData, &chunkWriter, loopData->getTargets());
}

void NLExecutor::runOutput(NLExecutionContext* context, NLFunctionData* data) {
    const NLOutputData* output = static_cast<NLOutputData*>(data);
    const auto& cols = output->outputs();
    bioassert(!cols.empty(), "nl.output requires at least one column");

    NLOutputSink* sink = context->getSink();
    sink->appendChunks(cols);
}

NLGatherFunction NLExecutor::selectGatherFunction(NLChunkKind kind) {
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

// Read one property of the current input chunk into a nullable value column.
// The with-null writer emits one value per input row (null where the row lacks
// it), so no row is dropped and the value column lines up with the input chunk.
// The PropertyTypeID was resolved from the name during translation.
template <typename ID, typename T>
void NLExecutor::runPropertyFetch(NLExecutionContext* context, NLFunctionData* data) {
    NLPropertyFetchData* fetchData = static_cast<NLPropertyFetchData*>(data);

    const GraphView& view = *context->getView();
    const PropertyTypeID propertyTypeID = fetchData->getPropertyTypeID();
    const auto* inputIDs = static_cast<const ColumnVector<ID>*>(fetchData->getInput());
    auto* output = static_cast<ColumnOptVector<typename T::Primitive>*>(fetchData->getOutput());

    GetPropertiesWithNullChunkWriter<ID, T> writer(view, propertyTypeID, inputIDs);
    writer.setOutput(output);
    writer.fill(inputIDs->size());
}

// The translator selects among these by the value type the property resolves
// to, on the node or edge side; only these (ID, T) pairs are available as
// handlers. String and Embedding are not lowered yet.
template void NLExecutor::runPropertyFetch<NodeID, types::Int64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::UInt64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Double>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Bool>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Int64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::UInt64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Double>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Bool>(NLExecutionContext*, NLFunctionData*);
