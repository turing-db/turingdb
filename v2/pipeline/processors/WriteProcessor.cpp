#include "WriteProcessor.h"

#include "ExecutionContext.h"
#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "ID.h"
#include "PipelineV2.h"
#include "PipelinePort.h"
#include "dataframe/Dataframe.h"

#include "FatalException.h"
#include "PipelineException.h"

using namespace db::v2;

namespace {

using namespace db;

template <TypedInternalID IDT>
void validateDeletions(const GraphReader reader, ColumnVector<IDT>* col) {
    for (IDT id : *col) {
        bool existsAndNotDeleted {false};

        if constexpr (std::is_same_v<IDT, NodeID>) {
            existsAndNotDeleted = reader.graphHasNode(id);
        } else if constexpr (std::is_same_v<IDT, EdgeID>) {
            existsAndNotDeleted = reader.graphHasEdge(id);
        }

        if (!existsAndNotDeleted) [[unlikely]] {
            std::string err =
                fmt::format("Graph does not contain {} with ID: {}.",
                            std::is_same_v<IDT, NodeID> ? "node" : "edge",
                            id.getValue());
            throw PipelineException(std::move(err));
        }
    }
}

template void validateDeletions<NodeID>(const GraphReader reader, ColumnVector<NodeID>* col);
template void validateDeletions<EdgeID>(const GraphReader reader, ColumnVector<EdgeID>* col);

}

WriteProcessor* WriteProcessor::create(PipelineV2* pipeline) {
    auto* processor = new WriteProcessor();

    {
        auto* inputPort = PipelineInputPort::create(pipeline, processor);
        processor->_input.setPort(inputPort);
    }

    {
        auto* outputPort = PipelineOutputPort::create(pipeline, processor);
        processor->_output.setPort(outputPort);
    }

    processor->postCreate(pipeline);
    return processor;
}

void WriteProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx) {
        throw FatalException("Attempted to prepare WriteProcessor in execution context "
                             "without transaction.");
    }

    if (!rawTx->writingPendingCommit()) {
        throw PipelineException("WriteProcessor: Cannot perform writes outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();
    CommitBuilder* commitBuilder = tx.commitBuilder();

    _metadataBuilder = &commitBuilder->metadata();
    _writeBuffer = &commitBuilder->writeBuffer();
}

void WriteProcessor::reset() {
    markAsReset();
}

void WriteProcessor::execute() {
    if (!_deletedNodes.empty() || !_deletedEdges.empty()) {
        performDeletions();
    }

    if (!_pendingNodes.empty() || !_pendingEdges.empty()) {
        performCreations();
    }
    finish();
}

void WriteProcessor::performDeletions() {
    const GraphReader reader = _ctxt->getGraphView().read();
    const Dataframe* inDf =_input.getDataframe();

    for (const ColumnTag deletedTag : _deletedNodes) {
        auto* nodeColumn = inDf->getColumn(deletedTag)->as<ColumnNodeIDs>();

        if (!nodeColumn) { // @ref as performs dynamic_cast
            throw FatalException(fmt::format("Column {} was marked as a column of"
                                             " deleted nodes, but is not a NodeID"
                                             " column.", deletedTag.getValue()));
        }

        validateDeletions(reader, nodeColumn);
        _writeBuffer->addDeletedNodes(nodeColumn->getRaw());
        // TODO: Guard around if the query is DETACH or NODETACH
        _writeBuffer->addHangingEdges(_ctxt->getGraphView());
    }

    for (const ColumnTag deletedTag : _deletedEdges) {
        auto* edgeColumn = inDf->getColumn(deletedTag)->as<ColumnEdgeIDs>();

        if (!edgeColumn) { // @ref as performs dynamic_cast
            throw FatalException(fmt::format("Column {} was marked as a column of"
                                             " deleted nodes, but is not a EdgeID"
                                             " column.", deletedTag.getValue()));
        }

        validateDeletions(reader, edgeColumn);
        _writeBuffer->addDeletedEdges(edgeColumn->getRaw());
    }
}

void WriteProcessor::performCreations() {
    for (const auto& pendingNode : _pendingNodes) {
        // TODO: Throw an exception, or make an implict assumption that we have valid
        // labels
        bioassert(pendingNode._labels.size() > 1);

    }
}
