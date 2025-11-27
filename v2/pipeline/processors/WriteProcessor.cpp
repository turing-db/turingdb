#include "WriteProcessor.h"

#include <algorithm>
#include <string_view>

#include "PipelineV2.h"
#include "PipelinePort.h"

#include "ExecutionContext.h"
#include "columns/ColumnIDs.h"
#include "dataframe/NamedColumn.h"
#include "reader/GraphReader.h"
#include "ID.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "dataframe/Dataframe.h"
#include "WriteProcessorTypes.h"
#include "views/PropertyView.h"
#include "writers/MetadataBuilder.h"

#include "FatalException.h"
#include "PipelineException.h"

using namespace db;
using namespace db::v2;

namespace {

using PropertyConstraintsSpan = std::span<const WriteProcessorTypes::PropertyConstraint>;

[[maybe_unused]] void addUntypedProperties(CommitWriteBuffer::UntypedProperties dest,
                          const PropertyConstraintsSpan& props,
                          MetadataBuilder& mdBuilder) {
    for (const WriteProcessorTypes::PropertyConstraint prop : props) {
        [[maybe_unused]] const ValueType type = prop._type;
        // HOW: Do I know what the value of the property to set is? It will be in a column
        // (for which I have a tag) but I do not know which row.
    }
}

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
        processor->addInput(inputPort);
        inputPort->setNeedsData(true);
    }

    {
        auto* outputPort = PipelineOutputPort::create(pipeline, processor);
        processor->_output.setPort(outputPort);
        processor->addOutput(outputPort);
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
    _input.getPort()->consume();
    _output.getPort()->writeData();
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

LabelSet WriteProcessor::getLabelSet(std::span<const std::string_view> labels) {
    LabelSet labelset;
    for (const std::string_view label : labels) {
        const LabelID id = _metadataBuilder->getOrCreateLabel(label);
        labelset.set(id);
    }
    return labelset;
}

void WriteProcessor::createNodes(size_t numIters) {
    NodeID nextNodeID = _writeBuffer->numPendingNodes();
    Dataframe* outDf = _output.getDataframe();
    for (const WriteProcessorTypes::PendingNode& node : _pendingNodes) {
        // Get the column in the output DF which will contain this node's IDs
        if (!outDf->hasColumn(node._tag)) {
            throw FatalException(
                fmt::format("Could not get column in WriteProcessor "
                            "output dataframe for PendingNode with tag {}.",
                            node._tag.getValue()));
        }
        auto* col = _output.getDataframe()->getColumn(node._tag)->as<ColumnNodeIDs>();
        bioassert(col->size() == 0);

        const std::span labels = node._labels;
        // TODO: Is this checked in planner?
        bioassert(labels.size() > 0);
        const LabelSet lblset = getLabelSet(labels);

        // Add each node that we need to the CommitWriteBuffer
        // Add labels first as they are locally contiguous in vector, keep cache hot
        for (size_t i = 0; i < numIters; i++) {
            CommitWriteBuffer::PendingNode& newNode = _writeBuffer->newPendingNode();
            newNode.labelsetHandle = _metadataBuilder->getOrCreateLabelSet(lblset);
        }
        // TODO: Properties

        std::vector<NodeID>& raw = col->getRaw();
        raw.resize(raw.size() + numIters);

        // Fill the output column with "fake IDs"
        std::iota(col->begin(), col->end(), nextNodeID);
        nextNodeID += numIters;
    }
}

// @warn assumes all pending nodes have already been created
void WriteProcessor::createEdges(size_t numIters) {
    EdgeID nextEdgeID = _writeBuffer->numPendingEdges();

    for (const WriteProcessorTypes::PendingEdge& edge : _pendingEdges) {
        Dataframe* outDf = _output.getDataframe();

        if (!outDf->hasColumn(edge._tag)) {
            throw FatalException(
                fmt::format("Could not get column in WriteProcessor "
                            "output dataframe for PendingEdge with tag {}.",
                            edge._tag.getValue()));
        }
        auto* col = outDf->getColumn(edge._tag)->as<ColumnEdgeIDs>();
        bioassert(col->size() == 0);

        ColumnNodeIDs* srcCol {nullptr};
        const ColumnTag srcTag = edge._srcTag;
        // If the tag exists in the input, it must be an already existing NodeID
        // column - e.g. something returned by a MATCH query - otherwise it is a
        // pending node which was created in this CREATE query
        const bool srcIsPending = _input.getDataframe()->getColumn(srcTag) == nullptr;
        // However, all input columns are also propagated to the output, so we can
        // always retrieve the column from the output dataframe, regardless of whether
        // the source node was the result of a CREATE or a MATCH
        if (!outDf->hasColumn(srcTag)) {
            throw FatalException(fmt::format("Attempted to create edge {} with "
                                             "source node with no such column: {}",
                                             edge._name, edge._srcTag.getValue()));
        }
        srcCol = _output.getDataframe()->getColumn(srcTag)->as<ColumnNodeIDs>();

        ColumnNodeIDs* tgtCol {nullptr};
        const ColumnTag tgtTag = edge._tgtTag;
        const bool tgtIsPending = _input.getDataframe()->getColumn(tgtTag) == nullptr;
        if (!outDf->hasColumn(tgtTag)) {
            throw FatalException(fmt::format("Attempted to create edge {} with "
                                             "target node with no such column: {}",
                                             edge._name, edge._tgtTag.getValue()));
        }
        tgtCol = _output.getDataframe()->getColumn(tgtTag)->as<ColumnNodeIDs>();

        bioassert(tgtCol->size() == srcCol->size());
        bioassert(tgtCol->size() == numIters);

        for (size_t rowPtr = 0; rowPtr < numIters; rowPtr++) {
            CommitWriteBuffer::ExistingOrPendingNode source =
                srcIsPending ? srcCol->at(rowPtr) // Exists => NodeID
                             : size_t {srcCol->at(rowPtr).getValue()}; // Pending => size_t offset in CWB

            CommitWriteBuffer::ExistingOrPendingNode target =
                tgtIsPending ? tgtCol->at(rowPtr)// Exists => NodeID
                             : size_t {tgtCol->at(rowPtr).getValue()}; // Pending => size_t offset in CWB

            _writeBuffer->newPendingEdge(source, target);
        }

        std::vector<EdgeID>& raw = col->getRaw();
        raw.resize(raw.size() + numIters);

        std::iota(col->begin(), col->end(), nextEdgeID);
        nextEdgeID += numIters;
    }
}

void WriteProcessor::postProcessFakeIDs() {
    const NodeID nextNodeID = _ctxt->getGraphView().read().getTotalNodesAllocated();

    for (const WriteProcessorTypes::PendingNode& node : _pendingNodes) {
        // Get the column in the output DF which will contain this node's IDs
        auto* col = _output.getDataframe()->getColumn(node._tag)->as<ColumnNodeIDs>();
        if (!col) {
            throw FatalException(
                fmt::format("Could not get column in WriteProcessor "
                            "output dataframe for PendingNode with tag {}.",
                            node._tag.getValue()));
        }

        std::vector<NodeID>& raw = col->getRaw();
        std::ranges::for_each(raw, [nextNodeID] (NodeID& fakeID) { fakeID += nextNodeID; });
    }

    const EdgeID nextEdgeID = _ctxt->getGraphView().read().getTotalEdgesAllocated();
    for (const WriteProcessorTypes::PendingEdge& edge : _pendingEdges) {
        // Get the column in the output DF which will contain this node's IDs
        auto* col = _output.getDataframe()->getColumn(edge._tag)->as<ColumnEdgeIDs>();
        if (!col) {
            throw FatalException(
                fmt::format("Could not get column in WriteProcessor "
                            "output dataframe for PendingEdge with tag {}.",
                            edge._tag.getValue()));
        }

        std::vector<EdgeID>& raw = col->getRaw();
        std::ranges::for_each(raw, [nextEdgeID](EdgeID& fakeID) { fakeID += nextEdgeID; });
    }
}

void WriteProcessor::performCreations() {
    // We apply the CREATE command for each row in the input
    const size_t numIters = _input.getDataframe()->getRowCount();

    // 1. Node creations
    createNodes(numIters);
    createEdges(numIters);

    // 2. Link up edges

    postProcessFakeIDs();
}
