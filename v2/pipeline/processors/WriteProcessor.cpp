#include "WriteProcessor.h"

#include <algorithm>
#include <optional>
#include <string_view>
#include <utility>

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

template <TypedInternalID IDT>
void validateDeletions(const GraphReader reader, const ColumnVector<IDT>* col) {
    if (!col) {
        throw FatalException("Attempted to delete null column.");
    }

    constexpr bool isNode = std::is_same_v<IDT, NodeID>;

    for (const IDT id : *col) {
        bool existsAndNotDeleted {false};

        if constexpr (isNode) {
            existsAndNotDeleted = reader.graphHasNode(id);
        } else {
            existsAndNotDeleted = reader.graphHasEdge(id);
        }

        if (!existsAndNotDeleted) [[unlikely]] {
            throw PipelineException(fmt::format("Graph does not contain {} with ID: {}.",
                                                isNode ? "node" : "edge", id.getValue()));
        }
    }
}

template void validateDeletions<NodeID>(const GraphReader reader, const ColumnVector<NodeID>* col);
template void validateDeletions<EdgeID>(const GraphReader reader, const ColumnVector<EdgeID>* col);

}

WriteProcessor* WriteProcessor::create(PipelineV2* pipeline, bool hasInput) {
    auto* processor = new WriteProcessor();

    if (hasInput) {
        processor->_input = std::make_optional<PipelineBlockInputInterface>();

        auto* inputPort = PipelineInputPort::create(pipeline, processor);
        processor->_input->setPort(inputPort);
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
    // NOTE: We currently do not have `CREATE (n) DELETE n` supported in PlanGraph,
    // meaning if we are deleting, we require an input. This may change.
    if (_input && (!_deletedNodes.empty() || !_deletedEdges.empty())) {
        performDeletions();
    }

    if (!_pendingNodes.empty() || !_pendingEdges.empty()) {
        performCreations();
    }

    if (_input) {
        _input->getPort()->consume();
    }
    _output.getPort()->writeData();
    finish();
}

void WriteProcessor::performDeletions() {
    bioassert(_input);

    const GraphReader reader = _ctxt->getGraphView().read();
    const Dataframe* inDf = _input->getDataframe();

    for (const ColumnTag deletedTag : _deletedNodes) {
        if (!inDf->hasColumn(deletedTag)) {
            throw FatalException(fmt::format(
                "Attempted to delete nodes in Column {}, but found no such column "
                "in the input Dataframe.",
                deletedTag.getValue()));
        }

        const auto* nodeColumn = inDf->getColumn(deletedTag)->as<ColumnNodeIDs>();
        if (!nodeColumn) { // @ref as<> performs dynamic_cast
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
        if (!inDf->hasColumn(deletedTag)) {
            throw FatalException(fmt::format(
                "Attempted to delete edges in Column {}, but found no such column "
                "in the input Dataframe.",
                deletedTag.getValue()));
        }

        const auto* edgeColumn = inDf->getColumn(deletedTag)->as<ColumnEdgeIDs>();
        if (!edgeColumn) { // @ref as<> performs dynamic_cast
            throw FatalException(fmt::format("Column {} was marked as a column of"
                                             " deleted edges, but is not an EdgeID"
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
    const Dataframe* outDf = _output.getDataframe();

    for (const WriteProcessorTypes::PendingNode& node : _pendingNodes) {
        // Get the column in the output DF which will contain this node's IDs
        if (!outDf->hasColumn(node._tag)) {
            throw FatalException(
                fmt::format("Could not get column in WriteProcessor "
                            "output dataframe for PendingNode with tag {}.",
                            node._tag.getValue()));
        }
        auto* col = _output.getDataframe()->getColumn(node._tag)->as<ColumnNodeIDs>();
        if (!col) { // @ref as<> performs dynamic_cast
            throw FatalException(fmt::format("Column {} was marked as a column of"
                                             " pending nodes, but is not a NodeID"
                                             " column.", node._tag.getValue()));
        }
        bioassert(col->size() == 0);

        const std::span labels = node._labels;
        // TODO: Is this checked in planner?
        bioassert(labels.size() > 0);
        const LabelSet lblset = getLabelSet(labels);

        // Add each node that we need to the CommitWriteBuffer
        // Add labels first as they are locally contiguous in vector
        for (size_t i = 0; i < numIters; i++) {
            CommitWriteBuffer::PendingNode& newNode = _writeBuffer->newPendingNode();
            newNode.labelsetHandle = _metadataBuilder->getOrCreateLabelSet(lblset);
        }
        // TODO: Properties; once their collumn is alloc'd

        std::vector<NodeID>& raw = col->getRaw();

        // Populate the output column for this node with the index in the CWB which it
        // appears. These indexes are later transformed into "fake IDs" (an estimate as to
        // what the NodeID will be when it is committed) in @ref postProcessTempIDs.
        raw.resize(raw.size() + numIters);
        std::iota(col->begin(), col->end(), nextNodeID);
        nextNodeID += numIters;
    }
}

// @warn assumes all pending nodes have already been created
void WriteProcessor::createEdges(size_t numIters) {
    EdgeID nextEdgeID = _writeBuffer->numPendingEdges();
    const Dataframe* outDf = _output.getDataframe();

    for (const WriteProcessorTypes::PendingEdge& edge : _pendingEdges) {
        if (!outDf->hasColumn(edge._tag)) {
            throw FatalException(
                fmt::format("Could not get column in WriteProcessor "
                            "output dataframe for PendingEdge with tag {}.",
                            edge._tag.getValue()));
        }

        auto* col = outDf->getColumn(edge._tag)->as<ColumnEdgeIDs>();
        if (!col) { // @ref as<> performs dynamic_cast
            throw FatalException(fmt::format("Column {} was marked as a column of"
                                             " pending edges, but is not an EdgeID"
                                             " column.", edge._tag.getValue()));
        }
        bioassert(col->size() == 0);

        ColumnNodeIDs* srcCol {nullptr};
        const ColumnTag srcTag = edge._srcTag;
        // If the tag exists in the input, it must be an already existing NodeID
        // column - e.g. something returned by a MATCH query - otherwise it is a
        // pending node which was created in this CREATE query
        const bool srcIsPending =
            !_input || _input->getDataframe()->getColumn(srcTag) == nullptr;

        // However, all input columns are also propagated to the output, so we can
        // always retrieve the column from the output dataframe, regardless of whether
        // the source node was the result of a CREATE or a MATCH
        if (!outDf->hasColumn(srcTag)) {
            throw FatalException(fmt::format("Attempted to create edge {} with "
                                             "source node with no such column: {}",
                                             edge._name, edge._srcTag.getValue()));
        }
        srcCol = _output.getDataframe()->getColumn(srcTag)->as<ColumnNodeIDs>();
        if (!srcCol) { // @ref as<> performs dynamic_cast
            throw FatalException(fmt::format("Column {} was marked as a column of"
                                             " pending source nodes, but is not a NodeID"
                                             " column.",
                                             srcTag.getValue()));
        }

        ColumnNodeIDs* tgtCol {nullptr};
        const ColumnTag tgtTag = edge._tgtTag;
        const bool tgtIsPending =
            !_input || _input->getDataframe()->getColumn(tgtTag) == nullptr;

        if (!outDf->hasColumn(tgtTag)) {
            throw FatalException(fmt::format("Attempted to create edge {} with "
                                             "target node with no such column: {}",
                                             edge._name, edge._tgtTag.getValue()));
        }
        tgtCol = _output.getDataframe()->getColumn(tgtTag)->as<ColumnNodeIDs>();
        if (!tgtCol) { // @ref as<> performs dynamic_cast
            throw FatalException(fmt::format("Column {} was marked as a column of"
                                             " pending target nodes, but is not a NodeID"
                                             " column.",
                                             tgtTag.getValue()));
        }

        bioassert(tgtCol->size() == srcCol->size());
        bioassert(tgtCol->size() == numIters);

        for (size_t rowPtr = 0; rowPtr < numIters; rowPtr++) {
            // If src/tgt is a pending (to be created; result of CREATE clause),
            // then set it to be the corresponding index in its column. Otherwise,
            // src/tgt is already committed node (result of a previous MATCH clause), so
            // set it to be that NodeID value in the relevant column
            CommitWriteBuffer::ExistingOrPendingNode source =
                srcIsPending ? CommitWriteBuffer::ExistingOrPendingNode {
                                    std::in_place_type<CommitWriteBuffer::PendingNodeOffset>,
                                    srcCol->at(rowPtr).getValue()
                               }
                             : CommitWriteBuffer::ExistingOrPendingNode {
                                   std::in_place_type<NodeID>, srcCol->at(rowPtr)
                               };

            CommitWriteBuffer::ExistingOrPendingNode target =
                tgtIsPending ? CommitWriteBuffer::ExistingOrPendingNode {
                                 std::in_place_type<CommitWriteBuffer::PendingNodeOffset>,
                                 tgtCol->at(rowPtr).getValue()
                               }
                             : CommitWriteBuffer::ExistingOrPendingNode {
                                 std::in_place_type<NodeID>, tgtCol->at(rowPtr)
                               };

            _writeBuffer->newPendingEdge(source, target);
        }

        std::vector<EdgeID>& raw = col->getRaw();

        // Populate the output column for this edge with the index in the CWB which it
        // appears. These indexes are later transformed into "fake IDs" (an estimate as to
        // what the EdgeID will be when it is committed) in @ref postProcessFakeIDs.
        raw.resize(raw.size() + numIters);
        std::iota(col->begin(), col->end(), nextEdgeID);
        nextEdgeID += numIters;
    }
}

void WriteProcessor::postProcessTempIDs() {
    const NodeID nextNodeID = _ctxt->getGraphView().read().getTotalNodesAllocated();
    Dataframe* out = _output.getDataframe();

    for (const WriteProcessorTypes::PendingNode& node : _pendingNodes) {
        // Get the column in the output DF which will contain this node's IDs
        if (!out->hasColumn(node._tag)) {
            throw FatalException(
                fmt::format("Could not get column in WriteProcessor "
                            "output dataframe for PendingNode with tag {}.",
                            node._tag.getValue()));
        }
        auto* col = out->getColumn(node._tag)->as<ColumnNodeIDs>();
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
        // Get the column in the output DF which will contain this edge's IDs
        if (!out->hasColumn(edge._tag)) {
            throw FatalException(
                fmt::format("Could not get column in WriteProcessor "
                            "output dataframe for PendingEdge with tag {}.",
                            edge._tag.getValue()));
        }
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
    // We apply the CREATE command for each row in the input, or just a single row if we
    // have no inputs
    const size_t numIters = _input ? _input->getDataframe()->getRowCount() : 1;

    createNodes(numIters);
    createEdges(numIters);

    postProcessTempIDs();
}
