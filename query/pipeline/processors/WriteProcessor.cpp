#include "WriteProcessor.h"

#include <algorithm>
#include <optional>
#include <string_view>
#include <utility>

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"

#include "columns/ColumnConst.h"
#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
#include "processors/ExprProgram.h"

#include "columns/ColumnIDs.h"
#include "dataframe/NamedColumn.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "ID.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "dataframe/Dataframe.h"
#include "WriteProcessorTypes.h"
#include "writers/MetadataBuilder.h"

#include "FatalException.h"
#include "PipelineException.h"

using namespace db;

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

CommitWriteBuffer::UntypedProperty getConstPropertyValue(Column* valueCol,
                                                         ValueType type,
                                                         PropertyTypeID propID) {
    switch (type) {
        case ValueType::Int64: {
            const auto* casted = dynamic_cast<ColumnConst<types::Int64::Primitive>*>(valueCol);
            bioassert(casted, "Could not get constant property value.");
            return {propID, casted->getRaw()};
        }
        break;

        case ValueType::UInt64: {
            const auto* casted = dynamic_cast<ColumnConst<types::UInt64::Primitive>*>(valueCol);
            bioassert(casted, "Could not get constant property value.");
            return {propID, casted->getRaw()};
        }
        break;

        case ValueType::String: {
            const auto* casted = dynamic_cast<ColumnConst<types::String::Primitive>*>(valueCol);
            bioassert(casted, "Could not get constant property value.");
            return {propID, std::string(casted->getRaw())};

        }
        break;

        case ValueType::Double: {
            const auto* casted = dynamic_cast<ColumnConst<types::Double::Primitive>*>(valueCol);
            bioassert(casted, "Could not get constant property value.");
            return {propID, casted->getRaw()};
        }
        break;

        case ValueType::Bool: {
            const auto* casted = dynamic_cast<ColumnConst<types::Bool::Primitive>*>(valueCol);
            bioassert(casted, "Could not get constant property value.");
            return {propID, casted->getRaw()};
        }
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE: {
            throw FatalException("Property value column with invalid type.");
        }
        break;
    }
    throw FatalException("Failed to match against property value type.");
}

}

WriteProcessor::WriteProcessor(ExprProgram* exprProg)
    : _exprProgram(exprProg)
{
}

WriteProcessor* WriteProcessor::create(PipelineV2* pipeline, ExprProgram* exprProg, bool hasInput) {
    auto* processor = new WriteProcessor(exprProg);

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

    bioassert(commitBuilder, "Failed to get CommitBuilder in WriteProcessor");

    _metadataBuilder = &commitBuilder->metadata();
    _writeBuffer = &commitBuilder->writeBuffer();

    bioassert(_metadataBuilder, "Failed to get MetadataBuilder in WriteProcessor");
    bioassert(_writeBuffer, "Failed to get CommitWriteBuffer in WriteProcessor");
}

void WriteProcessor::reset() {
    markAsReset();
}

void WriteProcessor::performDeletions() {
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

    LabelSet lblset;
    for (const WriteProcessorTypes::PendingNode& node : _pendingNodes) {
        // This is checked for validity in the call to @ref setup
        auto* createdNodeCol = outDf->getColumn(node._tag)->as<ColumnNodeIDs>();

        // Always empty on first call, or has been cleared by @ref setup.
        bioassert(createdNodeCol->size() == 0,
                  "Pending nodes column should be empty but is size {}",
                  createdNodeCol->size());

        const std::span labels = node._labels;

        if (labels.empty()) {
            throw PipelineException("Nodes require at least 1 label.");
        }
        lblset = getLabelSet(labels);

        // Get all properties: this can throw, so we do this BEFORE adding PendingNodes to
        // CommitWriteBuffer, as otherwise after throwing this could leave it in invalid
        // state.
        std::vector<CommitWriteBuffer::UntypedProperty> constProps;
        constProps.reserve(node._properties.size());
        for (const auto& [name, type, valueCol] : node._properties) {
            const PropertyTypeID propID =
                _metadataBuilder->getOrCreatePropertyType(name, type)._id;

            constProps.emplace_back(getConstPropertyValue(valueCol, type, propID));
        }

        {
            // Create nodes, set label set
            const size_t numPendingNodesPrior = _writeBuffer->numPendingNodes();
            const LabelSetHandle hdl = _metadataBuilder->getOrCreateLabelSet(lblset);
            for (size_t row = 0; row < numIters; row++) {
                CommitWriteBuffer::PendingNode& newNode = _writeBuffer->newPendingNode();
                newNode.labelsetHandle = hdl;
            }

            // Add each property; NOTE: for now all ColumnConst, so all same value.
            // Extract the value first, then add that value to each node to create.
            // TODO: More cache friendly access patterns
            for (const CommitWriteBuffer::UntypedProperty& prop : constProps) {
                for (size_t i = 0; i < numIters; i++) {
                    auto& pendingNode = _writeBuffer->getPendingNode(numPendingNodesPrior + i);
                    pendingNode.properties.emplace_back(prop);
                }
            }
        }

        // Populate the output column for this node with the index in the CWB which it
        // appears. These indexes are later transformed into "fake IDs" (an estimate as to
        // what the NodeID will be when it is committed) in @ref postProcessTempIDs.

        std::vector<NodeID>& raw = createdNodeCol->getRaw();
        const size_t oldSize = raw.size();
        raw.resize(oldSize + numIters);
        std::iota(raw.begin() + oldSize, raw.end(), nextNodeID);
        _writtenRowsThisCycle |= raw.size() > oldSize;

        nextNodeID += numIters;
    }
}

// @warn assumes all pending nodes have already been created
void WriteProcessor::createEdges(size_t numIters) {
    EdgeID nextEdgeID = _writeBuffer->numPendingEdges();
    const Dataframe* inDf = _input ? _input->getDataframe() : nullptr;
    const Dataframe* outDf = _output.getDataframe();

    for (const WriteProcessorTypes::PendingEdge& edge : _pendingEdges) {
        // This is checked for validity in the call to @ref setup
        auto* createdEdgeColumn = outDf->getColumn(edge._tag)->as<ColumnEdgeIDs>();

        // Always empty on first call, or has been cleared by @ref setup.
        bioassert(createdEdgeColumn->size() == 0,
                  "Pending edges column should be empty but is size {}",
                  createdEdgeColumn->size());

        ColumnNodeIDs* srcCol = nullptr;
        const ColumnTag srcTag = edge._srcTag;
        // If the tag exists in the input, it must be an already existing NodeID
        // column - e.g. something returned by a MATCH query - otherwise it is a
        // pending node which was created in this CREATE query
        const bool srcIsPending = !inDf || inDf->getColumn(srcTag) == nullptr;

        // However, all input columns are also propagated to the output, so we can
        // always retrieve the column from the output dataframe, regardless of whether
        // the source node was the result of a CREATE or a MATCH
        if (!outDf->hasColumn(srcTag)) {
            throw FatalException(fmt::format("Attempted to create edge {} with "
                                             "source node with no such column: {}",
                                             edge._name, edge._srcTag.getValue()));
        }
        srcCol = outDf->getColumn(srcTag)->as<ColumnNodeIDs>();
        if (!srcCol) { // @ref as<> performs dynamic_cast
            throw FatalException(fmt::format("Column {} was marked as a column of"
                                             " pending source nodes, but is not a NodeID"
                                             " column.",
                                             srcTag.getValue()));
        }

        ColumnNodeIDs* tgtCol = nullptr;
        const ColumnTag tgtTag = edge._tgtTag;
        const bool tgtIsPending = !inDf || inDf->getColumn(tgtTag) == nullptr;

        if (!outDf->hasColumn(tgtTag)) {
            throw FatalException(fmt::format("Attempted to create edge {} with "
                                             "target node with no such column: {}",
                                             edge._name, edge._tgtTag.getValue()));
        }
        tgtCol = outDf->getColumn(tgtTag)->as<ColumnNodeIDs>();
        if (!tgtCol) { // @ref as<> performs dynamic_cast
            throw FatalException(fmt::format("Column {} was marked as a column of"
                                             " pending target nodes, but is not a NodeID"
                                             " column.",
                                             tgtTag.getValue()));
        }

        bioassert(tgtCol->size() == srcCol->size(), "src and target column should have same dimension");
        bioassert(tgtCol->size() == numIters, "invalid size of target column");

        // Get all properties: this can throw, so we do this BEFORE adding PendingEdges to
        // CommitWriteBuffer, as otherwise after throwing this could leave it in invalid
        // state.
        std::vector<CommitWriteBuffer::UntypedProperty> constProps;
        constProps.reserve(edge._properties.size());
        for (const auto& [name, type, valueCol] : edge._properties) {
            const PropertyTypeID propID =
                _metadataBuilder->getOrCreatePropertyType(name, type)._id;

            constProps.emplace_back(getConstPropertyValue(valueCol, type, propID));
        }

        const size_t numPendingEdgesPrior = _writeBuffer->numPendingEdges();
        const EdgeTypeID typeID = _metadataBuilder->getOrCreateEdgeType(edge._edgeType);
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

            auto& pendingEdge = _writeBuffer->newPendingEdge(source, target);
            pendingEdge.edgeType = typeID;
        }

        // Add each property; NOTE: for now all ColumnConst, so all same value.
        // Extract the value first, then add that value to each node to create.
        // TODO: More cache friendly access patterns
        for (const CommitWriteBuffer::UntypedProperty& prop : constProps) {
            for (size_t i = 0; i < numIters; i++) {
                auto& pendingEdge = _writeBuffer->getPendingEdge(numPendingEdgesPrior + i);
                pendingEdge.properties.emplace_back(prop);
            }
        }


        // Populate the output column for this edge with the index in the CWB which it
        // appears. These indexes are later transformed into "fake IDs" (an estimate as to
        // what the EdgeID will be when it is committed) in @ref postProcessFakeIDs.

        std::vector<EdgeID>& raw = createdEdgeColumn->getRaw();
        const size_t oldSize = raw.size();
        raw.resize(oldSize + numIters);
        std::iota(raw.begin() + oldSize, raw.end(), nextEdgeID);
        _writtenRowsThisCycle |= raw.size() > oldSize;

        nextEdgeID += numIters;
    }
}

void WriteProcessor::postProcessTempIDs() {
    const NodeID nextNodeID = _ctxt->getGraphView().read().getTotalNodesAllocated();
    Dataframe* out = _output.getDataframe();

    for (const WriteProcessorTypes::PendingNode& node : _pendingNodes) {
        // Get the column in the output DF which contains this edge's IDs
        // This is checked for validity in the call to @ref setup
        auto* col = out->getColumn(node._tag)->as<ColumnNodeIDs>();

        std::vector<NodeID>& raw = col->getRaw();
        std::ranges::for_each(raw, [nextNodeID] (NodeID& fakeID) { fakeID += nextNodeID; });
    }

    const EdgeID nextEdgeID = _ctxt->getGraphView().read().getTotalEdgesAllocated();

    for (const WriteProcessorTypes::PendingEdge& edge : _pendingEdges) {
        // Get the column in the output DF which contains this edge's IDs
        // This is checked for validity in the call to @ref setup
        auto* col = out->getColumn(edge._tag)->as<ColumnEdgeIDs>();

        std::vector<EdgeID>& raw = col->getRaw();
        std::ranges::for_each(raw, [nextEdgeID](EdgeID& fakeID) { fakeID += nextEdgeID; });
    }
}

void WriteProcessor::performCreations() {
    // We apply the CREATE command for each row in the input, or just a single row if we
    // have no inputs
    const size_t numIters = _input ? _input->getDataframe()->getRowCount() : 1;
    if (numIters == 0) {
        return;
    }

    createNodes(numIters);
    createEdges(numIters);

    postProcessTempIDs();
}

void WriteProcessor::setup() {
    _writtenRowsThisCycle = false;

    // Check the output dataframe that all the pending node/edge columns are present
    const Dataframe* outDf = _output.getDataframe();

    for (const auto& edge : _pendingEdges) {
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
        // Clear each column. They are non-empty in the case that this processor has been
        // called previously, with a previous chunk input
        col->clear();
    }

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
        // Clear each column. They are non-empty in the case that this processor has been
        // called previously, with a previous chunk input
        col->clear();
    }
}

void WriteProcessor::execute() {
    setup();

    // NOTE: We currently do not have `CREATE (n) DELETE n` supported in PlanGraph,
    // meaning if we are deleting, we require an input. This may change.
    if (!_deletedNodes.empty() || !_deletedEdges.empty()) {
        if (!_input) {
            throw PipelineException("Cannot delete pending entity: DELETE queries require a MATCH input.");
        }
        performDeletions();
    }

    if (!_pendingNodes.empty() || !_pendingEdges.empty()) {
        // Evaluate instructions so that property columns are populated with their
        // evaluated value
        _exprProgram->evaluateInstructions();
        performCreations();
    }

    if (_input) {
        _input->getPort()->consume();
    }

    // Set our output port as containing data if:
    // 1. We wrote rows to the output.
    // 2. We had an input, but wrote no rows, but that input is now closed.
    // This emits an empty chunk iff our input was empty and is now closed.
    const bool inputClosed = _input && _input->getPort()->isClosed();
    if (_writtenRowsThisCycle || inputClosed) {
        _output.getPort()->writeData();
    }

    // Since we perform at most CHUNK_SIZE iterations, we write at most CHUNK_SIZE rows.
    // Hence we always finish in a single cycle, and always call @ref finish.
    finish();
}
