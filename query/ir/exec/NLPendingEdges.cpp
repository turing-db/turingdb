#include "NLPendingEdges.h"

#include <variant>

#include "iterators/EdgeTypeMatch.h"
#include "reader/GraphReader.h"

#include "NLExecutionContext.h"
#include "NLProgram.h"
#include "NLWriteProperties.h"

using namespace db;

namespace {

// The ID a pending edge names its endpoint by: one the graph holds, or the one a node this
// change wrote will commit as
NodeID nodeIDOf(const CommitWriteBuffer::ExistingOrPendingNode& node, size_t firstPendingNodeID) {
    if (std::holds_alternative<NodeID>(node)) {
        return std::get<NodeID>(node);
    }

    return NodeID(firstPendingNodeID + std::get<CommitWriteBuffer::PendingNodeOffset>(node));
}

// Whether @param node carries at least @param labels. A node this change wrote carries its
// labels in the buffer; one the graph already holds is read off the graph, and a node
// neither holds carries no label to match.
bool carriesLabels(const CommitWriteBuffer* writeBuffer,
                   const GraphView& view,
                   const CommitWriteBuffer::ExistingOrPendingNode& node,
                   const LabelSetHandle& labels) {
    if (std::holds_alternative<CommitWriteBuffer::PendingNodeOffset>(node)) {
        const CommitWriteBuffer::PendingNodeOffset offset = std::get<CommitWriteBuffer::PendingNodeOffset>(node);
        const CommitWriteBuffer::PendingNode& pending = writeBuffer->getPendingNode(offset);

        return pending.labelsetHandle.hasAtLeastLabels(labels);
    }

    const GraphReader reader(view);
    const LabelSetHandle nodeLabels = reader.getNodeLabelSet(std::get<NodeID>(node));

    return nodeLabels.isValid() && nodeLabels.hasAtLeastLabels(labels);
}

}

NLPendingEdgeIndex::NLPendingEdgeIndex() {
}

NLPendingEdgeIndex::~NLPendingEdgeIndex() {
}

void NLPendingEdgeIndex::indexEdges(const CommitWriteBuffer* writeBuffer, size_t firstPendingNodeID) {
    const CommitWriteBuffer::PendingEdges& edges = writeBuffer->pendingEdges();

    for (; _indexedEdges < edges.size(); _indexedEdges++) {
        const CommitWriteBuffer::PendingEdge& edge = edges[_indexedEdges];

        _outgoing[nodeIDOf(edge.src, firstPendingNodeID).getValue()].push_back(_indexedEdges);
        _incoming[nodeIDOf(edge.tgt, firstPendingNodeID).getValue()].push_back(_indexedEdges);
    }
}

const NLPendingEdgeIndex::Offsets* NLPendingEdgeIndex::outOf(NodeID node) const {
    return lookup(_outgoing, node);
}

const NLPendingEdgeIndex::Offsets* NLPendingEdgeIndex::into(NodeID node) const {
    return lookup(_incoming, node);
}

const NLPendingEdgeIndex::Offsets* NLPendingEdgeIndex::lookup(const Edges& edges, NodeID node) {
    const auto findIt = edges.find(node.getValue());
    if (findIt == end(edges)) {
        return nullptr;
    }

    return &findIt->second;
}

NLPendingEdgeHop::NLPendingEdgeHop(NLExecutionContext* context,
                                   NLEdgeLoopData* loopData,
                                   Direction direction,
                                   ColumnNodeIDs* others)
    : _writeBuffer(context->getWriteBuffer()),
    _view(context->getView()),
    _inputNodeIDs(loopData->getInput()),
    _indices(loopData->getIndices()),
    _edgeIDs(loopData->getEdgeIDs()),
    _types(loopData->getEdgeTypes()),
    _others(others),
    _pendingEdgeCount(_writeBuffer ? _writeBuffer->numPendingEdges() : 0),
    _direction(direction)
{
    if (_pendingEdgeCount == 0) {
        _row = _inputNodeIDs->size();
        return;
    }

    _firstQueryEdge = context->getFirstQueryEdge();
    _firstPendingNodeID = committedNodeCount(context->getView());
    _firstPendingEdgeID = committedEdgeCount(context->getView());

    NLPendingEdgeIndex& index = context->getPendingEdges();
    index.indexEdges(_writeBuffer, _firstPendingNodeID);
    _index = &index;

    beginRun();
}

NLPendingEdgeHop::~NLPendingEdgeHop() {
}

void NLPendingEdgeHop::setEndpointLabelSet(const LabelSet& labelset) {
    _endpointLabels = LabelSetHandle(labelset);
}

void NLPendingEdgeHop::fill(size_t maxCount) {
    clearChunks();

    const size_t rowCount = _inputNodeIDs->size();
    size_t remainingToMax = maxCount;

    while (remainingToMax > 0 && _row < rowCount) {
        const bool runDrained = !_offsets || _position == _offsets->size();

        // A run's offsets ascend, so the first one the hop did not start with ends it: the
        // rest of the run is what the hop's own body has written since.
        const bool runIsNewerThanTheHop = !runDrained && (*_offsets)[_position] >= _pendingEdgeCount;

        if (runDrained || runIsNewerThanTheHop) {
            nextRun();
            continue;
        }

        const size_t offset = (*_offsets)[_position];
        _position++;

        NodeID other;
        if (!walks(offset, other)) {
            continue;
        }

        _indices->push_back(_row);

        if (_edgeIDs) {
            _edgeIDs->push_back(EdgeID(_firstPendingEdgeID + offset));
        }
        if (_others) {
            _others->push_back(other);
        }
        if (_types) {
            _types->push_back(_writeBuffer->getPendingEdge(offset).edgeType);
        }

        remainingToMax--;
    }
}

bool NLPendingEdgeHop::walksIn() const {
    return _direction == Direction::In || (_direction == Direction::Either && _incoming);
}

void NLPendingEdgeHop::clearChunks() {
    _indices->clear();

    if (_edgeIDs) {
        _edgeIDs->clear();
    }
    if (_others) {
        _others->clear();
    }
    if (_types) {
        _types->clear();
    }
}

void NLPendingEdgeHop::beginRun() {
    _position = 0;

    if (_row >= _inputNodeIDs->size()) {
        _offsets = nullptr;
        return;
    }

    const NodeID node = (*_inputNodeIDs)[_row];
    _offsets = walksIn() ? _index->into(node) : _index->outOf(node);
}

void NLPendingEdgeHop::nextRun() {
    const bool walksEitherWay = _direction == Direction::Either;

    if (walksEitherWay && !_incoming) {
        _incoming = true;
    } else {
        _incoming = false;
        _row++;
    }

    beginRun();
}

bool NLPendingEdgeHop::walks(size_t offset, NodeID& other) const {
    if (offset < _firstQueryEdge || _writeBuffer->deletedPendingEdges().contains(offset)) {
        return false;
    }

    const CommitWriteBuffer::PendingEdge& edge = _writeBuffer->getPendingEdge(offset);
    if (_edgeTypes && !edgeTypeMatches(*_edgeTypes, edge.edgeType)) {
        return false;
    }

    const CommitWriteBuffer::ExistingOrPendingNode& otherEnd = walksIn() ? edge.src : edge.tgt;
    if (_endpointLabels.isValid() && !carriesLabels(_writeBuffer, *_view, otherEnd, _endpointLabels)) {
        return false;
    }

    other = nodeIDOf(otherEnd, _firstPendingNodeID);

    return true;
}

NLPendingEdgeScan::NLPendingEdgeScan(NLExecutionContext* context, NLScanEdgesLoopData* loopData)
    : _writeBuffer(context->getWriteBuffer()),
    _view(context->getView()),
    _srcs(loopData->getSources()),
    _edgeIDs(loopData->getEdgeIDs()),
    _types(loopData->getEdgeTypes()),
    _tgts(loopData->getTargets()),
    _pendingEdgeCount(_writeBuffer ? _writeBuffer->numPendingEdges() : 0)
{
    _edge = context->getFirstQueryEdge();

    if (_pendingEdgeCount == 0) {
        return;
    }

    _firstPendingNodeID = committedNodeCount(context->getView());
    _firstPendingEdgeID = committedEdgeCount(context->getView());
}

NLPendingEdgeScan::~NLPendingEdgeScan() {
}

void NLPendingEdgeScan::setSourceLabelSet(const LabelSet& labelset) {
    _endpointLabels = LabelSetHandle(labelset);
    _labelsTheTarget = false;
}

void NLPendingEdgeScan::setTargetLabelSet(const LabelSet& labelset) {
    _endpointLabels = LabelSetHandle(labelset);
    _labelsTheTarget = true;
}

void NLPendingEdgeScan::fill(size_t maxCount) {
    clearChunks();

    size_t remainingToMax = maxCount;

    while (remainingToMax > 0 && _edge < _pendingEdgeCount) {
        const size_t offset = _edge;
        _edge++;

        if (_writeBuffer->deletedPendingEdges().contains(offset)) {
            continue;
        }

        const CommitWriteBuffer::PendingEdge& edge = _writeBuffer->getPendingEdge(offset);
        if (!keeps(edge)) {
            continue;
        }

        if (_srcs) {
            _srcs->push_back(nodeIDOf(edge.src, _firstPendingNodeID));
        }
        if (_edgeIDs) {
            _edgeIDs->push_back(EdgeID(_firstPendingEdgeID + offset));
        }
        if (_types) {
            _types->push_back(edge.edgeType);
        }
        if (_tgts) {
            _tgts->push_back(nodeIDOf(edge.tgt, _firstPendingNodeID));
        }

        remainingToMax--;
    }
}

bool NLPendingEdgeScan::keeps(const CommitWriteBuffer::PendingEdge& edge) const {
    if (_edgeTypes && !edgeTypeMatches(*_edgeTypes, edge.edgeType)) {
        return false;
    }

    if (!_endpointLabels.isValid()) {
        return true;
    }

    return carriesLabels(_writeBuffer, *_view, _labelsTheTarget ? edge.tgt : edge.src, _endpointLabels);
}

void NLPendingEdgeScan::clearChunks() {
    if (_srcs) {
        _srcs->clear();
    }
    if (_edgeIDs) {
        _edgeIDs->clear();
    }
    if (_types) {
        _types->clear();
    }
    if (_tgts) {
        _tgts->clear();
    }
}
