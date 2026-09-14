#include "NLPendingEdges.h"

#include <variant>

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

std::span<const size_t> NLPendingEdgeIndex::outOf(NodeID node) const {
    return lookup(_outgoing, node);
}

std::span<const size_t> NLPendingEdgeIndex::into(NodeID node) const {
    return lookup(_incoming, node);
}

std::span<const size_t> NLPendingEdgeIndex::lookup(const Edges& edges, NodeID node) {
    const auto findIt = edges.find(node.getValue());
    if (findIt == end(edges)) {
        return {};
    }

    return findIt->second;
}

NLPendingEdges::NLPendingEdges(NLExecutionContext* context,
                               NLEdgeLoopData* loopData,
                               Direction direction,
                               ColumnNodeIDs* others)
    : _writeBuffer(context->getWriteBuffer()),
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

    _firstPendingNodeID = committedNodeCount(context->getView());
    _firstPendingEdgeID = committedEdgeCount(context->getView());

    NLPendingEdgeIndex& index = context->getPendingEdges();
    index.indexEdges(_writeBuffer, _firstPendingNodeID);
    _index = &index;

    beginRun();
}

NLPendingEdges::~NLPendingEdges() {
}

void NLPendingEdges::fill(size_t maxCount) {
    clearChunks();

    const size_t rowCount = _inputNodeIDs->size();
    size_t remainingToMax = maxCount;

    while (remainingToMax > 0 && _row < rowCount) {
        const bool runDrained = _position == _offsets.size();

        // A run's offsets ascend, so the first one the hop did not start with ends it: the
        // rest of the run is what the hop's own body has written since.
        const bool runIsNewerThanTheHop = !runDrained && _offsets[_position] >= _pendingEdgeCount;

        if (runDrained || runIsNewerThanTheHop) {
            nextRun();
            continue;
        }

        const size_t offset = _offsets[_position];
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

bool NLPendingEdges::walksIn() const {
    return _direction == Direction::In || (_direction == Direction::Either && _incoming);
}

void NLPendingEdges::clearChunks() {
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

void NLPendingEdges::beginRun() {
    _position = 0;

    if (_row >= _inputNodeIDs->size()) {
        _offsets = {};
        return;
    }

    const NodeID node = (*_inputNodeIDs)[_row];
    _offsets = walksIn() ? _index->into(node) : _index->outOf(node);
}

void NLPendingEdges::nextRun() {
    const bool walksEitherWay = _direction == Direction::Either;

    if (walksEitherWay && !_incoming) {
        _incoming = true;
    } else {
        _incoming = false;
        _row++;
    }

    beginRun();
}

bool NLPendingEdges::walks(size_t offset, NodeID& other) const {
    if (_writeBuffer->deletedPendingEdges().contains(offset)) {
        return false;
    }

    const CommitWriteBuffer::PendingEdge& edge = _writeBuffer->getPendingEdge(offset);
    if (_edgeType && edge.edgeType != *_edgeType) {
        return false;
    }

    other = walksIn() ? nodeIDOf(edge.src, _firstPendingNodeID) : nodeIDOf(edge.tgt, _firstPendingNodeID);

    return true;
}
