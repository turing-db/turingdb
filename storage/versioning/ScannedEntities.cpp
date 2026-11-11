#include "ScannedEntities.h"

#include <stdint.h>

#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

using namespace db;

namespace {

template <TypedInternalID IDType>
size_t committedCountOf(const GraphView& view) {
    if constexpr (std::same_as<IDType, NodeID>) {
        return committedNodeCount(&view);
    } else {
        return committedEdgeCount(&view);
    }
}

template <TypedInternalID IDType>
size_t pendingCountOf(const CommitWriteBuffer* writeBuffer) {
    if (!writeBuffer) {
        return 0;
    } else if constexpr (std::same_as<IDType, NodeID>) {
        return writeBuffer->numPendingNodes();
    } else {
        return writeBuffer->numPendingEdges();
    }
}

template <TypedInternalID IDType>
const CommitWriteBuffer::DeletedPendingEntities& deletedPendingOf(const CommitWriteBuffer* writeBuffer) {
    if constexpr (std::same_as<IDType, NodeID>) {
        return writeBuffer->deletedPendingNodes();
    } else {
        return writeBuffer->deletedPendingEdges();
    }
}

}

template <TypedInternalID IDType>
ScannedEntities<IDType>::ScannedEntities(const GraphView& view, const CommitWriteBuffer* writeBuffer, size_t firstQueryEntity)
    : _reader(view.read()),
    _writeBuffer(writeBuffer),
    _committedCount(committedCountOf<IDType>(view)),
    _firstQueryEntity(firstQueryEntity),
    _pendingCount(pendingCountOf<IDType>(writeBuffer))
{
}

template <TypedInternalID IDType>
ScannedEntities<IDType>::~ScannedEntities() {
}

template <TypedInternalID IDType>
bool ScannedEntities<IDType>::contains(IDType entity) const {
    const uint64_t id = entity.getValue();
    if (id < _committedCount) {
        return !_reader.getView().isDeleted(entity);
    }

    const uint64_t offset = id - _committedCount;
    const bool createdByThisQuery = offset >= _firstQueryEntity && offset < _pendingCount;

    return createdByThisQuery && !deletedPendingOf<IDType>(_writeBuffer).contains(offset);
}

template <TypedInternalID IDType>
void ScannedEntities<IDType>::readEdge(EdgeID edge, EdgeRecord& record) const requires std::same_as<IDType, EdgeID> {
    const uint64_t id = edge.getValue();
    if (id < _committedCount) {
        record = *_reader.getEdge(edge);
        return;
    }

    const CommitWriteBuffer::PendingEdge& pending = _writeBuffer->getPendingEdge(id - _committedCount);
    const size_t firstPendingNodeID = committedNodeCount(&_reader.getView());

    record._edgeID = edge;
    record._nodeID = CommitWriteBuffer::committedIDOf(pending.src, firstPendingNodeID);
    record._otherID = CommitWriteBuffer::committedIDOf(pending.tgt, firstPendingNodeID);
    record._edgeTypeID = pending.edgeType;
}

template class db::ScannedEntities<NodeID>;
template class db::ScannedEntities<EdgeID>;
