#include "ScannedNodes.h"

#include <stdint.h>

#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

using namespace db;

ScannedNodes::ScannedNodes(const GraphView& view, const CommitWriteBuffer* writeBuffer, size_t firstQueryNode)
    : _reader(view.read()),
    _writeBuffer(writeBuffer),
    _committedCount(view.isValid() ? _reader.getTotalNodesAllocated() : 0),
    _firstQueryNode(firstQueryNode),
    _pendingCount(writeBuffer ? writeBuffer->numPendingNodes() : 0)
{
}

ScannedNodes::~ScannedNodes() {
}

bool ScannedNodes::contains(NodeID node) const {
    const uint64_t id = node.getValue();
    if (id < _committedCount) {
        return !_reader.nodeIsDeleted(node);
    }

    const uint64_t offset = id - _committedCount;
    const bool createdByThisQuery = offset >= _firstQueryNode && offset < _pendingCount;

    return createdByThisQuery && !_writeBuffer->deletedPendingNodes().contains(offset);
}
