#pragma once

#include <stddef.h>

#include "reader/GraphReader.h"

#include "ID.h"

namespace db {

class CommitWriteBuffer;

// The nodes a node scan produces: the ones the view holds and did not delete, and the ones
// the running query created and did not delete
class ScannedNodes {
public:
    ScannedNodes(const GraphView& view, const CommitWriteBuffer* writeBuffer, size_t firstQueryNode);
    ~ScannedNodes();

    bool contains(NodeID node) const;

private:
    const GraphReader _reader;
    const CommitWriteBuffer* _writeBuffer {nullptr};
    size_t _committedCount {0};
    size_t _firstQueryNode {0};
    size_t _pendingCount {0};
};

}
