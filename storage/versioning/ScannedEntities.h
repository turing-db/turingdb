#pragma once

#include <stddef.h>

#include <concepts>

#include "datapart/EdgeRecord.h"
#include "reader/GraphReader.h"

#include "ID.h"

namespace db {

class CommitWriteBuffer;

// The nodes or the edges a scan produces: the ones the view holds and did not delete, and the
// ones the running query created and did not delete
template <TypedInternalID IDType>
class ScannedEntities {
public:
    ScannedEntities(const GraphView& view, const CommitWriteBuffer* writeBuffer, size_t firstQueryEntity);
    ~ScannedEntities();

    bool contains(IDType entity) const;

    // The source, type and target of an edge the scan produces
    void readEdge(EdgeID edge, EdgeRecord& record) const requires std::same_as<IDType, EdgeID>;

private:
    const GraphReader _reader;
    const CommitWriteBuffer* _writeBuffer {nullptr};
    size_t _committedCount {0};
    size_t _firstQueryEntity {0};
    size_t _pendingCount {0};
};

using ScannedNodes = ScannedEntities<NodeID>;
using ScannedEdges = ScannedEntities<EdgeID>;

}
