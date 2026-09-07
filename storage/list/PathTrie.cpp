#include "PathTrie.h"

#include "ListBufferTypeTag.h"
#include "ListWriteCursor.h"

using namespace db;

PathTrie::PathTrie() {
    clear();
}

PathTrie::~PathTrie() {
}

PathRef PathTrie::append(PathRef parent, EdgeID edge, NodeID node, uint64_t depth) {
    const PathRef path(_entries.size());
    _entries.push_back({parent, edge, node, depth});

    return path;
}

void PathTrie::clear() {
    _entries.clear();
    _entries.push_back({PathRef(), EdgeID(), NodeID(), 0});
}

ListView PathTrie::expandEdges(PathRef path, QueryListBuffer& buffer) const {
    const uint64_t depth = getDepth(path);
    ListWriteCursor cursor = buffer.reserveList(depth, depth * sizeof(EdgeID));

    PathRef current = path;
    for (uint64_t index = depth; index > 0; index--) {
        const PathTrieEntry& entry = _entries[current.getValue()];
        cursor.writeValueAt(index - 1, ListBufferTypeTag::EdgeID, entry._edge);
        current = entry._parent;
    }

    return cursor.getView();
}

ListView PathTrie::expandEnds(PathRef path, QueryListBuffer& buffer) const {
    const uint64_t depth = getDepth(path);
    ListWriteCursor cursor = buffer.reserveList(depth, depth * sizeof(NodeID));

    PathRef current = path;
    for (uint64_t index = depth; index > 0; index--) {
        const PathTrieEntry& entry = _entries[current.getValue()];
        cursor.writeValueAt(index - 1, ListBufferTypeTag::NodeID, entry._node);
        current = entry._parent;
    }

    return cursor.getView();
}

ListView PathTrie::expandSources(PathRef path, NodeID seed, QueryListBuffer& buffer) const {
    const uint64_t depth = getDepth(path);
    ListWriteCursor cursor = buffer.reserveList(depth, depth * sizeof(NodeID));

    if (depth == 0) {
        return cursor.getView();
    }

    PathRef current = _entries[path.getValue()]._parent;
    for (uint64_t index = depth - 1; index > 0; index--) {
        const PathTrieEntry& entry = _entries[current.getValue()];
        cursor.writeValueAt(index, ListBufferTypeTag::NodeID, entry._node);
        current = entry._parent;
    }

    cursor.writeValueAt(0, ListBufferTypeTag::NodeID, seed);

    return cursor.getView();
}
