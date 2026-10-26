#include "PathTrie.h"

#include "CellTag.h"
#include "ListWriteCursor.h"

#include "BioAssert.h"

using namespace db;

PathTrie::PathTrie() {
    clear();
}

PathTrie::~PathTrie() {
}

size_t PathTrie::acquireArena() {
    if (!_freeArenas.empty()) {
        const size_t arena = _freeArenas.back();
        _freeArenas.pop_back();
        return arena;
    }

    bioassert(_arenas.size() < (1ull << (64 - indexBits)), "Too many path arenas open at once");
    _arenas.emplace_back();

    return _arenas.size() - 1;
}

void PathTrie::releaseArena(size_t arena) {
    bioassert(arena != 0 && arena < _arenas.size(), "Releasing a path arena that was never acquired");
    _arenas[arena].clear();
    _freeArenas.push_back(arena);
}

PathRef PathTrie::append(size_t arena, PathRef parent, EdgeID edge, NodeID node, uint64_t depth) {
    std::vector<PathTrieEntry>& entries = _arenas[arena];
    const PathRef path = handleOf(arena, entries.size());
    entries.push_back({parent, edge, node, depth});

    return path;
}

void PathTrie::truncateArena(size_t arena, size_t size) {
    _arenas[arena].resize(size);
}

void PathTrie::retainChain(size_t arena, std::span<PathRef> chain) {
    std::vector<PathTrieEntry>& entries = _arenas[arena];
    const size_t hops = chain.empty() ? 0 : chain.size() - 1;

    for (size_t hop = 0; hop < hops; hop++) {
        const PathRef held = chain[hop + 1];
        bioassert(arenaOf(held) == arena, "A chain is retained in the arena that holds it");

        const PathTrieEntry entry = entries[indexOf(held)];
        const PathRef parent = hop == 0 ? ROOT : chain[hop];
        entries[hop] = {parent, entry._edge, entry._node, entry._depth};
        chain[hop + 1] = handleOf(arena, hop);
    }

    entries.resize(hops);
}

size_t PathTrie::size() const {
    size_t count = 0;
    for (const std::vector<PathTrieEntry>& entries : _arenas) {
        count += entries.size();
    }

    return count;
}

void PathTrie::clear() {
    _arenas.clear();
    _freeArenas.clear();

    std::vector<PathTrieEntry>& rootArena = _arenas.emplace_back();
    rootArena.push_back({PathRef(), EdgeID(), NodeID(), 0});
}

PathRef PathTrie::handleOf(size_t arena, size_t index) {
    return PathRef((static_cast<uint64_t>(arena) << indexBits) | index);
}

void PathTrie::appendHops(PathRef path, GraphPath& graphPath) const {
    bioassert(!graphPath.isNull(), "A walk taken with the pattern continues a path that opened on a node");

    const uint64_t depth = getDepth(path);
    const size_t firstHop = graphPath.getLength();
    graphPath.setNodeCount(firstHop + depth + 1);

    PathRef current = path;
    for (uint64_t hop = depth; hop > 0; hop--) {
        const PathTrieEntry& entry = get(current);

        graphPath.setNode(firstHop + hop, entry._node);
        graphPath.setEdge(firstHop + hop - 1, entry._edge);

        current = entry._parent;
    }
}

void PathTrie::appendReversedHops(PathRef path, NodeID seed, GraphPath& graphPath) const {
    const uint64_t depth = getDepth(path);
    const size_t firstHop = graphPath.getLength();
    graphPath.setNodeCount(firstHop + depth + 1);

    PathRef current = path;
    for (uint64_t step = 0; step < depth; step++) {
        const PathTrieEntry& entry = get(current);

        graphPath.setNode(firstHop + step, entry._node);
        graphPath.setEdge(firstHop + step, entry._edge);

        current = entry._parent;
    }

    graphPath.setNode(firstHop + depth, seed);
}

ListView PathTrie::expandEdges(PathRef path, QueryListBuffer& buffer, bool reversed, PathHopStride hops) const {
    const uint64_t depth = getDepth(path);
    bioassert(depth % hops._stride == 0, "A path ends after a whole number of repetitions");
    const uint64_t count = depth / hops._stride;
    ListWriteCursor cursor = buffer.reserveList(count, count * sizeof(EdgeID));

    PathRef current = path;
    for (uint64_t hop = depth; hop > 0; hop--) {
        const PathTrieEntry& entry = get(current);
        const uint64_t taken = hop - 1;

        if (taken % hops._stride == hops._offset) {
            const uint64_t index = taken / hops._stride;
            cursor.writeValueAt(reversed ? count - 1 - index : index, CellTag::EdgeID, entry._edge);
        }

        current = entry._parent;
    }

    return cursor.getView();
}

ListView PathTrie::expandEnds(PathRef path, QueryListBuffer& buffer, bool reversed, PathHopStride hops) const {
    const uint64_t depth = getDepth(path);
    bioassert(depth % hops._stride == 0, "A path ends after a whole number of repetitions");
    const uint64_t count = depth / hops._stride;
    ListWriteCursor cursor = buffer.reserveList(count, count * sizeof(NodeID));

    PathRef current = path;
    for (uint64_t hop = depth; hop > 0; hop--) {
        const PathTrieEntry& entry = get(current);
        const uint64_t taken = hop - 1;

        if (taken % hops._stride == hops._offset) {
            const uint64_t index = taken / hops._stride;
            cursor.writeValueAt(reversed ? count - 1 - index : index, CellTag::NodeID, entry._node);
        }

        current = entry._parent;
    }

    return cursor.getView();
}

ListView PathTrie::expandNodes(PathRef path, NodeID seed, QueryListBuffer& buffer, bool reversed) const {
    const uint64_t depth = getDepth(path);
    const uint64_t count = depth + 1;
    ListWriteCursor cursor = buffer.reserveList(count, count * sizeof(NodeID));

    PathRef current = path;
    for (uint64_t index = depth; index > 0; index--) {
        const PathTrieEntry& entry = get(current);
        cursor.writeValueAt(reversed ? depth - index : index, CellTag::NodeID, entry._node);
        current = entry._parent;
    }

    cursor.writeValueAt(reversed ? depth : 0, CellTag::NodeID, seed);

    return cursor.getView();
}

ListView PathTrie::expandSources(PathRef path, NodeID seed, QueryListBuffer& buffer, bool reversed, PathHopStride hops) const {
    const uint64_t depth = getDepth(path);
    bioassert(depth % hops._stride == 0, "A path ends after a whole number of repetitions");
    const uint64_t count = depth / hops._stride;
    ListWriteCursor cursor = buffer.reserveList(count, count * sizeof(NodeID));

    if (count == 0) {
        return cursor.getView();
    }

    PathRef current = get(path)._parent;
    for (uint64_t hop = depth - 1; hop > 0; hop--) {
        const PathTrieEntry& entry = get(current);

        if (hop % hops._stride == hops._offset) {
            const uint64_t index = hop / hops._stride;
            cursor.writeValueAt(reversed ? count - 1 - index : index, CellTag::NodeID, entry._node);
        }

        current = entry._parent;
    }

    if (hops._offset == 0) {
        cursor.writeValueAt(reversed ? count - 1 : 0, CellTag::NodeID, seed);
    }

    return cursor.getView();
}
