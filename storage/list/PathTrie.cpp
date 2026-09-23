#include "PathTrie.h"

#include "ListBufferTypeTag.h"
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

void PathTrie::appendHops(PathRef path, EntityList& entities, bool reversed) const {
    const uint64_t depth = getDepth(path);
    const size_t firstEntry = entities.size();
    entities.resize(firstEntry + depth * 2);

    PathRef current = path;
    for (uint64_t hop = depth; hop > 0; hop--) {
        const PathTrieEntry& entry = get(current);
        const size_t entryIndex = firstEntry + (reversed ? (depth - hop) : (hop - 1)) * 2;

        const EntityList::Entry edge {EntityType::Edge, EntityID(entry._edge.getValue())};
        const EntityList::Entry node {EntityType::Node, EntityID(entry._node.getValue())};

        entities[entryIndex] = reversed ? node : edge;
        entities[entryIndex + 1] = reversed ? edge : node;

        current = entry._parent;
    }
}

ListView PathTrie::expandEdges(PathRef path, QueryListBuffer& buffer, bool reversed) const {
    const uint64_t depth = getDepth(path);
    ListWriteCursor cursor = buffer.reserveList(depth, depth * sizeof(EdgeID));

    PathRef current = path;
    for (uint64_t index = depth; index > 0; index--) {
        const PathTrieEntry& entry = get(current);
        cursor.writeValueAt(reversed ? depth - index : index - 1, ListBufferTypeTag::EdgeID, entry._edge);
        current = entry._parent;
    }

    return cursor.getView();
}

ListView PathTrie::expandEnds(PathRef path, QueryListBuffer& buffer, bool reversed) const {
    const uint64_t depth = getDepth(path);
    ListWriteCursor cursor = buffer.reserveList(depth, depth * sizeof(NodeID));

    PathRef current = path;
    for (uint64_t index = depth; index > 0; index--) {
        const PathTrieEntry& entry = get(current);
        cursor.writeValueAt(reversed ? depth - index : index - 1, ListBufferTypeTag::NodeID, entry._node);
        current = entry._parent;
    }

    return cursor.getView();
}

ListView PathTrie::expandSources(PathRef path, NodeID seed, QueryListBuffer& buffer, bool reversed) const {
    const uint64_t depth = getDepth(path);
    ListWriteCursor cursor = buffer.reserveList(depth, depth * sizeof(NodeID));

    if (depth == 0) {
        return cursor.getView();
    }

    PathRef current = get(path)._parent;
    for (uint64_t index = depth - 1; index > 0; index--) {
        const PathTrieEntry& entry = get(current);
        cursor.writeValueAt(reversed ? depth - 1 - index : index, ListBufferTypeTag::NodeID, entry._node);
        current = entry._parent;
    }

    cursor.writeValueAt(reversed ? depth - 1 : 0, ListBufferTypeTag::NodeID, seed);

    return cursor.getView();
}
