#pragma once

#include <stdint.h>
#include <stddef.h>
#include <span>
#include <vector>

#include "ListBuffer.h"
#include "ListView.h"
#include "ID.h"

namespace db {

struct PathTrieEntry {
    PathRef _parent;
    EdgeID _edge;
    NodeID _node;
    uint64_t _depth {0};
};

// The paths one query enumerates, each stored as a chain of parent pointers: a prefix shared
// by many paths is one chain of entries, and a row holds only the handle of its last entry.
// Entries live in arenas, one per walker, that grow and shrink with the walk; a handle names
// its arena in its high bits and stays valid for the chunk that emitted it.
class PathTrie {
public:
    static constexpr PathRef ROOT {0};

    PathTrie();
    ~PathTrie();

    size_t acquireArena();
    void releaseArena(size_t arena);

    PathRef append(size_t arena, PathRef parent, EdgeID edge, NodeID node, uint64_t depth);

    size_t getArenaSize(size_t arena) const { return _arenas[arena].size(); }
    void truncateArena(size_t arena, size_t size);

    // Keeps the chain alone, rewritten to the bottom of the arena with its handles updated
    // in place: chain[0] is the root, each further handle one hop deeper
    void retainChain(size_t arena, std::span<PathRef> chain);

    const PathTrieEntry& get(PathRef path) const { return _arenas[arenaOf(path)][indexOf(path)]; }
    uint64_t getDepth(PathRef path) const { return get(path)._depth; }
    size_t size() const;

    void clear();

    ListView expandEdges(PathRef path, QueryListBuffer& buffer) const;
    ListView expandEnds(PathRef path, QueryListBuffer& buffer) const;
    ListView expandSources(PathRef path, NodeID seed, QueryListBuffer& buffer) const;

    static size_t arenaOf(PathRef path) { return path.getValue() >> indexBits; }
    static size_t indexOf(PathRef path) { return path.getValue() & indexMask; }

private:
    static constexpr unsigned indexBits = 48;
    static constexpr uint64_t indexMask = (1ull << indexBits) - 1;

    std::vector<std::vector<PathTrieEntry>> _arenas;
    std::vector<size_t> _freeArenas;

    static PathRef handleOf(size_t arena, size_t index);
};

}
