#pragma once

#include <stdint.h>
#include <stddef.h>
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
class PathTrie {
public:
    static constexpr PathRef ROOT {0};

    PathTrie();
    ~PathTrie();

    PathRef append(PathRef parent, EdgeID edge, NodeID node, uint64_t depth);

    const PathTrieEntry& get(PathRef path) const { return _entries[path.getValue()]; }
    uint64_t getDepth(PathRef path) const { return _entries[path.getValue()]._depth; }
    size_t size() const { return _entries.size(); }

    void clear();

    ListView expandEdges(PathRef path, QueryListBuffer& buffer) const;
    ListView expandEnds(PathRef path, QueryListBuffer& buffer) const;
    ListView expandSources(PathRef path, NodeID seed, QueryListBuffer& buffer) const;

private:
    std::vector<PathTrieEntry> _entries;
};

}
