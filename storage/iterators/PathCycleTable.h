#pragma once

#include <stdint.h>
#include <stddef.h>
#include <vector>

#include "ID.h"

namespace db {

// The nodes one batch of the cycle search has reached, each with the same number of words. The
// words are stored densely in the order the nodes were reached, so a batch costs its ball and
// not the graph.
class PathCycleTable {
public:
    PathCycleTable();
    ~PathCycleTable();

    // Forgets every node reached and gives each node reached from now on that many words
    void reset(size_t wordCount);

    // The node's words, zeroed the first time it is reached; valid until the next reach
    uint64_t* reach(NodeID node);

    // The words of a node reached before, or null
    uint64_t* find(NodeID node);

    size_t size() const { return _nodes.size(); }

private:
    static constexpr uint32_t emptySlot = UINT32_MAX;
    static constexpr size_t initialCapacity = 128;

    std::vector<uint32_t> _slots;
    std::vector<size_t> _occupied;
    std::vector<uint64_t> _nodes;
    std::vector<uint64_t> _words;
    size_t _wordCount {0};
    uint64_t _mask {0};

    static uint64_t hashOf(uint64_t key) { return (key * 0x9E3779B97F4A7C15ull) >> 20; }

    size_t slotOf(uint64_t key) const;
    void grow();
};

}
