#pragma once

#include <stdint.h>
#include <stddef.h>
#include <vector>

#include "ID.h"

namespace db {

// The nodes one batch of the distinct search has reached, an open-addressing table keyed by
// node so a batch costs its ball and not the graph. A slot holds the seeds that have reached
// the node, those it propagates at the level being expanded, and those it gained during it.
class PathReachTable {
public:
    // Thirty-two bytes, aligned so a probe reads one cache line
    struct alignas(32) Slot {
        uint64_t _node {emptyKey};
        uint64_t _seen {0};
        uint64_t _frontier {0};
        uint64_t _gained {0};
    };

    PathReachTable();
    ~PathReachTable();

    // The node's slot, inserted empty the first time the node is reached
    Slot& reach(NodeID node);

    // The slot of a node reached before
    Slot& get(NodeID node);

    size_t size() const { return _count; }

    // Forgets every node reached, at the cost of those nodes rather than of the table
    void clear();

private:
    static constexpr uint64_t emptyKey = ~0ull;
    static constexpr size_t initialCapacity = 128;

    std::vector<Slot> _slots;
    std::vector<size_t> _occupied;
    uint64_t _mask {0};
    size_t _count {0};

    static uint64_t hashOf(uint64_t key) { return (key * 0x9E3779B97F4A7C15ull) >> 20; }

    size_t find(uint64_t key) const;
    void allocate(size_t capacity);
    void grow();
};

}
