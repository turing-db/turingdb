#pragma once

#include <mutex>
#include <span>
#include <stddef.h>
#include <vector>

#include "datapart/DataPartSpan.h"
#include "ID.h"

namespace db {

// Whether the edges of a set of types, over every part and the deleted ones included,
// form a DAG: one topological sort per set, kept for the parts it ran on. The parts are
// immutable once committed, and an entry sorted on fewer of them than the caller holds
// is a miss.
class EdgeTypeAcyclicityCache {
public:
    EdgeTypeAcyclicityCache();
    ~EdgeTypeAcyclicityCache();

    EdgeTypeAcyclicityCache(const EdgeTypeAcyclicityCache&) = delete;
    EdgeTypeAcyclicityCache& operator=(const EdgeTypeAcyclicityCache&) = delete;

    bool isAcyclic(DataPartSpan parts, std::span<const EdgeTypeID> edgeTypes);

private:
    struct Entry {
        std::vector<EdgeTypeID> _edgeTypes;
        size_t _nodeCount {0};
        size_t _edgeCount {0};
        bool _acyclic {false};
    };

    std::mutex _mutex;
    std::vector<Entry> _entries;
};

}
