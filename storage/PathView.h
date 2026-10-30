#pragma once

#include <stddef.h>
#include <stdint.h>
#include <span>

#include "ID.h"

namespace db {

// A path held as a list element or a map value: a view of its IDs, laid out as GraphPath
// lays them out, in a buffer the list or map owns beside its elements. A list holds a null
// path as a null element, so a viewed path always has a node.
class PathView {
public:
    PathView() = default;

    PathView(const uint64_t* ids, size_t idCount)
        : _ids(ids, idCount)
    {
    }

    explicit PathView(std::span<const uint64_t> ids)
        : _ids(ids)
    {
    }

    size_t getNodeCount() const { return (_ids.size() + 1) / 2; }
    size_t getLength() const { return _ids.size() / 2; }

    NodeID getNode(size_t index) const { return NodeID(_ids[2 * index]); }
    EdgeID getEdge(size_t index) const { return EdgeID(_ids[2 * index + 1]); }

    std::span<const uint64_t> getIDs() const { return _ids; }

private:
    std::span<const uint64_t> _ids;
};

}
