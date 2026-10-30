#pragma once

#include <stddef.h>
#include <stdint.h>
#include <span>
#include <vector>

#include "ID.h"
#include "PathView.h"

namespace db {

// The IDs of a path in the order it runs: node i at position 2i, then edge i, which joins
// node i to node i + 1, at 2i + 1. A path thus holds an odd number of IDs. The null path,
// which an OPTIONAL MATCH that missed binds, holds none.
class GraphPath {
public:
    GraphPath();
    ~GraphPath();

    GraphPath(const GraphPath& other);
    GraphPath(GraphPath&& other) noexcept;
    GraphPath& operator=(const GraphPath& other);
    GraphPath& operator=(GraphPath&& other) noexcept;

    bool operator==(const GraphPath& other) const;

    void start(NodeID node);
    void addHop(EdgeID edge, NodeID node);
    void setNodeCount(size_t nodeCount);
    void assign(PathView path);
    void clear();

    bool isNull() const { return _ids.empty(); }
    size_t getNodeCount() const { return (_ids.size() + 1) / 2; }
    size_t getLength() const { return _ids.size() / 2; }

    NodeID getNode(size_t index) const { return NodeID(_ids[2 * index]); }
    EdgeID getEdge(size_t index) const { return EdgeID(_ids[2 * index + 1]); }
    void setNode(size_t index, NodeID node) { _ids[2 * index] = node.getValue(); }
    void setEdge(size_t index, EdgeID edge) { _ids[2 * index + 1] = edge.getValue(); }

    std::span<const uint64_t> getIDs() const { return _ids; }
    std::span<uint64_t> getIDs() { return _ids; }

    static bool isNodeAt(size_t position) { return position % 2 == 0; }
    static size_t idCountOf(size_t nodeCount);

private:
    std::vector<uint64_t> _ids;
};

}
