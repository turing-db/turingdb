#pragma once

#include <stddef.h>
#include <span>
#include <vector>

#include "ID.h"

namespace db {

// Edge i joins node i to node i + 1, so a path holds one node more than it has edges.
// A path with no node is the null path, which an OPTIONAL MATCH that missed binds.
class GraphPath {
public:
    GraphPath();
    ~GraphPath();

    GraphPath(const GraphPath& other);
    GraphPath(GraphPath&& other) noexcept;
    GraphPath& operator=(const GraphPath& other);
    GraphPath& operator=(GraphPath&& other) noexcept;

    bool operator==(const GraphPath& other) const;

    void addNode(NodeID node) { _nodes.push_back(node); }
    void addEdge(EdgeID edge) { _edges.push_back(edge); }

    void resize(size_t nodeCount, size_t edgeCount);
    void clear();

    bool isNull() const { return _nodes.empty(); }
    size_t getLength() const { return _edges.size(); }

    std::span<const NodeID> getNodes() const { return _nodes; }
    std::span<const EdgeID> getEdges() const { return _edges; }
    std::span<NodeID> getNodes() { return _nodes; }
    std::span<EdgeID> getEdges() { return _edges; }

private:
    std::vector<NodeID> _nodes;
    std::vector<EdgeID> _edges;
};

}
