#include "GraphPath.h"

using namespace db;

GraphPath::GraphPath() = default;

GraphPath::~GraphPath() = default;

GraphPath::GraphPath(const GraphPath& other) = default;

GraphPath::GraphPath(GraphPath&& other) noexcept = default;

GraphPath& GraphPath::operator=(const GraphPath& other) = default;

GraphPath& GraphPath::operator=(GraphPath&& other) noexcept = default;

bool GraphPath::operator==(const GraphPath& other) const {
    return _nodes == other._nodes && _edges == other._edges;
}

void GraphPath::resize(size_t nodeCount, size_t edgeCount) {
    _nodes.resize(nodeCount);
    _edges.resize(edgeCount);
}

void GraphPath::clear() {
    _nodes.clear();
    _edges.clear();
}
