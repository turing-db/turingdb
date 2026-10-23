#include "GraphPath.h"

#include "BioAssert.h"

using namespace db;

GraphPath::GraphPath() = default;

GraphPath::~GraphPath() = default;

GraphPath::GraphPath(const GraphPath& other) = default;

GraphPath::GraphPath(GraphPath&& other) noexcept = default;

GraphPath& GraphPath::operator=(const GraphPath& other) = default;

GraphPath& GraphPath::operator=(GraphPath&& other) noexcept = default;

bool GraphPath::operator==(const GraphPath& other) const {
    return _ids == other._ids;
}

void GraphPath::start(NodeID node) {
    _ids.assign(1, node.getValue());
}

void GraphPath::addHop(EdgeID edge, NodeID node) {
    bioassert(!isNull(), "A hop leaves the last node of a path, and a null path has none");

    _ids.push_back(edge.getValue());
    _ids.push_back(node.getValue());
}

void GraphPath::setNodeCount(size_t nodeCount) {
    _ids.resize(idCountOf(nodeCount));
}

size_t GraphPath::idCountOf(size_t nodeCount) {
    return nodeCount == 0 ? 0 : 2 * nodeCount - 1;
}

void GraphPath::clear() {
    _ids.clear();
}
