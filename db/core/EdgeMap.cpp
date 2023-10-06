#include "EdgeMap.h"

#include "Edge.h"
#include "EdgeType.h"

using namespace db;

EdgeMap::EdgeMap()
{
}

EdgeMap::~EdgeMap() {
}

EdgeMap::EdgeRange EdgeMap::edges() const {
    return FlatRange(STLIndexRange(&_edges), EdgeMapFlattener());
}

EdgeMap::EdgeVectorRange EdgeMap::edges(const EdgeType* type) const {
    const auto it = _edges.find(type);
    if (it == _edges.end()) {
        return EdgeVectorRange();
    }

    const EdgeVector& edgeVec = (*it).second;
    return EdgeVectorRange(&edgeVec);
}

size_t EdgeMap::size() const {
    size_t count = 0;
    for (const auto& [name, container] : _edges) {
        count += container.size();
    }
    return count;
}

void EdgeMap::addEdge(Edge* edge) {
    _edges[edge->getType()].push_back(edge);
}

