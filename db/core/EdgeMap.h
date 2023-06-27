#pragma once

#include <vector>
#include <map>

#include "Range.h"
#include "DBObject.h"

namespace db {

class Edge;
class EdgeType;

class EdgeMap {
public:
    friend DBComparator;
    using EdgeVector = std::vector<Edge*>;
    using Edges = std::map<const EdgeType*, EdgeVector, DBObject::Sorter>;
    using EdgeVectorRange = STLRange<EdgeVector>;

    struct EdgeMapFlattener {
        STLRange<EdgeVector> operator()(const auto& baseIt) const {
            const EdgeVector& edgeVec = *baseIt;
            return STLRange(&edgeVec);
        }
    };

    using EdgeRange = FlatRange<STLIndexRange<Edges>, EdgeMapFlattener>;

    EdgeMap();
    ~EdgeMap();

    EdgeRange edges() const;

    EdgeVectorRange edges(const EdgeType* type) const;
    size_t size() const { return _edges.size(); }

    void addEdge(Edge* edge);

private:
    Edges _edges;
};

}
