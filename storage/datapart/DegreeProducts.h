#pragma once

#include <memory>
#include <span>
#include <vector>

#include "ID.h"
#include "datapart/DataPartSpan.h"
#include "metadata/LabelSetHandle.h"

namespace db {

class EdgeContainer;
class EdgeIndexer;
class NodeContainer;

// A node's degree in the edges of one type that it is the source of, or the target of
struct TypedDegree {
    EdgeTypeID _edgeTypeID;
    bool _isOut {true};
};

// The typed degrees a sum reads on one side of its products: those of some types, or of
// every type, all on the same side of their edges
struct DegreeSelection {
    std::span<const EdgeTypeID> _edgeTypes;
    bool _isEveryType {false};
    bool _isOut {true};
};

struct DegreeProduct {
    LabelSetHandle _labelSet;
    TypedDegree _first;
    TypedDegree _second;
    size_t _sum {0};
};

// For each label set and each pair of typed degrees, the sum over the label set's nodes of
// the product of the node's two degrees, or of degree times degree less one when the two are
// one, as a pattern never takes one edge twice. One entry per triple some node has both
// degrees of, ordered by label set ID, then by the two degrees, the first never after the
// second.
class DegreeProducts {
public:
    DegreeProducts();
    ~DegreeProducts();

    DegreeProducts(const DegreeProducts&) = delete;
    DegreeProducts(DegreeProducts&&) = delete;
    DegreeProducts& operator=(const DegreeProducts&) = delete;
    DegreeProducts& operator=(DegreeProducts&&) = delete;

    // The products a part adds to those of @param earlierParts: those of the nodes it holds,
    // and for each earlier node it adds edges to, its products after them less before
    static std::unique_ptr<DegreeProducts> create(const EdgeContainer& edges,
                                                  const EdgeIndexer& indexer,
                                                  const NodeContainer& nodes,
                                                  DataPartSpan earlierParts);

    // Replaces the entries by @param products, adding up those of the same triple
    void assign(std::span<const DegreeProduct> products);

    std::span<const DegreeProduct> getProducts() const { return _products; }

private:
    std::vector<DegreeProduct> _products;
};

// Degree products laid out for sums over them: each pair under both of its orders, by label
// set, then by the side and type of the first degree, so a sum reads only the entries of
// the degrees it selects first
class DegreeProductIndex {
public:
    DegreeProductIndex();
    ~DegreeProductIndex();

    DegreeProductIndex(const DegreeProductIndex&) = delete;
    DegreeProductIndex(DegreeProductIndex&&) = delete;
    DegreeProductIndex& operator=(const DegreeProductIndex&) = delete;
    DegreeProductIndex& operator=(DegreeProductIndex&&) = delete;

    void build(std::span<const DegreeProduct> products);

    // The sum over the nodes carrying every label of @param labels of their degree in the
    // edges of @param first times their degree in the edges of @param second, pairs of one
    // edge left out
    size_t sumProducts(const LabelSetHandle& labels,
                       const DegreeSelection& first,
                       const DegreeSelection& second) const;

private:
    struct Entry {
        TypedDegree _first;
        TypedDegree _second;
        size_t _sum {0};
    };

    struct LabelSetEntries {
        LabelSetHandle _labelSet;
        size_t _begin {0};
        size_t _end {0};
    };

    std::vector<Entry> _entries;
    std::vector<LabelSetEntries> _labelSets;
};

}
