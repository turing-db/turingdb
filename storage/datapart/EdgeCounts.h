#pragma once

#include <memory>
#include <span>
#include <vector>

#include "ID.h"
#include "metadata/LabelSetHandle.h"

namespace db {

class EdgeIndexer;
class NodeContainer;

struct EdgeCount {
    EdgeTypeID _edgeTypeID;
    LabelSetHandle _sourceLabelSet;
    LabelSetHandle _targetLabelSet;
    size_t _count {0};
};

// The number of edges of each type from the nodes of one label set to the nodes of
// another, one entry per triple that holds an edge, ordered by type, then by the source's
// and the target's label set IDs.
class EdgeCounts {
public:
    EdgeCounts();
    ~EdgeCounts();

    EdgeCounts(const EdgeCounts&) = delete;
    EdgeCounts(EdgeCounts&&) = delete;
    EdgeCounts& operator=(const EdgeCounts&) = delete;
    EdgeCounts& operator=(EdgeCounts&&) = delete;

    static std::unique_ptr<EdgeCounts> create(const EdgeIndexer& indexer, const NodeContainer& nodes);

    // Replaces the entries by @param counts, adding up those of the same triple
    void assign(std::span<const EdgeCount> counts);

    // Takes away the edges of @param counts, every one of which the entries hold
    void subtract(std::span<const EdgeCount> counts);

    std::span<const EdgeCount> getCounts() const { return _counts; }

    // The edges from a node carrying every label of @param source to one carrying every
    // label of @param target, of every type or of @param edgeType
    size_t countEdges(const LabelSetHandle& source, const LabelSetHandle& target) const;
    size_t countEdges(const LabelSetHandle& source, EdgeTypeID edgeType, const LabelSetHandle& target) const;

private:
    std::vector<EdgeCount> _counts;
};

}
