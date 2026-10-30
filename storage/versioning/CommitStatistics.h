#pragma once

#include <mutex>

#include "datapart/DataPartSpan.h"
#include "datapart/DegreeProducts.h"
#include "datapart/EdgeCounts.h"

namespace db {

class Tombstones;

// The statistics of a commit: the edge counts and degree products of its parts summed, the
// edges its tombstones delete taken out of the counts, though not of the products, which
// would need each node's degrees. A commit still being built gains parts and tombstones
// after it is first read, so the sums are taken again once it holds a number of either they
// were not taken on.
class CommitStatistics {
public:
    CommitStatistics();
    ~CommitStatistics();

    CommitStatistics(const CommitStatistics&) = delete;
    CommitStatistics(CommitStatistics&&) = delete;
    CommitStatistics& operator=(const CommitStatistics&) = delete;
    CommitStatistics& operator=(CommitStatistics&&) = delete;

    size_t countEdges(DataPartSpan parts,
                      const Tombstones& tombstones,
                      const LabelSetHandle& source,
                      const LabelSetHandle& target);

    size_t countEdges(DataPartSpan parts,
                      const Tombstones& tombstones,
                      const LabelSetHandle& source,
                      EdgeTypeID edgeType,
                      const LabelSetHandle& target);

    size_t sumDegreeProducts(DataPartSpan parts,
                             const Tombstones& tombstones,
                             const LabelSetHandle& labels,
                             const DegreeSelection& first,
                             const DegreeSelection& second);

private:
    std::mutex _mutex;
    EdgeCounts _edgeCounts;
    DegreeProductIndex _degreeProducts;
    size_t _datapartCount {0};
    size_t _deletedEdgeCount {0};
    bool _isSummed {false};

    void sumIfStale(DataPartSpan parts, const Tombstones& tombstones);
};

}
