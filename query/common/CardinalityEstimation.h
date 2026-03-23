#pragma once

#include "views/GraphView.h"
#include "metadata/LabelSet.h"

namespace db {

class CardinalityEstimation {
public:
    static constexpr size_t SMALL_CARTESIAN_THRESHOLD = 100000;

    // When the query has a small LIMIT, cartesian product can stop early
    // while VHJ must process full chunks from both sides.  Skip VHJ when
    // the larger input side exceeds queryLimit * VHJ_LIMIT_RATIO.
    static constexpr size_t VHJ_LIMIT_RATIO = 200;
    static constexpr size_t MAX_LIMIT_FOR_VHJ_HEURISTIC = 10000;

    explicit CardinalityEstimation(GraphView graphView);

    // Estimate number of nodes matching a label set.
    // Returns total node count if labelset is empty.
    size_t estimateNodeCount(const LabelSet& labelset) const;

    // Returns true when cartesian product + filter is likely cheaper
    // than value hash join, either because the product is small or
    // because a small LIMIT makes early-termination effective.
    bool shouldPreferCartesian(const LabelSet& left, const LabelSet& right, size_t queryLimit) const;

    // Covers the case rhs expression is not var node ( e.g. a yielded item)
    bool shouldPreferCartesian(const LabelSet& left, size_t rightCardinality, size_t queryLimit) const;

private:
    GraphView _graphView;
};
}
