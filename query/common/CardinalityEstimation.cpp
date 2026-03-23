#include "CardinalityEstimation.h"

#include <algorithm>

#include "reader/GraphReader.h"

using namespace db;

CardinalityEstimation::CardinalityEstimation(GraphView graphView)
    : _graphView(graphView)
{
}

size_t CardinalityEstimation::estimateNodeCount(const LabelSet& labelset) const {
    GraphReader reader(_graphView);
    if (labelset.empty()) {
        return reader.getNodeCount();
    }
    return reader.getNodeCountMatchingLabelset(labelset.handle());
}

bool CardinalityEstimation::shouldPreferCartesian(const LabelSet& left, const LabelSet& right, size_t queryLimit) const {
    const size_t leftCount = estimateNodeCount(left);
    const size_t rightCount = estimateNodeCount(right);

    // Small cartesian product is cheap enough without a hash join.
    // Ceiling division avoids overflow: leftCount * rightCount < T iff leftCount < ceil(T / rightCount).
    if (rightCount == 0 || leftCount < (SMALL_CARTESIAN_THRESHOLD + rightCount - 1) / rightCount) {
        return true;
    }

    // With a small LIMIT, cartesian can terminate early while VHJ
    // must process full chunks from both sides
    if (queryLimit > 0 && queryLimit <= MAX_LIMIT_FOR_VHJ_HEURISTIC) {
        const size_t largerSide = std::max(leftCount, rightCount);
        if (largerSide > queryLimit * VHJ_LIMIT_RATIO) {
            return true;
        }
    }

    return false;
}

bool CardinalityEstimation::shouldPreferCartesian(const LabelSet& left, const size_t rightCardinality, size_t queryLimit) const {
    const size_t leftCount = estimateNodeCount(left);
    const size_t rightCount = rightCardinality;

    // Small cartesian product is cheap enough without a hash join.
    // Ceiling division avoids overflow: leftCount * rightCount < T iff leftCount < ceil(T / rightCount).
    if (rightCount == 0 || leftCount < (SMALL_CARTESIAN_THRESHOLD + rightCount - 1) / rightCount) {
        return true;
    }

    // With a small LIMIT, cartesian can terminate early while VHJ
    // must process full chunks from both sides
    if (queryLimit > 0 && queryLimit <= MAX_LIMIT_FOR_VHJ_HEURISTIC) {
        const size_t largerSide = std::max(leftCount, rightCount);
        if (largerSide > queryLimit * VHJ_LIMIT_RATIO) {
            return true;
        }
    }

    return false;
}
