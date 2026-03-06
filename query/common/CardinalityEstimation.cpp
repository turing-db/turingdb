#include "CardinalityEstimation.h"

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

bool CardinalityEstimation::isSmallCartesianProduct(const LabelSet& left,
                                                     const LabelSet& right) const {
    const size_t leftCount = estimateNodeCount(left);
    const size_t rightCount = estimateNodeCount(right);
    return leftCount * rightCount < SMALL_CARTESIAN_THRESHOLD;
}
