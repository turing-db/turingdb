#pragma once

#include "views/GraphView.h"
#include "metadata/LabelSet.h"

namespace db {

class CardinalityEstimation {
public:
    static constexpr size_t SMALL_CARTESIAN_THRESHOLD = 100000;

    explicit CardinalityEstimation(GraphView graphView);

    // Estimate number of nodes matching a label set.
    // Returns total node count if labelset is empty.
    size_t estimateNodeCount(const LabelSet& labelset) const;

    // Returns true if N*M < threshold, meaning a cartesian
    // product is cheap enough to not need a hash join.
    bool isSmallCartesianProduct(const LabelSet& left,
                                 const LabelSet& right) const;

private:
    GraphView _graphView;
};

}
