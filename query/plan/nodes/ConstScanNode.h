#pragma once

#include "PlanGraphNode.h"

#include <vector>

namespace db {

template <typename T>
class ConstScanNode : public PlanGraphNode {
public:
    explicit ConstScanNode(const std::vector<T>& values)
        : PlanGraphNode(PlanGraphOpcode::CONST_SCAN),
        _values(values)
    {
    }

    const std::vector<T>& getValues() const { return _values; }

private:
    std::vector<T> _values;
};

}
