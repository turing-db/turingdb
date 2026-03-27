#pragma once

#include "PlanGraphNode.h"

namespace db {

class Column;

class ConstScanNode : public PlanGraphNode {
public:
    explicit ConstScanNode(const Column* values)
        : PlanGraphNode(PlanGraphOpcode::CONST_SCAN),
        _values(values)
    {
    }

    const Column* values() const { return _values; }

private:
    const Column* _values;
};

}
