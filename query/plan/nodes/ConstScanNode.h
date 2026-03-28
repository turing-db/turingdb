#pragma once

#include "PlanGraphNode.h"

namespace db {

class Column;

class ConstScanNode : public PlanGraphNode {
public:
    explicit ConstScanNode(Column* values)
        : PlanGraphNode(PlanGraphOpcode::CONST_SCAN),
        _values(values)
    {
    }

    Column* values() const { return _values; }

private:
    Column* _values;
};

}
