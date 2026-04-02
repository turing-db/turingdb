#pragma once

#include "PlanGraphNode.h"

namespace db {

class Column;
class VarDecl;

class ConstScanNode : public PlanGraphNode {
public:
    explicit ConstScanNode(Column* values, const VarDecl* var)
        : PlanGraphNode(PlanGraphOpcode::CONST_SCAN),
        _values(values),
        _var(var)
    {
    }

    Column* values() const { return _values; }
    const VarDecl* var() const { return _var; }

private:
    Column* _values {nullptr};
    /// The var used to track VarDecl -> ColumnTag correspondence
    const VarDecl* _var {nullptr};
};

}
