#pragma once

#include "PlanGraphNode.h"

namespace db {

class Column;
class VarDecl;

class ConstWriteSourceNode : public PlanGraphNode {
public:
    ConstWriteSourceNode(PlanGraphNodeID id,
                         Column* nodeIDs,
                         Column* values,
                         const VarDecl* nodeIDDecl,
                         const VarDecl* valuesDecl)
        : PlanGraphNode(id, PlanGraphOpcode::CONST_WRITE_SOURCE),
        _nodeIDs(nodeIDs),
        _values(values),
        _nodeIDDecl(nodeIDDecl),
        _valuesDecl(valuesDecl)
    {
    }

    ~ConstWriteSourceNode() override;

    Column* nodeIDs() const { return _nodeIDs; }
    Column* values() const { return _values; }
    const VarDecl* nodeIDDecl() const { return _nodeIDDecl; }
    const VarDecl* valuesDecl() const { return _valuesDecl; }

private:
    Column* _nodeIDs {nullptr};
    Column* _values {nullptr};
    const VarDecl* _nodeIDDecl {nullptr};
    const VarDecl* _valuesDecl {nullptr};
};

}
