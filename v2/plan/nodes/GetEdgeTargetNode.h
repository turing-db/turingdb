#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class GetEdgeTargetNode : public PlanGraphNode {
public:
    explicit GetEdgeTargetNode()
        : PlanGraphNode(PlanGraphOpcode::GET_EDGE_TARGET)
    {
    }

    VarDecl* getNodeDecl() const { return _nodeDecl; }

    void setNodeDecl(VarDecl* nodeDecl) {
        _nodeDecl = nodeDecl;
    }

private:
    VarDecl* _nodeDecl {nullptr};
};

}
