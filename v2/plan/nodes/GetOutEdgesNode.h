#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class GetOutEdgesNode : public PlanGraphNode {
public:
    explicit GetOutEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_OUT_EDGES)
    {
    }

    VarDecl* getEdgeDecl() const { return _edgeDecl; }

    void setEdgeDecl(VarDecl* edgeDecl) {
        _edgeDecl = edgeDecl;
    }

private:
    VarDecl* _edgeDecl {nullptr};
};

}
