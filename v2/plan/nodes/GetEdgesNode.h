#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class GetEdgesNode : public PlanGraphNode {
public:
    explicit GetEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_EDGES)
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
