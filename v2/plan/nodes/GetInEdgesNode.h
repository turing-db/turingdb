#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class GetInEdgesNode : public PlanGraphNode {
public:
    explicit GetInEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_IN_EDGES)
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
