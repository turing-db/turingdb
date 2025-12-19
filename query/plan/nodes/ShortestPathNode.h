#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {
class VarDecl;

class ShortestPathNode: public PlanGraphNode {
public:

    explicit ShortestPathNode()
        : PlanGraphNode(PlanGraphOpcode::SHORTEST_PATH)
    {
    }

private:
    const VarDecl* _leftJoinKeyVar;
    const VarDecl* _rightJoinKeyVar;
};

}
