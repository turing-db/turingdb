#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class ScanNodesNode : public PlanGraphNode {
public:
    explicit ScanNodesNode()
        : PlanGraphNode(PlanGraphOpcode::SCAN_NODES)
    {
    }

    VarDecl* getNodeIDsDecl() const { return _nodeIDsDecl; }

    void setNodeIDsDecl(VarDecl* nodeIDsDecl) {
        _nodeIDsDecl = nodeIDsDecl;
    }

private:
    VarDecl* _nodeIDsDecl {nullptr};
};

}
