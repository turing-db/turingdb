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

    VarDecl* getNodeDecl() const { return _nodeDecl; }

    void setNodeDecl(VarDecl* nodeDecl) {
        _nodeDecl = nodeDecl;
    }

private:
    VarDecl* _nodeDecl {nullptr};
};

}
