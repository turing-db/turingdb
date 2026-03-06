#pragma once

#include <stdint.h>

#include "PathExplorationDir.h"
#include "PlanGraphNode.h"

namespace db {

class VarDecl;

class PathExplorerNode : public PlanGraphNode {
public:
    explicit PathExplorerNode(const VarDecl* edgeDecl,
                              const VarDecl* targetDecl,
                              int64_t minHops,
                              int64_t maxHops)
        : PlanGraphNode(PlanGraphOpcode::PATH_EXPLORER),
        _edgeDecl(edgeDecl),
        _targetDecl(targetDecl),
        _minHops(minHops),
        _maxHops(maxHops)
    {
    }

    void setDir(PathExplorationDir dir) { _dir = dir; }

    PathExplorationDir getDir() const { return _dir; }
    int64_t getMinHops() const { return _minHops; }
    int64_t getMaxHops() const { return _maxHops; }
    const VarDecl* getEdgeDecl() const { return _edgeDecl; }
    const VarDecl* getTargetDecl() const { return _targetDecl; }

private:
    PathExplorationDir _dir {PathExplorationDir::Both};
    const VarDecl* _edgeDecl {nullptr};
    const VarDecl* _targetDecl {nullptr};
    int64_t _minHops {0};
    int64_t _maxHops {0};
};

}
