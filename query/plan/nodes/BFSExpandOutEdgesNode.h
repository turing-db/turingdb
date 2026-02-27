#pragma once

#include <stdint.h>

#include "PlanGraphNode.h"

namespace db {

class VarDecl;

class BFSExpandOutEdgesNode : public PlanGraphNode {
public:
    explicit BFSExpandOutEdgesNode(const VarDecl* edgeDecl,
                                   const VarDecl* targetDecl,
                                   int64_t minHops,
                                   int64_t maxHops)
        : PlanGraphNode(PlanGraphOpcode::BFS_EXPAND_EDGES),
          _edgeDecl(edgeDecl),
          _targetDecl(targetDecl),
          _minHops(minHops),
          _maxHops(maxHops) {
    }

    int64_t getMinHops() const { return _minHops; }
    int64_t getMaxHops() const { return _maxHops; }
    const VarDecl* getEdgeDecl() const { return _edgeDecl; }
    const VarDecl* getTargetDecl() const { return _targetDecl; }

private:
    const VarDecl* _edgeDecl {nullptr};
    const VarDecl* _targetDecl {nullptr};
    int64_t _minHops {0};
    int64_t _maxHops {0};
};

}
