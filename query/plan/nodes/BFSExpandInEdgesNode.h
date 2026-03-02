#pragma once

#include <stdint.h>

#include "PlanGraphNode.h"

namespace db {

class VarDecl;

class BFSExpandInEdgesNode : public PlanGraphNode {
public:
    explicit BFSExpandInEdgesNode(const VarDecl* edgeDecl,
                                  const VarDecl* sourceDecl,
                                  int64_t minHops,
                                  int64_t maxHops)
        : PlanGraphNode(PlanGraphOpcode::BFS_EXPAND_IN_EDGES),
          _edgeDecl(edgeDecl),
          _sourceDecl(sourceDecl),
          _minHops(minHops),
          _maxHops(maxHops) {
    }

    int64_t getMinHops() const { return _minHops; }
    int64_t getMaxHops() const { return _maxHops; }
    const VarDecl* getEdgeDecl() const { return _edgeDecl; }
    const VarDecl* getSourceDecl() const { return _sourceDecl; }

private:
    const VarDecl* _edgeDecl {nullptr};
    const VarDecl* _sourceDecl {nullptr};
    int64_t _minHops {0};
    int64_t _maxHops {0};
};

}
