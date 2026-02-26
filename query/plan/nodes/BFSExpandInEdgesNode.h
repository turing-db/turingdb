#pragma once

#include <stdint.h>

#include "PlanGraphNode.h"

namespace db {

class BFSExpandInEdgesNode : public PlanGraphNode {
public:
    explicit BFSExpandInEdgesNode(int64_t minHops, int64_t maxHops)
        : PlanGraphNode(PlanGraphOpcode::BFS_EXPAND_IN_EDGES),
        _minHops(minHops),
        _maxHops(maxHops)
    {
    }

    int64_t getMinHops() const { return _minHops; }
    int64_t getMaxHops() const { return _maxHops; }

private:
    int64_t _minHops {0};
    int64_t _maxHops {0};
};

}
