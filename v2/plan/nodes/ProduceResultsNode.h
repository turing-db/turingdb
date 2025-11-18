#pragma once

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class Projection;

class ProduceResultsNode : public PlanGraphNode {
public:
    ProduceResultsNode()
        : PlanGraphNode(PlanGraphOpcode::PRODUCE_RESULTS)
    {
    }

    const Projection* getProjection() const {
        return _proj;
    }

    void setProjection(const Projection* proj) {
        _proj = proj;
    }

private:
    const Projection* _proj {nullptr};
};

}
