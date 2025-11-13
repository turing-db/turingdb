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
        return m_proj;
    }

    void setProjection(const Projection* proj) {
        m_proj = proj;
    }

private:
    const Projection* m_proj {nullptr};
};

}
