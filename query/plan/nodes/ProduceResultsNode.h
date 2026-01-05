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

    bool isProduceNone() const { return _prodNone; }

    void setProduceNone(bool prodNone) { _prodNone = prodNone; }

private:
    const Projection* _proj {nullptr};
    bool _prodNone {false};
};

}
