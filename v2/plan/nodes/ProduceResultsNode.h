#pragma once

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class ProduceResultsNode : public PlanGraphNode {
public:
    using ReturnValues = std::vector<const VarDecl*>;

    ProduceResultsNode()
        : PlanGraphNode(PlanGraphOpcode::PRODUCE_RESULTS)
    {
    }

    const ReturnValues& getReturnValues() const {
        return _toReturn;
    }

    void addReturnValue(const VarDecl* var) {
        _toReturn.push_back(var);
    }

private:
    ReturnValues _toReturn;
};

}
