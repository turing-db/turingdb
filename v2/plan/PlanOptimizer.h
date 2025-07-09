#pragma once

#include <vector>

namespace db {

class PlanGraph;
class PlanGraphNode;

class PlanOptimizer {
public:
    PlanOptimizer(PlanGraph& planGraph);
    ~PlanOptimizer();

    void optimize();

private:
    PlanGraph& _planGraph;
    std::vector<PlanGraphNode*> _roots;

    void removeUnusedVars();
};

}
