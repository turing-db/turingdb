#pragma once

namespace db {

class PlanGraph;

class PlanOptimizer {
public:
    explicit PlanOptimizer(PlanGraph* plan, GraphView view);
    ~PlanOptimizer();

    void optimize();

private:
    PlanGraph* _plan {nullptr};

    void rewriteScanByLabels();
    void rewriteScanByConstIDs();
};

}
