#pragma once

namespace db::v2 {

class PlanGraph;

class PlanOptimizer {
public:
    explicit PlanOptimizer(PlanGraph* plan);
    ~PlanOptimizer();

    void optimize();

private:
    PlanGraph* _plan {nullptr};

    void rewriteScanByLabels();
};

}
