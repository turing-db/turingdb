#pragma once

#include "views/GraphView.h"

namespace db {

class PlanGraph;
class LocalMemory;

class PlanOptimizer {
public:
    explicit PlanOptimizer(PlanGraph* plan, GraphView view, LocalMemory* mem);
    ~PlanOptimizer();

    void optimize();

private:
    PlanGraph* _plan {nullptr};
    GraphView _view;
    LocalMemory* _mem {nullptr};

    void rewriteScanByLabels();
    void rewriteScanByConstIDs();
};

}
