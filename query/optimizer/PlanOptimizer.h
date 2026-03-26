#pragma once

#include "views/GraphView.h"

namespace db {

class PlanGraph;
class LocalMemory;
class CypherAST;

class PlanOptimizer {
public:
    explicit PlanOptimizer(PlanGraph* plan, GraphView view, LocalMemory* mem, CypherAST* ast);
    ~PlanOptimizer();

    void optimize();

private:
    PlanGraph* _plan {nullptr};
    GraphView _view;
    LocalMemory* _mem {nullptr};
    CypherAST* _ast {nullptr};

    void rewriteScanByLabels();
    void rewriteScanByConstIDs();
    void rewriteConstWriteSources();
    void rewritePropertyFilterWithIndex();
    void rewriteNodePropertyFilterWithIndex();
};

}
