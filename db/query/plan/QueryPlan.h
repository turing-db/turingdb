#pragma once

#include <vector>

namespace db {

class QueryPlanStep;

class QueryPlan {
public:
    QueryPlan();
    ~QueryPlan();

    QueryPlanStep* getRoot() const { return _root; }

    void setRoot(QueryPlanStep* step) { _root = step; }

private:
    using QueryPlanSteps = std::vector<QueryPlanStep*>;
    QueryPlanSteps _steps;
    QueryPlanStep* _root {nullptr};

    void addStep(QueryPlanStep* step);
};

}
