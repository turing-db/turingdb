#pragma once

#include "QueryPlanStep.h"

namespace db {

class QueryPlan;

class ListDBStep : public QueryPlanStep {
public:
    std::string getName() const override;

    static ListDBStep* create(QueryPlan* plan);

private:
    ListDBStep();
    ~ListDBStep();
};

}
