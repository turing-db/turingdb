#include "ListDBStep.h"

#include "QueryPlan.h"

using namespace db;

ListDBStep::ListDBStep()
{
}

ListDBStep::~ListDBStep() {
}

std::string ListDBStep::getName() const {
    return "ListDBStep";
}

ListDBStep* ListDBStep::create(QueryPlan* plan) {
    ListDBStep* step = new ListDBStep();
    plan->addStep(step);
    return step;
}
