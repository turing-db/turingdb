#include "Planner.h"

#include <vector>

#include "QueryCommand.h"
#include "QueryPlan.h"
#include "FromTarget.h"
#include "PathPattern.h"

#include "ListDBStep.h"
#include "OpenDBStep.h"

using namespace db;

Planner::Planner(const QueryCommand* query,
                 InterpreterContext* interpCtxt)
    : _query(query),
    _interpCtxt(interpCtxt),
    _plan(new QueryPlan())
{
}

Planner::~Planner() {
    if (_plan) {
        delete _plan;
    }
}

bool Planner::buildQueryPlan() {
    switch (_query->getKind()) {
        case QueryCommand::QCOM_LIST_COMMAND:
            return planListCommand(static_cast<const ListCommand*>(_query));
        break;

        case QueryCommand::QCOM_OPEN_COMMAND:
            return planOpenCommand(static_cast<const OpenCommand*>(_query));
        break;

        case QueryCommand::QCOM_SELECT_COMMAND:
            return planSelectCommand(static_cast<const SelectCommand*>(_query));
        break;

        default:
            return false;
        break;
    }
}

bool Planner::planListCommand(const ListCommand* cmd) {
    ListDBStep* listDB = ListDBStep::create(_plan);
    _plan->setRoot(listDB);
    return true;
}

bool Planner::planOpenCommand(const OpenCommand* cmd) {
    OpenDBStep* openDB = OpenDBStep::create(_plan, cmd->getPath());
    _plan->setRoot(openDB);
    return true;
}

bool Planner::planSelectCommand(const SelectCommand* cmd) {
    return false;
}

QueryPlanStep* Planner::planPathPattern(const PathPattern* pattern) {
    auto originStep = planNodePattern(pattern->getOrigin());
    if (!originStep) {
        return nullptr;
    }

    auto prevStep = originStep;
    for (const PathElement* elem : pattern->elements()) {
        auto expandStep = planPathElement(elem, prevStep);
        if (!expandStep) {
            return nullptr;
        }
    }

    return prevStep;
}

QueryPlanStep* Planner::planNodePattern(const NodePattern* pattern) {
    EntityPattern* entity = pattern->getEntity();
    
    TypeConstraint* typeConstr = entity->getTypeConstraint();
    ExprConstraint* exprConstr = entity->getExprConstraint();

    return nullptr;
}
