#include "QueryPlanner.h"

#include <range/v3/view/drop.hpp>
#include <range/v3/view/chunk.hpp>

#include "QueryCommand.h"
#include "MatchTarget.h"
#include "PathPattern.h"
#include "Expr.h"

#include "PlanTreeNode.h"

#include "PlannerException.h"

using namespace db;
namespace rv = ranges::views;

QueryPlannerV2::QueryPlannerV2(const GraphView& view,
                               QueryCallback callback)
{
}

QueryPlannerV2::~QueryPlannerV2() {
}

void QueryPlannerV2::plan(const QueryCommand& query) {
    switch (query.getKind()) {
        case QueryCommand::Kind::MATCH_COMMAND:
            planMatchCommand(static_cast<const MatchCommand&>(query));
            break;

        default:
            throw PlannerException("Unsupported query command of type "
                                   + std::to_string((unsigned)query.getKind()));
            break;
    }
}

void QueryPlannerV2::planMatchCommand(const MatchCommand& cmd) {    
    const auto& matchTargets = cmd.matchTargets();
    for (const auto& target : matchTargets) {
        planMatchTarget(target);
    }
}

void QueryPlannerV2::planMatchTarget(const MatchTarget* target) {
    const PathPattern* pattern = target->getPattern();
    const auto& path = pattern->elements();

    if (path.empty()) {
        throw PlannerException("Empty match target pattern");
    }

    PlanTreeNode* currentNode = planPathOrigin(path[0]);

    const auto expandSteps = path | rv::drop(1) | rv::chunk(2);
    for (auto step : expandSteps) {
        const EntityPattern* edge = step[0];
        const EntityPattern* target = step[1];
        currentNode = planPathExpand(currentNode, edge, target);
    }
}

PlanTreeNode* QueryPlannerV2::planPathOrigin(const EntityPattern* pattern) {
    const VarExpr* var = pattern->getVar();
    const TypeConstraint* typeConstr = pattern->getTypeConstraint();
    const ExprConstraint* exprConstr = pattern->getExprConstraint();

    // Scan nodes
    PlanTreeNode* current = nullptr;
    if (typeConstr) {
        current = _tree.create(PlanTreeOpcode::SCAN_NODES_BY_LABEL); 
    } else {
        current = _tree.create(PlanTreeOpcode::SCAN_NODES)
    }

    // Expression constraints
    if (exprConstr) {
        const auto& exprs = exprConstr->getExpressions();
        for (const BinExpr* expr : exprs) {
            auto filter = _tree.create(PlanTreeOpcode::FILTER);
            filter->setInput(current);
            current = filter;
        }
    }

    if (var) {
        PlanTreeNode* varNode = _tree.create(PlanTreeOpcode::VAR);
        varNode->setVarDecl(var->getDecl());
        varNode->setInput(current);
    }

    return nullptr;
}

PlanTreeNode* QueryPlannerV2::planPathExpand(PlanTreeNode* currentNode,
                                             const EntityPattern* edge,
                                             const EntityPattern* target) {
    return nullptr;
}
