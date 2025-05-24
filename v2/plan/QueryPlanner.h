#pragma once

#include "QueryCallback.h"
#include "PlanTree.h"

namespace db {

class GraphView;
class QueryCommand;
class MatchCommand;
class MatchTarget;
class EntityPattern;
class PlanTreeNode;

class QueryPlannerV2 {
public:
    QueryPlannerV2(const GraphView& view,
                   QueryCallback callback);
    ~QueryPlannerV2();

    void plan(const QueryCommand& query);

private:
    PlanTree _tree;

    void planMatchCommand(const MatchCommand& cmd);
    void planMatchTarget(const MatchTarget* target);

    PlanTreeNode* planPathOrigin(const EntityPattern* pattern);
    PlanTreeNode* planPathExpand(PlanTreeNode* currentNode,
                                 const EntityPattern* edge,
                                 const EntityPattern* target);
};

}
