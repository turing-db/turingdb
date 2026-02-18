#pragma once

#include <string_view>
namespace db {

class PlanGraph;
class PlanGraphNode;
class PlanGraphVariables;
class ReturnStmt;
class FuncEvalNode;
class AggregateEvalNode;
class GetPropertyCache;
class GetEntityTypeCache;
class Expr;
class Projection;

/**
 * @brief Helper class to wrap logic for generating the plan graph structure from a
 * @ref ReturnStmt
 */
class ReturnStmtGenerator {
public:
    ReturnStmtGenerator(ReturnStmt* rtnStmt, PlanGraph* tree, PlanGraphNode* prevNode,
                        PlanGraphVariables* vars, GetPropertyCache& propCache,
                        GetEntityTypeCache& entCache);


    PlanGraphNode* generateReturnStmt();

private:
    ReturnStmt* _stmt {nullptr};
    Projection* _proj {nullptr};

    PlanGraph* _tree {nullptr};
    PlanGraphNode* _prevNode {nullptr};
    PlanGraphVariables* _variables {nullptr};

    AggregateEvalNode* _aggrEvalNode {nullptr};
    FuncEvalNode* _funcEvalNode {nullptr};

    GetPropertyCache& _propCache;
    GetEntityTypeCache& _entCache;

    void prepare();
    void handleExprDependencies(Expr* expr);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
