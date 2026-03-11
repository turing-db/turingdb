#pragma once

#include <queue>
#include <string_view>

namespace db {

class PlanGraph;
class PlanGraphNode;
class PlanGraphVariables;
class ReturnStmt;
class FuncEvalNode;
class ExprEvalNode;
class AggregateEvalNode;
class GetPropertyCache;
class GetEntityTypeCache;
class Expr;
class Projection;
class CypherAST;

/**
 * @brief Helper class to wrap logic for generating the plan graph structure from a
 * @ref ReturnStmt
 */
class ReturnStmtGenerator {
public:
    ReturnStmtGenerator(const CypherAST* ast,
                        const ReturnStmt* rtnStmt,
                        PlanGraph* tree,
                        PlanGraphNode* prevNode,
                        PlanGraphVariables* vars,
                        GetPropertyCache& propCache,
                        GetEntityTypeCache& entCache);

    ~ReturnStmtGenerator() = default;

    ReturnStmtGenerator(const ReturnStmtGenerator&) = delete;
    ReturnStmtGenerator(ReturnStmtGenerator&&) = delete;
    ReturnStmtGenerator& operator=(const ReturnStmtGenerator&) = delete;
    ReturnStmtGenerator& operator=(ReturnStmtGenerator&&) = delete;

    PlanGraphNode* generateReturnStmt();

private:
    using EvaluationQueue = std::queue<const Expr*>;

    const CypherAST* _ast {nullptr};

    const ReturnStmt* _stmt {nullptr};
    Projection* _proj {nullptr};

    PlanGraph* _tree {nullptr};
    PlanGraphNode* _prevNode {nullptr};
    PlanGraphVariables* _variables {nullptr};

    ExprEvalNode* _exprEvalNode {nullptr};
    AggregateEvalNode* _aggrEvalNode {nullptr};

    GetPropertyCache& _propCache;
    GetEntityTypeCache& _entCache;

    EvaluationQueue _frontier;
    EvaluationQueue _blockers;

    void prepare();
    void handleExprDependencies(Expr* expr);

    void treeWalkExpr(const Expr* expr);
    static bool isEvaluationBlocker(const Expr* expr);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = nullptr) const;
};

}
