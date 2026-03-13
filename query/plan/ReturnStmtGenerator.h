#pragma once

#include <queue>
#include <string_view>
#include <variant>

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
class PropertyExpr;
class GetPropertyWithNullNode;

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
                        GetPropertyCache& propCache);

    ~ReturnStmtGenerator() = default;

    ReturnStmtGenerator(const ReturnStmtGenerator&) = delete;
    ReturnStmtGenerator(ReturnStmtGenerator&&) = delete;
    ReturnStmtGenerator& operator=(const ReturnStmtGenerator&) = delete;
    ReturnStmtGenerator& operator=(ReturnStmtGenerator&&) = delete;

    /**
     * @brief Performs breadth first search over expressions in the RETURN clause to
     * determine the placement of @ref ExprEvalNode, @ref AggregateEvalNode, and @ref
     * GetPropertyWithNullNodes; subsequently adds any other nodes required (e.g. @ref
     * OrderByNode).
     * @detail Uses two queues to explore a maximal initial segment of the expression
     * tree, such that this segment may be exhaustively evaulated by a single
     * @ref ExprEvalNode. Operations which may not be evaluated by this ExprEvalNode are
     * called "blockers", and form the boundary of this initial segment. All such
     * "blockers" are added to the @ref _blockers queue, and the exploration process
     * begins again from the children of each blocker. Each maximal initial segment is
     * added for evaluation in a single node by inserting that node into @ref _evalSteps.
     * Similarly, blockers are added (as e.g. AggregateEvalNodes) to @ref _evalSteps when
     * encountered. Since we are exploring from the root of each return expression, the
     * order of @ref _evalSteps is the *reverse* order which the nodes need be evaluated
     * in.
     */
    PlanGraphNode* generateReturnStmt();

private:
    using EvaluationQueue = std::queue<Expr*>;
    using EvaluationStep =
        std::variant<ExprEvalNode*, AggregateEvalNode*, GetPropertyWithNullNode*>;

    const CypherAST* _ast {nullptr};

    const ReturnStmt* _stmt {nullptr};
    Projection* _proj {nullptr};

    PlanGraph* _tree {nullptr};
    PlanGraphNode* _prevNode {nullptr};

    /// Series of PlanGraphNodes require to evaluate the return clause
    std::vector<EvaluationStep> _evalSteps;

    /// Pointers to nodes which may be needed for evaluation. Location not stable.
    ExprEvalNode* _exprEvalNode {nullptr};
    AggregateEvalNode* _aggrEvalNode {nullptr};

    /// Cache for potentially fetching properties which were already retrieved
    GetPropertyCache& _propCache;

    /// Queues used in the double-queue BFS exploration of returned expressions
    EvaluationQueue _frontier;
    EvaluationQueue _blockers;

    void prepare();

    /// Adds child expressions of @param expr to the @ref _frontier of BFS
    void expandExpr(Expr* expr);

    /// Whether @param expr requires an eternal (non-ExprEval) processor to be evaluated
    static bool isEvaluationBlocker(const Expr* expr);

    /// Adds @param expr to the appropriate external (non-ExprEval) node
    void handleEvaluationBlocker(const Expr* expr);

    /// Ensures @param prop has the correct VarDecl, potentially updating @param prop in place
    void fetchOrGenerateProperty(PropertyExpr* prop);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = nullptr) const;
};

}
