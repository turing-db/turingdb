#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "views/GraphView.h"

namespace db {
class GraphMetadata;
}

namespace db::v2 {

class CypherAST;
class PlanGraph;
class PlanGraphNode;
class PlanGraphVariables;
class Stmt;
class MatchStmt;
class Skip;
class Limit;
class WhereClause;
class PatternElement;
class Expr;
class VarNode;
class NodePattern;
class EdgePattern;
struct PropertyConstraint;

class ReadStmtGenerator {
public:
    ReadStmtGenerator(const CypherAST* ast,
                      GraphView graphView,
                      PlanGraph* tree,
                      PlanGraphVariables* variables);

    ~ReadStmtGenerator();

    ReadStmtGenerator(const ReadStmtGenerator&) = delete;
    ReadStmtGenerator(ReadStmtGenerator&&) = delete;
    ReadStmtGenerator& operator=(const ReadStmtGenerator&) = delete;
    ReadStmtGenerator& operator=(ReadStmtGenerator&&) = delete;

    void generateStmt(const Stmt* stmt);
    void generateMatchStmt(const MatchStmt* stmt);
    void generateWhereClause(const WhereClause* where);
    void generatePatternElement(const PatternElement* element);

    VarNode* generatePatternElementOrigin(const NodePattern* origin);
    VarNode* generatePatternElementEdge(VarNode* prevNode, const EdgePattern* edge);
    VarNode* generatePatternElementTarget(VarNode* prevNode, const NodePattern* target);

    void unwrapWhereExpr(const Expr*);

    void incrementDeclOrders(uint32_t declOrder, PlanGraphNode* origin);
    void placeJoinsOnVars();
    void placePropertyExprJoins();
    void placePredicateJoins();
    PlanGraphNode* generateEndpoint();
    void insertDataFlowNode(const VarNode* node, VarNode* dependency);

private:
    const CypherAST* _ast {nullptr};
    GraphView _graphView;
    const GraphMetadata& _graphMetadata;
    PlanGraph* _tree {nullptr};
    PlanGraphVariables* _variables {nullptr};

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
