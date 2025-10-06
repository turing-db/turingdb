#pragma once

#include <cstdint>
#include <memory>
#include <vector>

namespace db::v2 {

class CypherAST;
class Stmt;
class MatchStmt;
class WhereClause;
class PatternElement;
class Expr;
class VarNode;
class NodePattern;
class EdgePattern;
class PlanGraph;
class PlanGraphNode;
class PlanGraphVariables;
struct PropertyConstraint;

class ReadStatementGenerator {
public:
    ReadStatementGenerator(const CypherAST* ast,
                           PlanGraph* tree,
                           PlanGraphVariables* variables);

    ~ReadStatementGenerator();

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
    void insertDataFlowNode(const VarNode* node, VarNode* dependency);

private:
    const CypherAST* _ast {nullptr};
    PlanGraph* _tree {nullptr};
    PlanGraphVariables* _variables {nullptr};

    std::vector<std::unique_ptr<PropertyConstraint>> _propConstraints;
};

}
