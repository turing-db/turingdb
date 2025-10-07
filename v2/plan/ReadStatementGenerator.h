#pragma once

#include "views/GraphView.h"
#include <cstdint>
#include <memory>
#include <vector>

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
class WhereClause;
class PatternElement;
class Expr;
class VarNode;
class NodePattern;
class EdgePattern;
struct PropertyConstraint;

class ReadStatementGenerator {
public:
    ReadStatementGenerator(const CypherAST* ast,
                           GraphView graphView,
                           PlanGraph* tree,
                           PlanGraphVariables* variables);

    ~ReadStatementGenerator();

    ReadStatementGenerator(const ReadStatementGenerator&) = delete;
    ReadStatementGenerator(ReadStatementGenerator&&) = delete;
    ReadStatementGenerator& operator=(const ReadStatementGenerator&) = delete;
    ReadStatementGenerator& operator=(ReadStatementGenerator&&) = delete;

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
    GraphView _graphView;
    const GraphMetadata& _graphMetadata;
    PlanGraph* _tree {nullptr};
    PlanGraphVariables* _variables {nullptr};

    std::vector<std::unique_ptr<PropertyConstraint>> _propConstraints;

    void throwError(std::string_view msg, const void* obj = 0) const;
};

}
