#pragma once

#include <unordered_set>
#include <vector>
#include <string_view>
#include <unordered_map>
#include <memory>

#include "PlanGraphVariables.h"
#include "QueryCallback.h"
#include "metadata/LabelSet.h"

#include "PlanGraph.h"

#include "VectorHash.h"
#include "metadata/PropertyType.h"

namespace db {
class GraphView;
}

namespace db::v2 {

class QueryCommand;
class SinglePartQuery;
class Stmt;
class Symbol;
class MatchStmt;
class ReturnStmt;
class PatternElement;
class EntityPattern;
class NodePattern;
class EdgePattern;
class CypherAST;
class Expr;
class BinaryExpr;
class UnaryExpr;
class StringExpr;
class NodeLabelExpr;
class PropertyExpr;
class PathExpr;
class SymbolExpr;
class LiteralExpr;
class FilterNode;
class VarNode;
class WhereClause;
class WherePredicate;
class PlanGraphTopology;
class PropertyConstraint;

class PlanGraphGenerator {
public:
    PlanGraphGenerator(const CypherAST& ast,
                       const GraphView& view,
                       const QueryCallback& callback);
    ~PlanGraphGenerator();

    PlanGraph& getPlanGraph() { return _tree; }

    void generate(const QueryCommand* query);

private:
    using Symbols = std::vector<Symbol*>;
    using LabelNames = std::vector<std::string_view>;

    const CypherAST* _ast {nullptr};
    const GraphView& _view;
    PlanGraph _tree;
    PlanGraphVariables _variables;
    std::vector<std::unique_ptr<PropertyConstraint>> _propConstraints;

    void generateSinglePartQuery(const SinglePartQuery* query);
    void generateStmt(const Stmt* stmt);
    void generateMatchStmt(const MatchStmt* stmt);
    void generateReturnStmt(const ReturnStmt* stmt);
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

    void throwError(std::string_view msg, const void* obj = 0) const;
};

}
