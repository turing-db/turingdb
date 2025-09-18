#pragma once

#include <vector>
#include <string_view>
#include <unordered_map>
#include <memory>

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

using ExprConstraints = std::vector<std::pair<PropertyType, Expr*>>;

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

    const GraphView& _view;

    PlanGraph _tree;
    const CypherAST* _ast {nullptr};

    // Cache for label sets
    std::unordered_map<LabelNames,
                       const LabelSet*,
                       VectorHash<std::string_view>> _labelSetCache;
    std::vector<std::unique_ptr<LabelSet>> _labelSets;

    // Map of variable nodes
    std::unordered_map<const VarDecl*, PlanGraphNode*> _varNodesMap;

    // Map of expr dependencies
    std::unordered_multimap<const Expr*, PlanGraphNode*> _exprDependenciesMap;

    const LabelSet* getOrCreateLabelSet(const Symbols& symbols);
    const LabelSet* buildLabelSet(const Symbols& symbols);
    LabelID getLabel(const Symbol* symbol);

    void addVarNode(PlanGraphNode* node, const VarDecl* varDecl);
    PlanGraphNode* getOrCreateVarNode(const VarDecl* varDecl);
    PlanGraphNode* getVarNode(const VarDecl* varDecl) const;

    void generateSinglePartQuery(const SinglePartQuery* query);
    void generateStmt(const Stmt* stmt);
    void generateMatchStmt(const MatchStmt* stmt);
    void generatePatternElement(const PatternElement* element);
    PlanGraphNode* generatePatternElementOrigin(const NodePattern* origin);
    PlanGraphNode* generatePatternElementEdge(PlanGraphNode* currentNode, const EdgePattern* edge);
    PlanGraphNode* generatePatternElementTarget(PlanGraphNode* currentNode, const NodePattern* target);
    PlanGraphNode* generateExprConstraints(PlanGraphNode* currentNode, const VarDecl* varDecl, const ExprConstraints& expr);
    PlanGraphNode* generateVarNode(PlanGraphNode* currentNode, const VarDecl* varDecl);
    void generateExprDependencies(PlanGraphNode* currentNode, const Expr* expr);
    void generateExprDependencies(PlanGraphNode* currentNode, const BinaryExpr* expr);
    void generateExprDependencies(PlanGraphNode* currentNode, const UnaryExpr* expr);
    void generateExprDependencies(PlanGraphNode* currentNode, const StringExpr* expr);
    void generateExprDependencies(PlanGraphNode* currentNode, const NodeLabelExpr* expr);
    void generateExprDependencies(PlanGraphNode* currentNode, const PropertyExpr* expr);
    void generateExprDependencies(PlanGraphNode* currentNode, const PathExpr* expr);
    void generateExprDependencies(PlanGraphNode* currentNode, const SymbolExpr* expr);
    void generateExprDependencies(PlanGraphNode* currentNode, const LiteralExpr* expr);

    void throwError(std::string_view msg, const void* obj = 0) const;
};

}
