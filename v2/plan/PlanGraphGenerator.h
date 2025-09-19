#pragma once

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

    PlanGraphVariables _variables;

    // Map of expr dependencies
    std::unordered_multimap<const Expr*, PlanGraphNode*> _exprDependenciesMap;

    const LabelSet* getOrCreateLabelSet(const Symbols& symbols);
    const LabelSet* buildLabelSet(const Symbols& symbols);
    LabelID getLabel(const Symbol* symbol);

    void generateSinglePartQuery(const SinglePartQuery* query);
    void generateStmt(const Stmt* stmt);
    void generateMatchStmt(const MatchStmt* stmt);
    void generatePatternElement(const PatternElement* element);
    PlanGraphNode* generatePatternElementOrigin(const NodePattern* origin);
    PlanGraphNode* generatePatternElementEdge(PlanGraphNode* currentNode, const EdgePattern* edge);
    PlanGraphNode* generatePatternElementTarget(PlanGraphNode* currentNode, const NodePattern* target);

    void throwError(std::string_view msg, const void* obj = 0) const;
};

}
