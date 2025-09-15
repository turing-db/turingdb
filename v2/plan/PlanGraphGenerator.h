#pragma once

#include <vector>
#include <string_view>
#include <unordered_map>
#include <memory>

#include "QueryCallback.h"
#include "metadata/LabelSet.h"

#include "PlanGraph.h"

#include "VectorHash.h"

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

class PlanGraphGenerator {
public:
    PlanGraphGenerator(const GraphView& view,
                       QueryCallback callback);
    ~PlanGraphGenerator();

    PlanGraph& getPlanGraph() { return _tree; }

    void generate(const QueryCommand* query);

private:
    using Symbols = std::vector<Symbol*>;
    using LabelNames = std::vector<std::string_view>;

    const GraphView& _view;

    PlanGraph _tree;

    // Cache for label sets
    std::unordered_map<LabelNames,
                       const LabelSet*,
                       VectorHash<std::string_view>> _labelSetCache;
    std::vector<std::unique_ptr<LabelSet>> _labelSets;

    // Map of variable nodes
    std::unordered_map<VarDecl*, PlanGraphNode*> _varNodesMap;

    const LabelSet* getOrCreateLabelSet(const Symbols& symbols);
    const LabelSet* buildLabelSet(const Symbols& symbols);
    LabelID getLabel(const Symbol* symbol);

    void addVarNode(PlanGraphNode* node, VarDecl* varDecl);
    PlanGraphNode* getOrCreateVarNode(VarDecl* varDecl);
    PlanGraphNode* getVarNode(VarDecl* varDecl) const;

    void generateSinglePartQuery(const SinglePartQuery* query);
    void generateStmt(const Stmt* stmt);
    void generateMatchStmt(const MatchStmt* stmt);
    void generatePatternElement(const PatternElement* element);
    PlanGraphNode* generatePatternElementOrigin(const EntityPattern* pattern);
    PlanGraphNode* generatePatternElementExpand(PlanGraphNode* currentNode,
                                                const EntityPattern* edge,
                                                const EntityPattern* target);

    /*
    PlanGraphNode* planPathMatchOrigin(const EntityPattern* pattern);
    PlanGraphNode* planPathExpand(PlanGraphNode* currentNode,
                                  const EntityPattern* edge,
                                  const EntityPattern* target);
    void planCreateTarget(const CreateTarget* target);
    PlanGraphNode* planCreateOrigin(const EntityPattern* pattern);
    PlanGraphNode* planCreateStep(PlanGraphNode* currentNode,
                                  const EntityPattern* edge,
                                  const EntityPattern* target);
    */
};

}
