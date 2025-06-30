#pragma once

#include "QueryCallback.h"
#include "PlanGraph.h"
#include "metadata/LabelSet.h"
#include "VectorHash.h"

namespace db {

class GraphView;
class QueryCommand;
class MatchCommand;
class MatchTarget;
class EntityPattern;
class PlanGraphNode;
class TypeConstraint;
class VarDecl;
class CreateCommand;
class CreateTarget;
class CreateGraphCommand;

class PlanGraphGenerator {
public:
    PlanGraphGenerator(const GraphView& view,
                       QueryCallback callback);
    ~PlanGraphGenerator();

    const PlanGraph& getPlanGraph() const { return _tree; }

    void generate(const QueryCommand* query);

private:
    using LabelNamePtr = const std::string*;
    using LabelNames = std::vector<LabelNamePtr>;

    const GraphView& _view;

    PlanGraph _tree;

    // Cache for label sets
    std::unordered_map<LabelNames,
                       const LabelSet*,
                       VectorHash<LabelNamePtr>> _labelSetCache;
    std::vector<std::unique_ptr<LabelSet>> _labelSets;

    // Map of variable nodes
    std::unordered_map<VarDecl*, PlanGraphNode*> _varNodesMap;

    const LabelSet* getOrCreateLabelSet(const TypeConstraint* typeConstr);
    const LabelSet* buildLabelSet(const LabelNames& labelNames);
    LabelID getLabel(const std::string& labelName);

    void addVarNode(PlanGraphNode* node, VarDecl* varDecl);
    PlanGraphNode* getOrCreateVarNode(VarDecl* varDecl);
    PlanGraphNode* getVarNode(VarDecl* varDecl) const;

    void planMatchCommand(const MatchCommand* cmd);
    void planMatchTarget(const MatchTarget* target);
    void planCreateCommand(const CreateCommand* cmd);
    void planCreateGraphCommand(const CreateGraphCommand* cmd);

    PlanGraphNode* planPathMatchOrigin(const EntityPattern* pattern);
    PlanGraphNode* planPathExpand(PlanGraphNode* currentNode,
                                  const EntityPattern* edge,
                                  const EntityPattern* target);
    void planCreateTarget(const CreateTarget* target);
    PlanGraphNode* planCreateOrigin(const EntityPattern* pattern);
    PlanGraphNode* planCreateStep(PlanGraphNode* currentNode,
                                  const EntityPattern* edge,
                                  const EntityPattern* target);
};

}
