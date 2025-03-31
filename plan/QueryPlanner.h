#pragma once

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>

#include "ExprConstraint.h"
#include "QueryCommand.h"
#include "columns/ColumnIDs.h"
#include "labels/LabelSet.h"
#include "VectorHash.h"
#include "QueryCallback.h"

namespace db {

class QueryCommand;
class GraphView;
class LocalMemory;
class ReturnCommand;
class Pipeline;
class EntityPattern;
class TypeConstraint;
class TransformData;
class CreateGraphCommand;
class ListGraphCommand;
class LoadGraphCommand;
class ExplainCommand;
class VarDecl;
class Block;

class QueryPlanner {
public:
    QueryPlanner(const GraphView& view,
                 LocalMemory* mem,
                 QueryCallback callback);

    ~QueryPlanner();

    bool plan(const QueryCommand* query);

    Pipeline* getPipeline() const { return _pipeline.get(); }

private:
    const GraphView& _view;
    LocalMemory* _mem {nullptr};
    QueryCallback _queryCallback;
    std::unique_ptr<Pipeline> _pipeline;

    // Code generation utilities
    std::unique_ptr<Block> _output;
    ColumnIDs* _result {nullptr};
    std::unique_ptr<TransformData> _transformData;

    // Temporary containers
    std::vector<LabelSetID> _tmpLabelSetIDs;

    // LabelSets
    using LabelNamePtr = const std::string*;
    using LabelNames = std::vector<LabelNamePtr>;
    std::vector<LabelSet*> _labelSets;
    std::unordered_map<LabelNames,
                       LabelSet*,
                       VectorHash<LabelNamePtr>,
                       VectorHash<LabelNamePtr>::Equal> _labelSetCache;
    LabelSet* getLabelSet(const TypeConstraint* typeConstr);
    LabelSet* buildLabelSet(const LabelNames& labelNames);
    LabelID getLabel(const std::string& labelName);
    void getMatchingLabelSets(std::vector<LabelSetID>& labelSets,
                              const LabelSet* targetLabelSet);

    // Property Functions
    void generateNodePropertyFilterMasks(std::vector<ColumnMask*> filterMasks,
                                         std::span<const BinExpr* const> expressions,
                                         const ColumnIDs* entities);
    void generateEdgePropertyFilterMasks(std::vector<ColumnMask*> filterMasks,
                                         std::span<const BinExpr* const> expressions,
                                         const ColumnIDs* entities);

    // Planning functions
    bool planMatch(const MatchCommand* matchCmd);
    void planPath(const std::vector<EntityPattern*>& path);

    void planScanNodes(const EntityPattern* entity);
    void planScanNodesWithPropertyConstraints(ColumnIDs* const& outputNodes,
                                              const ExprConstraint* exprConstraint);
    void planScanNodesWithPropertyAndLabelConstraints(ColumnIDs* const& outputNodes,
                                                      const LabelSet* labelSet,
                                                      const ExprConstraint* exprConstraint);

    void planExpandEdge(const EntityPattern* edge, const EntityPattern* target);
    void planExpandEdgeWithNoConstraint(const EntityPattern* edge,
                                        const EntityPattern* target);
    void planExpandEdgeWithTargetConstraint(const EntityPattern* edge,
                                            const EntityPattern* target);
    void planExpandEdgeWithEdgeConstraint(const EntityPattern* edge,
                                          const EntityPattern* target);
    void planExpandEdgeWithEdgeAndTargetConstraint(const EntityPattern* edge,
                                                   const EntityPattern* target);

    void planExpressionConstraintFilters(const ExprConstraint* edgeExprConstr,
                                         const ExprConstraint* targetExprConstr,
                                         const ColumnIDs* edges,
                                         const ColumnIDs* targetNodes,
                                         VarDecl* edgeDecl,
                                         VarDecl* targetDecl,
                                         bool mustWriteEdges,
                                         bool mustWriteTargetNodes);

    void planTransformStep();
    void planPathUsingScanEdges(const std::vector<EntityPattern*>& path);
    void planScanEdges(const EntityPattern* source,
                       const EntityPattern* edge,
                       const EntityPattern* target);

    bool planCreateGraph(const CreateGraphCommand* createCmd);
    bool planListGraph(const ListGraphCommand* listCmd);
    bool planLoadGraph(const LoadGraphCommand* loadCmd);
    void planProjection(const MatchCommand* matchCmd);
    void planPropertyProjection(ColumnIDs* columnIDs,
                                const VarDecl* parentDecl,
                                const std::string& memberName);
    void planOutputLambda();
    bool planExplain(const ExplainCommand* explain);
};
}
