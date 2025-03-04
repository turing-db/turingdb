#pragma once

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>

#include "columns/ColumnIDs.h"
#include "labels/LabelSet.h"
#include "VectorHash.h"
#include "QueryCallback.h"

namespace db {

class QueryCommand;
class GraphView;
class LocalMemory;
class SelectCommand;
class Pipeline;
class EntityPattern;
class TypeConstraint;
class TransformData;
class CreateGraphCommand;
class ListGraphCommand;
class LoadGraphCommand;
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

    // Planning functions
    bool planSelect(const SelectCommand* select);
    void planPath(const std::vector<EntityPattern*>& path);
    void planScanNodes(const EntityPattern* entity);
    void planExpandEdge(const EntityPattern* edge,
                        const EntityPattern* target);
    void planExpandEdgeWithNoConstraint(const EntityPattern* edge,
                                        const EntityPattern* target);
    void planExpandEdgeWithTargetConstraint(const EntityPattern* edge,
                                            const EntityPattern* target);
    void planExpandEdgeWithEdgeConstraint(const EntityPattern* edge,
                                          const EntityPattern* target);
    void planExpandEdgeWithEdgeAndTargetConstraint(const EntityPattern* edge,
                                                   const EntityPattern* target);

    void planTransformStep();
    void planPathUsingScanEdges(const std::vector<EntityPattern*>& path);
    void planScanEdges(const EntityPattern* source,
                       const EntityPattern* edge,
                       const EntityPattern* target);
    
    bool planCreateGraph(const CreateGraphCommand* createCmd);
    bool planListGraph(const ListGraphCommand* listCmd);
    bool planLoadGraph(const LoadGraphCommand* loadCmd);
    void planProjection(const SelectCommand* select);
    void planPropertyProjection(ColumnIDs* columnIDs,
                                const std::string& memberName);
    void planOutputLambda();
};

}
