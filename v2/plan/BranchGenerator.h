#pragma once

#include "QueryCallback.h"

#include "PipelineBuilder.h"
#include "views/GraphView.h"

namespace db {
class LocalMemory;
}

namespace db::v2 {

class SourceManager;
class PlanGraph;
class PipelineBranch;
class PipelineV2;
class PlanGraphNode;
class VarNode;
class VarDecl;
class ScanNodesNode;
class GetOutEdgesNode;
class GetInEdgesNode;
class GetEdgesNode;
class GetEdgeTargetNode;
class GetPropertyNode;
class GetPropertyWithNullNode;
class NodeFilterNode;
class EdgeFilterNode;
class ProduceResultsNode;
class JoinNode;
class SkipNode;
class LimitNode;
class CartesianProductNode;
class AggregateEvalNode;

class BranchGenerator {
public:
    explicit BranchGenerator(LocalMemory* mem,
                             SourceManager* srcMan,
                             const PlanGraph* graph,
                             const GraphView& view,
                             PipelineV2* pipeline,
                             const QueryCallbackV2& callback);

    ~BranchGenerator();

    void translateBranch(PipelineBranch* branch);

private:
    LocalMemory* _mem {nullptr};
    SourceManager* _sourceManager {nullptr};
    const PlanGraph* _graph {nullptr};
    GraphView _view;
    PipelineV2* _pipeline {nullptr};
    QueryCallbackV2 _callback;
    PipelineBuilder _builder;

    // Map of VarDecl to ColumnTag
    std::unordered_map<const VarDecl*, ColumnTag> _declToColumn;

    // Current branch
    PipelineBranch* _currentBranch {nullptr};

    void setupBranch(PipelineBranch* branch);

    PipelineOutputInterface* translateNode(PlanGraphNode* node);
    PipelineOutputInterface* translateVarNode(VarNode* node);
    PipelineOutputInterface* translateScanNodesNode(ScanNodesNode* node);
    PipelineOutputInterface* translateGetOutEdgesNode(GetOutEdgesNode* node);
    PipelineOutputInterface* translateGetInEdgesNode(GetInEdgesNode* node);
    PipelineOutputInterface* translateGetEdgesNode(GetEdgesNode* node);
    PipelineOutputInterface* translateGetEdgeTargetNode(GetEdgeTargetNode* node);
    PipelineOutputInterface* translateGetPropertyWithNullNode(GetPropertyWithNullNode* node);
    PipelineOutputInterface* translateGetPropertyNode(GetPropertyNode* node);
    PipelineOutputInterface* translateNodeFilterNode(NodeFilterNode* node);
    PipelineOutputInterface* translateEdgeFilterNode(EdgeFilterNode* node);
    PipelineOutputInterface* translateProduceResultsNode(ProduceResultsNode* node);
    PipelineOutputInterface* translateJoinNode(JoinNode* node);
    PipelineOutputInterface* translateSkipNode(SkipNode* node);
    PipelineOutputInterface* translateLimitNode(LimitNode* node);
    PipelineOutputInterface* translateCartesianProductNode(CartesianProductNode* node);
    PipelineOutputInterface* translateAggregateEvalNode(AggregateEvalNode* node);
};

}
