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

class PipelineGenerator {
public:
    PipelineGenerator(LocalMemory* mem,
                      SourceManager* srcMan,
                      const PlanGraph* graph,
                      const GraphView& view,
                      PipelineV2* pipeline,
                      const QueryCallbackV2& callback)
        : _mem(mem),
        _sourceManager(srcMan),
        _graph(graph),
        _view(view),
        _pipeline(pipeline),
        _callback(callback),
        _builder(mem, pipeline)
    {
    }

    ~PipelineGenerator();

    void generate();

private:
    LocalMemory* _mem {nullptr};
    SourceManager* _sourceManager {nullptr};
    const PlanGraph* _graph {nullptr};
    GraphView _view;
    PipelineV2* _pipeline {nullptr};
    QueryCallbackV2 _callback;
    PipelineBuilder _builder;

    std::unordered_map<const VarDecl*, ColumnTag> _declToColumn;

    PipelineOutputInterface* translateNode(PlanGraphNode* node);
    PipelineOutputInterface* translateVarNode(VarNode* node);
    PipelineOutputInterface* translateScanNodesNode(ScanNodesNode* node);
    PipelineOutputInterface* translateGetOutEdgesNode(GetOutEdgesNode* node);
    PipelineOutputInterface* translateGetInEdgesNode(GetInEdgesNode* node);
    PipelineOutputInterface* translateGetEdgesNode(GetEdgesNode* node);
    PipelineOutputInterface* translateGetEdgeTargetNode(GetEdgeTargetNode* node);
    PipelineOutputInterface* translateGetPropertyWithNullNode(GetPropertyWithNullNode* node);
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
