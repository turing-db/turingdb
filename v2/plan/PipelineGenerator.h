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
    PipelineGenerator(const PlanGraph* graph,
                      const GraphView& view,
                      PipelineV2* pipeline,
                      LocalMemory* mem,
                      SourceManager* sourceManager,
                      const QueryCallbackV2& callback)
        : _graph(graph),
        _view(view),
        _pipeline(pipeline),
        _mem(mem),
        _sourceManager(sourceManager),
        _callback(callback),
        _builder(mem, pipeline)
    {
    }

    ~PipelineGenerator() = default;

    void generate();

    struct BinaryNodeVisitInformation {
        // The OUTPUT of the INPUT to the binary node which is being tracked
        PipelineOutputInterface* visitedInput {nullptr};
        // Whether @ref visitedInput is the LEFT or RIGHT input to the binary node
        bool isLhs {true};
    };

    using BinaryNodeVisitedMap = std::unordered_map<PlanGraphNode*, BinaryNodeVisitInformation>;

private:
    const PlanGraph* _graph {nullptr};
    GraphView _view;
    PipelineV2* _pipeline {nullptr};
    LocalMemory* _mem {nullptr};
    SourceManager* _sourceManager {nullptr};
    QueryCallbackV2 _callback;
    PipelineBuilder _builder;

    std::unordered_map<const VarDecl*, ColumnTag> _declToColumn;

    // [BinaryNode -> Visited input] map
    BinaryNodeVisitedMap _binaryVisitedMap;

    void translateAggregateEvalNode(AggregateEvalNode* node);
    const PipelineOutputInterface* translateNode(PlanGraphNode* node);
    const PipelineOutputInterface* translateVarNode(VarNode* node);
    const PipelineOutputInterface* translateScanNodesNode(ScanNodesNode* node);
    const PipelineOutputInterface* translateGetOutEdgesNode(GetOutEdgesNode* node);
    const PipelineOutputInterface* translateGetInEdgesNode(GetInEdgesNode* node);
    const PipelineOutputInterface* translateGetEdgesNode(GetEdgesNode* node);
    const PipelineOutputInterface* translateGetEdgeTargetNode(GetEdgeTargetNode* node);
    const PipelineOutputInterface* translateGetPropertyWithNullNode(GetPropertyWithNullNode* node);
    const PipelineOutputInterface* translateNodeFilterNode(NodeFilterNode* node);
    const PipelineOutputInterface* translateEdgeFilterNode(EdgeFilterNode* node);
    const PipelineOutputInterface* translateProduceResultsNode(ProduceResultsNode* node);
    const PipelineOutputInterface* translateJoinNode(JoinNode* node);
    const PipelineOutputInterface* translateSkipNode(SkipNode* node);
    const PipelineOutputInterface* translateLimitNode(LimitNode* node);
    const PipelineOutputInterface* translateCartesianProductNode(CartesianProductNode* node);
};

}
