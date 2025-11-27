#pragma once

#include "QueryCallback.h"

#include "PipelineBuilder.h"
#include "views/GraphView.h"

namespace db {
class LocalMemory;
}

namespace db::v2 {

class SourceManager;
class ProcedureBlueprintMap;
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
class ProcedureEvalNode;

class PipelineGenerator {
public:
    PipelineGenerator(const PlanGraph* graph,
                      const GraphView& view,
                      PipelineV2* pipeline,
                      LocalMemory* mem,
                      SourceManager* sourceManager,
                      const ProcedureBlueprintMap& blueprints,
                      const QueryCallbackV2& callback)
        : _graph(graph),
        _blueprints(&blueprints),
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
    const ProcedureBlueprintMap* _blueprints {nullptr};
    GraphView _view;
    PipelineV2* _pipeline {nullptr};
    LocalMemory* _mem {nullptr};
    SourceManager* _sourceManager {nullptr};
    QueryCallbackV2 _callback;
    PipelineBuilder _builder;

    std::unordered_map<const VarDecl*, ColumnTag> _declToColumn;

    // [BinaryNode -> Visited input] map
    BinaryNodeVisitedMap _binaryVisitedMap;

    PipelineOutputInterface* translateNode(PlanGraphNode* node);
    PipelineOutputInterface* translateVarNode(VarNode* node);
    PipelineOutputInterface* translateScanNodesNode(ScanNodesNode* node);
    PipelineOutputInterface* translateGetOutEdgesNode(GetOutEdgesNode* node);
    PipelineOutputInterface* translateGetInEdgesNode(GetInEdgesNode* node);
    PipelineOutputInterface* translateGetEdgesNode(GetEdgesNode* node);
    PipelineOutputInterface* translateGetEdgeTargetNode(GetEdgeTargetNode* node);
    PipelineOutputInterface* translateGetPropertyNode(GetPropertyNode* node);
    PipelineOutputInterface* translateGetPropertyWithNullNode(GetPropertyWithNullNode* node);
    PipelineOutputInterface* translateNodeFilterNode(NodeFilterNode* node);
    PipelineOutputInterface* translateEdgeFilterNode(EdgeFilterNode* node);
    PipelineOutputInterface* translateProduceResultsNode(ProduceResultsNode* node);
    PipelineOutputInterface* translateJoinNode(JoinNode* node);
    PipelineOutputInterface* translateSkipNode(SkipNode* node);
    PipelineOutputInterface* translateLimitNode(LimitNode* node);
    PipelineOutputInterface* translateCartesianProductNode(CartesianProductNode* node);
    PipelineOutputInterface* translateAggregateEvalNode(AggregateEvalNode* node);
    PipelineOutputInterface* translateProcedureEvalNode(ProcedureEvalNode* node);
};
}
