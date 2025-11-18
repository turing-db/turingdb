#pragma once

#include "PipelineInterface.h"
#include "QueryCallback.h"

#include "PipelineBuilder.h"

namespace db {
class LocalMemory;
}

namespace db::v2 {

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
class NodeFilterNode;
class EdgeFilterNode;
class ProduceResultsNode;
class JoinNode;
class SkipNode;
class LimitNode;
class CartesianProductNode;

class PipelineGenerator {
public:
    PipelineGenerator(const PlanGraph* graph,
                      PipelineV2* pipeline,
                      LocalMemory* mem,
                      const QueryCallbackV2& callback)
        : _graph(graph),
        _pipeline(pipeline),
        _mem(mem),
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
    PipelineV2* _pipeline {nullptr};
    LocalMemory* _mem {nullptr};
    QueryCallbackV2 _callback;
    PipelineBuilder _builder;

    std::unordered_map<const VarDecl*, ColumnTag> _declToColumn;

    // [BinaryNode -> Visited input] map
    BinaryNodeVisitedMap _binaryVisitedMap;

    void translateNode(PlanGraphNode* node);
    void translateVarNode(VarNode* node);
    void translateScanNodesNode(ScanNodesNode* node);
    void translateGetOutEdgesNode(GetOutEdgesNode* node);
    void translateGetInEdgesNode(GetInEdgesNode* node);
    void translateGetEdgesNode(GetEdgesNode* node);
    void translateGetEdgeTargetNode(GetEdgeTargetNode* node);
    void translateNodeFilterNode(NodeFilterNode* node);
    void translateEdgeFilterNode(EdgeFilterNode* node);
    void translateProduceResultsNode(ProduceResultsNode* node);
    void translateJoinNode(JoinNode* node);
    void translateSkipNode(SkipNode* node);
    void translateLimitNode(LimitNode* node);
    void translateCartesianProductNode(CartesianProductNode* node);
};

}
