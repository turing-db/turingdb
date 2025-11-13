#pragma once

#include "QueryCallback.h"

#include "PipelineBuilder.h"

namespace db {
class LocalMemory;
}

namespace db::v2 {

class PlanGraph;
class PipelineV2;
class PlanGraphNode;
class PlanGraphStream;
class VarNode;
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

private:
    const PlanGraph* _graph {nullptr};
    PipelineV2* _pipeline {nullptr};
    LocalMemory* _mem {nullptr};
    QueryCallbackV2 _callback;
    PipelineBuilder _builder;

    void translateNode(PlanGraphNode* node, PlanGraphStream& stream);
    void translateVarNode(VarNode* node, PlanGraphStream& stream);
    void translateScanNodesNode(ScanNodesNode* node, PlanGraphStream& stream);
    void translateGetOutEdgesNode(GetOutEdgesNode* node, PlanGraphStream& stream);
    void translateGetInEdgesNode(GetInEdgesNode* node, PlanGraphStream& stream);
    void translateGetEdgesNode(GetEdgesNode* node, PlanGraphStream& stream);
    void translateGetEdgeTargetNode(GetEdgeTargetNode* node, PlanGraphStream& stream);
    void translateNodeFilterNode(NodeFilterNode* node, PlanGraphStream& stream);
    void translateEdgeFilterNode(EdgeFilterNode* node, PlanGraphStream& stream);
    void translateProduceResultsNode(ProduceResultsNode* node, PlanGraphStream& stream);
    void translateJoinNode(JoinNode* node, PlanGraphStream& stream);
    void translateSkipNode(SkipNode* node, PlanGraphStream& stream);
    void translateLimitNode(LimitNode* node, PlanGraphStream& stream);
};

}
