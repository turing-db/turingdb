#pragma once

#include "QueryCallbacks.h"

#include <string>
#include <vector>

#include "PipelineBuilder.h"
#include "views/GraphView.h"

namespace db {

class LocalMemory;
class SystemManager;
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
class ExprEvalNode;
class WriteNode;
class ScanNodesByLabelNode;
class LoadGraphNode;
class LoadJsonlNode;
class ChangeNode;
class ListGraphNode;
class CreateGraphNode;
class LoadGMLNode;
class S3ConnectNode;
class S3TransferNode;
class ShowProceduresNode;
class ShortestPathNode;
class CommitNode;
class LoadCSVNode;
class CreateVectorIndexNode;
class LoadVectorNode;
class VectorSearchNode;
class DeleteVectorIndexNode;
class ShowVectorIndexesNode;
class OrderByNode;
class LoadCommitNode;

class PipelineGenerator {
public:
    PipelineGenerator(const PlanGraph* graph,
                      const GraphView& view,
                      PipelineV2* pipeline,
                      LocalMemory* mem,
                      const SystemManager* sysMan,
                      const ProcedureManager* procedures,
                      const QueryCallbacks* callbacks)
        : _graph(graph),
        _procedures(procedures),
        _view(view),
        _pipeline(pipeline),
        _mem(mem),
        _sysMan(sysMan),
        _callbacks(callbacks),
        _builder(mem, pipeline)
    {
    }

    ~PipelineGenerator() = default;

    void generate();

    struct BinaryNodeVisitInformation {
        // The OUTPUT of the INPUT to the binary node which is being tracked
        PipelineOutputInterface* _visitedInput {nullptr};
        // Whether @ref _visitedInput is the LEFT or RIGHT input to the binary node
        bool _isLhs {true};
    };

    using BinaryNodeVisitedMap = std::unordered_map<PlanGraphNode*, BinaryNodeVisitInformation>;
    using VarColumnMap = std::unordered_map<const VarDecl*, ColumnTag>;

    const VarColumnMap& varColMap() const { return _declToColumn; }
    LocalMemory& memory() { return *_mem; }
    GraphView view() { return _view; }
    const std::vector<std::string>& getCSVHeaders() const {
        return _csvHeaders;
    }

private:
    const PlanGraph* _graph {nullptr};
    const ProcedureManager* _procedures {nullptr};
    GraphView _view;
    PipelineV2* _pipeline {nullptr};
    LocalMemory* _mem {nullptr};
    const SystemManager* _sysMan {nullptr};
    const QueryCallbacks* _callbacks {nullptr};
    PipelineBuilder _builder;

    VarColumnMap _declToColumn;

    ColumnTag getCol(const VarDecl* var);

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
    PipelineOutputInterface* translateExprEvalNode(ExprEvalNode* node);
    PipelineOutputInterface* translateWriteNode(WriteNode* node);
    PipelineOutputInterface* translateScanNodesByLabelNode(ScanNodesByLabelNode* node);
    PipelineOutputInterface* translateLoadGraph(LoadGraphNode* node);
    PipelineOutputInterface* translateLoadJsonl(LoadJsonlNode* node);
    PipelineOutputInterface* translateChangeNode(ChangeNode* node);
    PipelineOutputInterface* translateCommitNode(CommitNode* node);
    PipelineOutputInterface* translateListGraphNode(ListGraphNode* node);
    PipelineOutputInterface* translateCreateGraphNode(CreateGraphNode* node);
    PipelineOutputInterface* translateLoadGML(LoadGMLNode* node);
    PipelineOutputInterface* translateS3ConnectNode(S3ConnectNode* node);
    PipelineOutputInterface* translateS3TransferNode(S3TransferNode* node);
    PipelineOutputInterface* translateShowProceduresNode(ShowProceduresNode* node);
    PipelineOutputInterface* translateShortestPathNode(ShortestPathNode* node);
    PipelineOutputInterface* translateLoadCSVNode(LoadCSVNode* node);
    PipelineOutputInterface* translateCreateVectorIndexNode(CreateVectorIndexNode* node);
    PipelineOutputInterface* translateLoadVectorNode(LoadVectorNode* node);
    PipelineOutputInterface* translateVectorSearchNode(VectorSearchNode* node);
    PipelineOutputInterface* translateDeleteVectorIndexNode(DeleteVectorIndexNode* node);
    PipelineOutputInterface* translateShowVectorIndexesNode(ShowVectorIndexesNode* node);
    PipelineOutputInterface* translateOrderByNode(OrderByNode* node);
    PipelineOutputInterface* translateLoadCommit(LoadCommitNode* node);

    std::vector<std::string> _csvHeaders;
    size_t _csvFieldCount {0};
};
}
