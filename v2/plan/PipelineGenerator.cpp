#include "PipelineGenerator.h"

#include <stack>

#include <spdlog/fmt/fmt.h>

#include "PlanGraph.h"
#include "PlanGraphStream.h"

#include "Projection.h"
#include "nodes/VarNode.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/GetOutEdgesNode.h"
#include "nodes/ProduceResultsNode.h"
#include "nodes/FilterNode.h"
#include "nodes/SkipNode.h"
#include "nodes/LimitNode.h"
#include "nodes/GetEdgeTargetNode.h"
#include "nodes/GetEdgesNode.h"
#include "nodes/GetInEdgesNode.h"

#include "decl/VarDecl.h"
#include "expr/LiteralExpr.h"
#include "Literal.h"

#include "PlannerException.h"

using namespace db::v2;

namespace {

struct TranslateToken {
    PlanGraphNode* node {nullptr};
    PlanGraphStream stream;
};

using TranslateTokenStack = std::stack<TranslateToken>;

}

void PipelineGenerator::generate() {
    TranslateTokenStack nodeStack;

    // Insert root nodes
    std::vector<PlanGraphNode*> rootNodes;
    _graph->getRoots(rootNodes);

    for (const auto& node : rootNodes) {
        nodeStack.emplace(node, PlanGraphStream());
    }

    // Translate nodes in a DFS manner
    while (!nodeStack.empty()) {
        auto [node, stream] = nodeStack.top();
        nodeStack.pop();

        translateNode(node, stream);

        for (PlanGraphNode* nextNode : node->outputs()) {
            nodeStack.emplace(nextNode, stream);
        }
    }
}

void PipelineGenerator::translateNode(PlanGraphNode* node, PlanGraphStream& stream) {
    switch (node->getOpcode()) {
        case PlanGraphOpcode::VAR:
            translateVarNode(static_cast<VarNode*>(node), stream);
        break;

        case PlanGraphOpcode::SCAN_NODES:
            translateScanNodesNode(static_cast<ScanNodesNode*>(node), stream);
        break;

        case PlanGraphOpcode::GET_OUT_EDGES:
            translateGetOutEdgesNode(static_cast<GetOutEdgesNode*>(node), stream);
        break;

        case PlanGraphOpcode::PRODUCE_RESULTS:
            translateProduceResultsNode(static_cast<ProduceResultsNode*>(node), stream);
        break;

        case PlanGraphOpcode::FILTER_NODE:
            translateNodeFilterNode(static_cast<NodeFilterNode*>(node), stream);
        break;

        case PlanGraphOpcode::FILTER_EDGE:
            translateEdgeFilterNode(static_cast<EdgeFilterNode*>(node), stream);
        break;

        case PlanGraphOpcode::SKIP:
            translateSkipNode(static_cast<SkipNode*>(node), stream);
        break;

        case PlanGraphOpcode::LIMIT:
            translateLimitNode(static_cast<LimitNode*>(node), stream);
        break;

        case PlanGraphOpcode::GET_EDGE_TARGET:
            translateGetEdgeTargetNode(static_cast<GetEdgeTargetNode*>(node), stream);
        break;

        case PlanGraphOpcode::GET_IN_EDGES:
            translateGetInEdgesNode(static_cast<GetInEdgesNode*>(node), stream);
        break;

        case PlanGraphOpcode::GET_EDGES:
            translateGetEdgesNode(static_cast<GetEdgesNode*>(node), stream);
        break;

        case PlanGraphOpcode::GET_PROPERTY:
        case PlanGraphOpcode::GET_PROPERTY_WITH_NULL:
        case PlanGraphOpcode::GET_ENTITY_TYPE:
        case PlanGraphOpcode::JOIN:
        case PlanGraphOpcode::CREATE_GRAPH:
        case PlanGraphOpcode::PROJECT_RESULTS:
        case PlanGraphOpcode::CARTESIAN_PRODUCT:
        case PlanGraphOpcode::WRITE:
        case PlanGraphOpcode::FUNC_EVAL:
        case PlanGraphOpcode::AGGREGATE_EVAL:
        case PlanGraphOpcode::ORDER_BY:
        case PlanGraphOpcode::UNKNOWN:
        case PlanGraphOpcode::_SIZE:
            throw PlannerException(fmt::format("PipelineGenerator does not support PlanGraphNode: {}",
                PlanGraphOpcodeDescription::value(node->getOpcode())));
    }
}

void PipelineGenerator::translateVarNode(VarNode* node, PlanGraphStream& stream) {
    const std::string_view varName = node->getVarDecl()->getName();
    if (varName.empty()) {
        throw PlannerException("VarNode with empty name");
    }

    _builder.rename(varName);

    const PipelineOutputInterface* output = _builder.getOutput();

    if (const auto* out = dynamic_cast<const PipelineNodeOutputInterface*>(output)) {
        _declToColumn[node->getVarDecl()] = out->getNodeIDs()->getTag();
    } else if (const auto* out = dynamic_cast<const PipelineEdgeOutputInterface*>(output)) {
        const PendingOutputView& outView = _builder.getOutputView();

        if (outView.isEdgesProjectedOnOtherIDs()) {
            _declToColumn[node->getVarDecl()] = out->getOtherNodes()->getTag();
        } else {
            _declToColumn[node->getVarDecl()] = out->getEdgeIDs()->getTag();
        }
    } else {
        throw PlannerException("VarNode output is not a node or edge");
    }
}

void PipelineGenerator::translateScanNodesNode(ScanNodesNode* node, PlanGraphStream& stream) {
    _builder.addScanNodes();
}

void PipelineGenerator::translateGetOutEdgesNode(GetOutEdgesNode* node, PlanGraphStream& stream) {
    _builder.addGetOutEdges();
}

void PipelineGenerator::translateGetInEdgesNode(GetInEdgesNode* node, PlanGraphStream& stream) {
    _builder.addGetInEdges();
}

void PipelineGenerator::translateGetEdgesNode(GetEdgesNode* node, PlanGraphStream& stream) {
    _builder.addGetEdges();
}

void PipelineGenerator::translateGetEdgeTargetNode(GetEdgeTargetNode* node, PlanGraphStream& stream) {
    _builder.projectEdgesOnOtherIDs();
}

void PipelineGenerator::translateNodeFilterNode(NodeFilterNode* node, PlanGraphStream& stream) {
}

void PipelineGenerator::translateEdgeFilterNode(EdgeFilterNode* node, PlanGraphStream& stream) {
}

void PipelineGenerator::translateProduceResultsNode(ProduceResultsNode* node, PlanGraphStream& stream) {
    // If MaterializeNode has not been seen at that point,
    // we materialize if we have data in flight
    if (_builder.isMaterializeOpen() && !_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    _builder.closeMaterialize();
    const Projection* projNode = node->getProjection();

    std::vector<ColumnTag> tags;

    for (const Expr* item : projNode->items()) {
        const VarDecl* decl = item->getExprVarDecl();

        if (!decl) {
            throw PlannerException("Projection item does not have a variable declaration");
        }

        const ColumnTag tag = _declToColumn.at(decl);

        tags.push_back(tag);
    }

    _builder.addProjection(tags);

    auto lambdaCallback = [this](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        _callback(df);
    };

    _builder.addLambda(lambdaCallback);
}

void PipelineGenerator::translateJoinNode(JoinNode* node, PlanGraphStream& stream) {
}

void PipelineGenerator::translateSkipNode(SkipNode* node, PlanGraphStream& stream) {
    // If MaterializeNode has not been seen at that point,
    // we materialize if we have data in flight
    if (_builder.isMaterializeOpen() && !_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const Expr* skipExpr = node->getExpr();
    const LiteralExpr* literalExpr = dynamic_cast<const LiteralExpr*>(skipExpr);
    if (!literalExpr) {
        throw PlannerException("Skip expression must be a literal");
    }

    const IntegerLiteral* integerLiteral = dynamic_cast<const IntegerLiteral*>(literalExpr->getLiteral());
    if (!integerLiteral) {
        throw PlannerException("Skip expression must be an integer");
    }

    if (integerLiteral->getValue() < 0) {
        throw PlannerException("Skip expression must be a positive integer");
    }

    _builder.addSkip(static_cast<size_t>(integerLiteral->getValue()));
}

void PipelineGenerator::translateLimitNode(LimitNode* node, PlanGraphStream& stream) {
    // If MaterializeNode has not been seen at that point,
    // we materialize if we have data in flight
    if (_builder.isMaterializeOpen() && !_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const Expr* limitExpr = node->getExpr();
    const LiteralExpr* literalExpr = dynamic_cast<const LiteralExpr*>(limitExpr);
    if (!literalExpr) {
        throw PlannerException("Limit expression must be a literal");
    }

    const IntegerLiteral* integerLiteral = dynamic_cast<const IntegerLiteral*>(literalExpr->getLiteral());
    if (!integerLiteral) {
        throw PlannerException("Limit expression must be an integer");
    }

    if (integerLiteral->getValue() < 0) {
        throw PlannerException("Limit expression must be a positive integer");
    }

    _builder.addLimit(static_cast<size_t>(integerLiteral->getValue()));
}
