#include "PipelineGenerator.h"

#include <stack>

#include <spdlog/fmt/fmt.h>

#include "PendingOutputView.h"
#include "PipelineInterface.h"
#include "PlanGraph.h"

#include "Projection.h"
#include "nodes/CartesianProductNode.h"
#include "nodes/PlanGraphNode.h"
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

#include "PipelineException.h"
#include "PlannerException.h"
#include "FatalException.h"

using namespace db::v2;

namespace {

using TranslateTokenStack = std::stack<PlanGraphNode*>;

}

void PipelineGenerator::generate() {
    TranslateTokenStack nodeStack;

    // Insert root nodes
    std::vector<PlanGraphNode*> rootNodes;
    _graph->getRoots(rootNodes);

    for (const auto& node : rootNodes) {
        nodeStack.push(node);
    }

    // Translate nodes in a DFS manner
    while (!nodeStack.empty()) {
        PlanGraphNode* node = nodeStack.top();
        nodeStack.pop();

        translateNode(node);

        for (PlanGraphNode* nextNode : node->outputs()) {
            if (!nextNode->isBinary()) {
                nodeStack.push(nextNode);
                continue;
            }

            // Binary node case
            // Cartesian Product and Join require their inputs to be materialised
            if (_builder.isMaterializeOpen()) {
                throw PipelineException("Attempted to add input to binary node "
                                        "whilst Materialize is open.");
            }

            const bool visited = _binaryVisitedMap.contains(nextNode);
            if (visited) {
                nodeStack.push(nextNode);
                continue;
            }

            const PendingOutputView& pendingOutput = _builder.getPendingOutput();

            const PlanGraphNode::Nodes& binaryNodeInputs = nextNode->inputs();
            const bool isLhs = (node == binaryNodeInputs.front());
            bioassert((node == binaryNodeInputs.front()) || (node == binaryNodeInputs.back()));
            const BinaryNodeVisitInformation info {pendingOutput.getInterface(), isLhs};

            _binaryVisitedMap.emplace(nextNode, info);
        }
    }
}

void PipelineGenerator::translateNode(PlanGraphNode* node) {
    switch (node->getOpcode()) {
        case PlanGraphOpcode::VAR:
            translateVarNode(static_cast<VarNode*>(node));
        break;

        case PlanGraphOpcode::SCAN_NODES:
            translateScanNodesNode(static_cast<ScanNodesNode*>(node));
        break;

        case PlanGraphOpcode::GET_OUT_EDGES:

            translateGetOutEdgesNode(static_cast<GetOutEdgesNode*>(node));
        break;

        case PlanGraphOpcode::PRODUCE_RESULTS:
            translateProduceResultsNode(static_cast<ProduceResultsNode*>(node));
        break;

        case PlanGraphOpcode::FILTER_NODE:
            translateNodeFilterNode(static_cast<NodeFilterNode*>(node));
        break;

        case PlanGraphOpcode::FILTER_EDGE:
            translateEdgeFilterNode(static_cast<EdgeFilterNode*>(node));
        break;

        case PlanGraphOpcode::SKIP:
            translateSkipNode(static_cast<SkipNode*>(node));
        break;

        case PlanGraphOpcode::LIMIT:
            translateLimitNode(static_cast<LimitNode*>(node));
        break;

        case PlanGraphOpcode::GET_EDGE_TARGET:
            translateGetEdgeTargetNode(static_cast<GetEdgeTargetNode*>(node));
        break;

        case PlanGraphOpcode::GET_IN_EDGES:
            translateGetInEdgesNode(static_cast<GetInEdgesNode*>(node));
        break;

        case PlanGraphOpcode::GET_EDGES:
            translateGetEdgesNode(static_cast<GetEdgesNode*>(node));
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

void PipelineGenerator::translateVarNode(VarNode* node) {
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

void PipelineGenerator::translateScanNodesNode(ScanNodesNode* node) {
    _builder.addScanNodes();
}

void PipelineGenerator::translateGetOutEdgesNode(GetOutEdgesNode* node) {
    _builder.addGetOutEdges();
}

void PipelineGenerator::translateGetInEdgesNode(GetInEdgesNode* node) {
    _builder.addGetInEdges();
}

void PipelineGenerator::translateGetEdgesNode(GetEdgesNode* node) {
    _builder.addGetEdges();
}

void PipelineGenerator::translateGetEdgeTargetNode(GetEdgeTargetNode* node) {
    _builder.projectEdgesOnOtherIDs();
}

void PipelineGenerator::translateNodeFilterNode(NodeFilterNode* node) {
}

void PipelineGenerator::translateEdgeFilterNode(EdgeFilterNode* node) {
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

void PipelineGenerator::translateJoinNode(JoinNode* node) {
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

void PipelineGenerator::translateCartesianProductNode(CartesianProductNode* node) {
    if (!_binaryVisitedMap.contains(node)) {
        throw PipelineException("Attempted to translate CartesianProductNode which was "
                                "not already encountered.");
    }

    PipelineOutputInterface* inputA = _builder.getPendingOutputInterface();
    auto& [inputB, isBLhs] = _binaryVisitedMap.at(node);

    PipelineOutputInterface* lhs = isBLhs ? inputB : inputA;
    [[maybe_unused]] PipelineOutputInterface* rhs = isBLhs ? inputA : inputB;

    // LHS is implicit in @ref _pendingOutput
    _builder.getPendingOutput().setInterface(lhs);

    throw FatalException("CartesianProduct not yet implemented");
}
