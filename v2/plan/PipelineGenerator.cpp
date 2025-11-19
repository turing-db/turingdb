#include "PipelineGenerator.h"

#include <stack>

#include <spdlog/fmt/fmt.h>

#include "PendingOutputView.h"
#include "PlanGraph.h"
#include "reader/GraphReader.h"
#include "SourceManager.h"

#include "nodes/CartesianProductNode.h"
#include "nodes/PlanGraphNode.h"
#include "nodes/GetPropertyWithNullNode.h"
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

#include "Projection.h"
#include "decl/VarDecl.h"
#include "expr/LiteralExpr.h"
#include "Literal.h"

#include "PipelineException.h"
#include "PlannerException.h"

using namespace db::v2;

namespace {

using TranslateTokenStack = std::stack<PlanGraphNode*>;

struct PropertyTypeDispatcher {
    db::ValueType _valueType;

    void execute(const auto& executor) const {
        switch (_valueType) {
            case db::ValueType::Int64:
                executor.template operator()<db::types::Int64>();
                break;
            case db::ValueType::UInt64:
                executor.template operator()<db::types::UInt64>();
                break;
            case db::ValueType::Double:
                executor.template operator()<db::types::Double>();
                break;
            case db::ValueType::String:
                executor.template operator()<db::types::String>();
                break;
            case db::ValueType::Bool:
                executor.template operator()<db::types::Bool>();
                break;
            case db::ValueType::_SIZE:
            case db::ValueType::Invalid: {
                throw PlannerException("Unsupported property type");
            }
        }
    }
};

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
            // Unary node case
            if (!nextNode->isBinary()) {
                nodeStack.push(nextNode);
                continue;
            }
            // Binary node case

            // TODO: Add a `needsMaterialised` check based on PlanNode - some binary nodes
            // may not need their inputs materialised perhaps?
            if (_builder.isMaterializeOpen() && !_builder.isSingleMaterializeStep()) {
                _builder.addMaterialize();
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

        case PlanGraphOpcode::CARTESIAN_PRODUCT:
            translateCartesianProductNode(static_cast<CartesianProductNode*>(node));
        break;

        case PlanGraphOpcode::GET_PROPERTY:
        case PlanGraphOpcode::GET_PROPERTY_WITH_NULL:
            translateGetPropertyWithNullNode(static_cast<GetPropertyWithNullNode*>(node));
        break;
        case PlanGraphOpcode::GET_ENTITY_TYPE:
        case PlanGraphOpcode::JOIN:
        case PlanGraphOpcode::CREATE_GRAPH:
        case PlanGraphOpcode::PROJECT_RESULTS:
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

    const PipelineOutputInterface* output = _builder.getPendingOutputInterface();

    if (const auto* out = dynamic_cast<const PipelineNodeOutputInterface*>(output)) {
        _declToColumn[node->getVarDecl()] = out->getNodeIDs()->getTag();
    } else if (const auto* out = dynamic_cast<const PipelineEdgeOutputInterface*>(output)) {
        const PendingOutputView& outView = _builder.getPendingOutput();

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

void PipelineGenerator::translateGetPropertyWithNullNode(GetPropertyWithNullNode* node) {
    if (_builder.isMaterializeOpen() && !_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const VarDecl* entityDecl = node->getEntityVarDecl();
    if (!entityDecl) {
        throw PlannerException("GetPropertyWithNullNode does not have an entity variable declaration");
    }

    ColumnTag entityTag = _declToColumn.at(entityDecl);

    const std::string propName {node->getPropName()};

    PipelineValuesOutputInterface* output = nullptr;

    const std::optional<PropertyType> foundProp = _view.read().getMetadata().propTypes().get(propName);
    if (!foundProp) {
        throw PlannerException(fmt::format("Property type {} does not exist", propName));
    }

    if (entityDecl->getType() == EvaluatedType::NodePattern) {
        const auto process = [&]<SupportedType Type> {
            output = &_builder.addGetPropertiesWithNull<EntityType::Node, Type>(entityTag, *foundProp);
        };

        PropertyTypeDispatcher {foundProp->_valueType}.execute(process);
    } else if (entityDecl->getType() == EvaluatedType::EdgePattern) {
        const auto process = [&]<SupportedType Type> {
            output = &_builder.addGetPropertiesWithNull<EntityType::Edge, Type>(entityTag, *foundProp);
        };

        PropertyTypeDispatcher {foundProp->_valueType}.execute(process);
    } else {
        throw PlannerException(fmt::format(
            "GetPropertyWithNull must act on a Node/EdgePattern. Instead acting on {}",
            EvaluatedTypeName::value(entityDecl->getType())));
    }

    const Expr* expr = node->getExpr();
    if (!expr) {
        throw PlannerException("GetPropertyWithNullNode does not have an expression");
    }

    const VarDecl* exprDecl = expr->getExprVarDecl();
    if (!exprDecl) {
        throw PlannerException("GetPropertyWithNullNode does not have an expression variable declaration");
    }

    const std::string_view exprStrRep = _sourceManager->getStringRepr((std::uintptr_t)expr);
    if (exprStrRep.empty()) {
        throw PlannerException("Unknown error. Could not get string representation of expression");
    }

    output->rename(exprStrRep);

    _declToColumn[exprDecl] = output->getValues()->getTag();
}

void PipelineGenerator::translateNodeFilterNode(NodeFilterNode* node) {
    if (!node->isEmpty()) {
        throw PlannerException("PipelineGenerator does not support non-empty NodeFilterNode.");
    }
}

void PipelineGenerator::translateEdgeFilterNode(EdgeFilterNode* node) {
    if (!node->isEmpty()) {
        throw PlannerException("PipelineGenerator does not support non-empty NodeFilterNode.");
    }
}

void PipelineGenerator::translateProduceResultsNode(ProduceResultsNode* node) {
    // If MaterializeNode has not been seen at that point,
    // we materialize if we have data in flight
    if (_builder.isMaterializeOpen() && !_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

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

void PipelineGenerator::translateSkipNode(SkipNode* node) {
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

void PipelineGenerator::translateLimitNode(LimitNode* node) {
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
    PipelineOutputInterface* rhs = isBLhs ? inputA : inputB;

    // LHS is implicit in @ref _pendingOutput
    _builder.getPendingOutput().setInterface(lhs);

    _builder.addCartesianProduct(rhs);
}
