#include "BranchGenerator.h"

#include <spdlog/fmt/fmt.h>

#include "PlanGraph.h"
#include "PipelineBranch.h"
#include "PendingOutputView.h"
#include "interfaces/PipelineNodeOutputInterface.h"
#include "interfaces/PipelineOutputInterface.h"
#include "reader/GraphReader.h"
#include "SourceManager.h"

#include "nodes/AggregateEvalNode.h"
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
#include "FunctionInvocation.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/ExprChain.h"

#include "PipelineException.h"
#include "PlannerException.h"
#include "FatalException.h"

using namespace db::v2;
using namespace db;

namespace {

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

BranchGenerator::BranchGenerator(LocalMemory* mem,
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

BranchGenerator::~BranchGenerator() {
}

void BranchGenerator::translateBranch(const PipelineBranch* branch) {
    for (PlanGraphNode* node : branch->nodes()) {
        translateNode(node);
    }
}

PipelineOutputInterface* BranchGenerator::translateNode(PlanGraphNode* node) {
    switch (node->getOpcode()) {
        case PlanGraphOpcode::VAR:
            return translateVarNode(static_cast<VarNode*>(node));
        break;

        case PlanGraphOpcode::SCAN_NODES:
            return translateScanNodesNode(static_cast<ScanNodesNode*>(node));
        break;

        case PlanGraphOpcode::GET_OUT_EDGES:
            return translateGetOutEdgesNode(static_cast<GetOutEdgesNode*>(node));
        break;

        case PlanGraphOpcode::PRODUCE_RESULTS:
            return translateProduceResultsNode(static_cast<ProduceResultsNode*>(node));
        break;

        case PlanGraphOpcode::FILTER_NODE:
            return translateNodeFilterNode(static_cast<NodeFilterNode*>(node));
        break;

        case PlanGraphOpcode::FILTER_EDGE:
            return translateEdgeFilterNode(static_cast<EdgeFilterNode*>(node));
        break;

        case PlanGraphOpcode::SKIP:
            return translateSkipNode(static_cast<SkipNode*>(node));
        break;

        case PlanGraphOpcode::LIMIT:
            return translateLimitNode(static_cast<LimitNode*>(node));
        break;

        case PlanGraphOpcode::GET_EDGE_TARGET:
            return translateGetEdgeTargetNode(static_cast<GetEdgeTargetNode*>(node));
        break;

        case PlanGraphOpcode::GET_IN_EDGES:
            return translateGetInEdgesNode(static_cast<GetInEdgesNode*>(node));
        break;

        case PlanGraphOpcode::GET_EDGES:
            return translateGetEdgesNode(static_cast<GetEdgesNode*>(node));
        break;

        case PlanGraphOpcode::CARTESIAN_PRODUCT:
            return translateCartesianProductNode(static_cast<CartesianProductNode*>(node));
        break;

        case PlanGraphOpcode::GET_PROPERTY:
            return translateGetPropertyNode(static_cast<GetPropertyNode*>(node));
        break;

        case PlanGraphOpcode::GET_PROPERTY_WITH_NULL:
            return translateGetPropertyWithNullNode(static_cast<GetPropertyWithNullNode*>(node));
        break;

        case PlanGraphOpcode::AGGREGATE_EVAL:
            return translateAggregateEvalNode(static_cast<AggregateEvalNode*>(node));
        break;

        case PlanGraphOpcode::GET_ENTITY_TYPE:
        case PlanGraphOpcode::JOIN:
        case PlanGraphOpcode::CREATE_GRAPH:
        case PlanGraphOpcode::PROJECT_RESULTS:
        case PlanGraphOpcode::WRITE:
        case PlanGraphOpcode::FUNC_EVAL:
        case PlanGraphOpcode::ORDER_BY:
        case PlanGraphOpcode::UNKNOWN:
        case PlanGraphOpcode::_SIZE:
            throw PlannerException(fmt::format("BranchGenerator does not support PlanGraphNode: {}",
                PlanGraphOpcodeDescription::value(node->getOpcode())));
    }

    throw FatalException("Failed to match against PlanGraphOpcode");
}

PipelineOutputInterface* BranchGenerator::translateVarNode(VarNode* node) {
    const std::string_view varName = node->getVarDecl()->getName();
    if (varName.empty()) {
        throw PlannerException("VarNode with empty name");
    }

    const PipelineOutputInterface* output = _builder.getPendingOutputInterface();
    const EntityOutputStream& stream = output->getStream();

    Dataframe* outDf = output->getDataframe();

    const auto visitor = Overloaded {
        [&](const EntityOutputStream::NodeStream& stream) {
            msgbioassert(stream._nodeIDsTag.isValid(), "NodeStream does not have a nodeIDsTag");
            msgbioassert(outDf->getColumn(stream._nodeIDsTag), "NodeStream does not have a nodeIDs column");

            _declToColumn[node->getVarDecl()] = stream._nodeIDsTag;
            outDf->getColumn(stream._nodeIDsTag)->rename(varName);
        },
        [&](const EntityOutputStream::EdgeStream& stream) {
            msgbioassert(stream._edgeIDsTag.isValid(), "EdgeStream does not have a edgeIDsTag");
            msgbioassert(outDf->getColumn(stream._edgeIDsTag), "EdgeStream does not have a edgeIDs column");

            _declToColumn[node->getVarDecl()] = stream._edgeIDsTag;
            outDf->getColumn(stream._edgeIDsTag)->rename(varName);
        },
    };

    stream.visit(visitor);


    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateScanNodesNode(ScanNodesNode* node) {
    _builder.addScanNodes();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateGetOutEdgesNode(GetOutEdgesNode* node) {
    _builder.addGetOutEdges();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateGetInEdgesNode(GetInEdgesNode* node) {
    _builder.addGetInEdges();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateGetEdgesNode(GetEdgesNode* node) {
    _builder.addGetEdges();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateGetEdgeTargetNode(GetEdgeTargetNode* node) {
    _builder.projectEdgesOnOtherIDs();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateGetPropertyNode(GetPropertyNode* node) {
    const VarDecl* entityDecl = node->getEntityVarDecl();
    if (!entityDecl) {
        throw PlannerException("GetPropertyNode does not have an entity variable declaration");
    }

    const std::string propName {node->getPropName()};

    PipelineValuesOutputInterface* output = nullptr;

    // Retrieving the property type from the graph metadata
    const std::optional<PropertyType> foundProp = _view.read().getMetadata().propTypes().get(propName);
    if (!foundProp) {
        throw PlannerException(fmt::format("Property type {} does not exist", propName));
    }

    // Adding the GetProperty processor to the pipeline
    switch (entityDecl->getType()) {
        case EvaluatedType::NodePattern: {
            const auto process = [&]<SupportedType Type> {
                output = &_builder.addGetProperties<EntityType::Node, Type>(*foundProp);
            };
            PropertyTypeDispatcher {foundProp->_valueType}.execute(process);
        }
        case EvaluatedType::EdgePattern: {
            const auto process = [&]<SupportedType Type> {
                output = &_builder.addGetProperties<EntityType::Edge, Type>(*foundProp);
            };

            PropertyTypeDispatcher {foundProp->_valueType}.execute(process);
        }
        default: {
            throw PlannerException(fmt::format(
                "GetProperty must act on a Node/EdgePattern. Instead acting on {}",
                EvaluatedTypeName::value(entityDecl->getType())));
        }
    }

    // Mapping the expr decl to the column tag
    const Expr* expr = node->getExpr();
    if (!expr) {
        throw PlannerException("GetPropertyNode does not have an expression");
    }

    const VarDecl* exprDecl = expr->getExprVarDecl();
    if (!exprDecl) [[unlikely]] {
        throw PlannerException("GetPropertyNode does not have an expression variable declaration");
    }

    _declToColumn[exprDecl] = output->getValues()->getTag();

    // Adding the materialize step
    _builder.addMaterialize();

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateGetPropertyWithNullNode(GetPropertyWithNullNode* node) {
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
    if (!exprDecl) [[unlikely]] {
        throw PlannerException("GetPropertyWithNullNode does not have an expression variable declaration");
    }

    _declToColumn[exprDecl] = output->getValues()->getTag();

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateNodeFilterNode(NodeFilterNode* node) {
    if (!node->isEmpty()) {
        throw PlannerException("BranchGenerator does not support non-empty NodeFilterNode.");
    }
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateEdgeFilterNode(EdgeFilterNode* node) {
    if (!node->isEmpty()) {
        throw PlannerException("BranchGenerator does not support non-empty NodeFilterNode.");
    }
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateProduceResultsNode(ProduceResultsNode* node) {
    // If MaterializeNode has not been seen at that point,
    // we materialize if we have data in flight
    if (_builder.isMaterializeOpen() && !_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const Projection* projNode = node->getProjection();

    std::vector<ProjectionItem> items;

    for (const Expr* item : projNode->items()) {
        const VarDecl* decl = item->getExprVarDecl();

        if (!decl) {
            throw PlannerException("Projection item does not have a variable declaration");
        }

        const ColumnTag tag = _declToColumn.at(decl);
        const std::string_view repr = _sourceManager->getStringRepr((std::uintptr_t)item);

        items.push_back({tag, repr});
    }

    _builder.addProjection(items);

    auto lambdaCallback = [this](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        _callback(df);
    };

    _builder.addLambda(lambdaCallback);
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateJoinNode(JoinNode* node) {
    throw FatalException("Join Processor not implemented");
}

PipelineOutputInterface* BranchGenerator::translateSkipNode(SkipNode* node) {
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
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateLimitNode(LimitNode* node) {
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
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateCartesianProductNode(CartesianProductNode* node) {
    const auto binaryNodeIt = _binaryNodeMap.find(node);
    if (binaryNodeIt == _binaryNodeMap.end()) {
        throw PipelineException("Attempted to translate CartesianProductNode which was "
                                "not already encountered.");
    }

    PipelineOutputInterface* inputA = _builder.getPendingOutputInterface();
    auto& [inputB, isBLhs] = binaryNodeIt->second;

    PipelineOutputInterface* lhs = isBLhs ? inputB : inputA;
    PipelineOutputInterface* rhs = isBLhs ? inputA : inputB;

    // LHS is implicit in @ref _pendingOutput
    _builder.getPendingOutput().updateInterface(lhs);

    _builder.addCartesianProduct(rhs);

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* BranchGenerator::translateAggregateEvalNode(AggregateEvalNode* node) {
    if (_builder.isMaterializeOpen() && !_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const auto& groupByKeys = node->getGroupByKeys();
    if (!groupByKeys.empty()) {
        throw PlannerException("Group by keys are not supported yet");
    }

    const auto& funcs = node->getFuncs();

    if (funcs.empty()) [[unlikely]] {
        throw PlannerException("AggregateEvalNode does not have any functions");
    }

    if (funcs.size() != 1) [[unlikely]] {
        throw PlannerException("Evaluation of multiple aggregate functions is not supported yet");
    }

    for (const FunctionInvocationExpr* func : funcs) {
        const FunctionInvocation* invocation = func->getFunctionInvocation();
        const ExprChain* args = invocation->getArguments();
        const FunctionSignature* signature = invocation->getSignature();

        if (!invocation) [[unlikely]] {
            throw PlannerException("FunctionInvocationExpr does not have a FunctionInvocation");
        }

        if (!args) [[unlikely]] {
            throw PlannerException("FunctionInvocation does not have arguments");
        }

        if (!signature) [[unlikely]] {
            throw PlannerException("FunctionInvocation does not have a FunctionSignature");
        }

        if (!signature->_isAggregate) [[unlikely]] {
            throw PlannerException("FunctionInvocation is not an aggregate function");
        }

        if (signature->_fullName == "count") {
            PipelineValueOutputInterface* output = nullptr;
            if (args->empty()) {
                // e.g. count() Not supported yet
                output = &_builder.addCount();

            } else if (args->size() == 1) {
                // e.g. count(expr)
                const Expr* arg = args->front();
                const VarDecl* argDecl = arg->getExprVarDecl();
                if (arg->getType() == EvaluatedType::Wildcard) {
                    output = &_builder.addCount();
                } else {
                    // count(*)
                    output = &_builder.addCount(_declToColumn.at(argDecl));
                }

            } else [[unlikely]] {
                // Already checked in the planner
                throw PlannerException("Invalid arguments for count()");
            }

            const VarDecl* exprDecl = func->getExprVarDecl();

            if (!exprDecl) [[unlikely]] {
                throw PlannerException("Count() expression does not have an expression variable declaration");
            }

            _declToColumn[exprDecl] = output->getValue()->getTag();
        } else {
            throw PlannerException(fmt::format("Aggregate function '{}' is not implemented yet", signature->_fullName));
        }
    }
    return _builder.getPendingOutputInterface();
}
