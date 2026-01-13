#include "PipelineGenerator.h"

#include <stack>
#include <string_view>

#include <range/v3/view/zip.hpp>
#include <spdlog/fmt/fmt.h>

#include "EntityOutputStream.h"
#include "ExecutionContext.h"
#include "FunctionInvocation.h"
#include "PendingOutputView.h"
#include "PlanGraph.h"
#include "Predicate.h"
#include "Symbol.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "dataframe/ColumnTag.h"
#include "dataframe/NamedColumn.h"
#include "decl/PatternData.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/SymbolExpr.h"
#include "interfaces/PipelineNodeOutputInterface.h"
#include "interfaces/PipelineOutputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"
#include "procedures/ProcedureBlueprintMap.h"
#include "processors/PredicateProgram.h"
#include "processors/WriteProcessor.h"
#include "processors/WriteProcessorTypes.h"
#include "reader/GraphReader.h"
#include "SourceManager.h"

#include "processors/MaterializeProcessor.h"

#include "nodes/ChangeNode.h"
#include "nodes/CommitNode.h"
#include "nodes/CartesianProductNode.h"
#include "nodes/JoinNode.h"
#include "nodes/PlanGraphNode.h"
#include "nodes/GetPropertyNode.h"
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
#include "nodes/AggregateEvalNode.h"
#include "nodes/ProcedureEvalNode.h"
#include "nodes/WriteNode.h"
#include "nodes/ScanNodesByLabelNode.h"
#include "nodes/LoadGraphNode.h"
#include "nodes/ListGraphNode.h"
#include "nodes/CreateGraphNode.h"
#include "nodes/LoadGMLNode.h"
#include "nodes/LoadNeo4jNode.h"
#include "nodes/S3ConnectNode.h"
#include "nodes/S3TransferNode.h"

#include "Projection.h"
#include "decl/VarDecl.h"
#include "expr/LiteralExpr.h"
#include "Literal.h"

#include "Overloaded.h"
#include "processors/ExprProgram.h"
#include "ExprProgramGenerator.h"

#include "PipelineException.h"
#include "PlannerException.h"
#include "FatalException.h"
#include "BioAssert.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

struct TranslateNodeToken {
    PlanGraphNode* node {nullptr};
    PipelineOutputInterface* previousInterface {nullptr};
    MaterializeProcessor* matProc {nullptr};
};

using TranslateTokenStack = std::stack<TranslateNodeToken>;

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

ColumnTag PipelineGenerator::getCol(const VarDecl* var) {
    if (!var) {
        throw FatalException("Attempted to get column for null variable");
    }

    const auto it = _declToColumn.find(var);
    if (it == end(_declToColumn)) {
        throw PlannerException(fmt::format("Failed to find column for variable {}.", var->getName()));
    }
    const ColumnTag tag = it->second;
    return tag;
}

void PipelineGenerator::generate() {
    TranslateTokenStack nodeStack;

    // Insert root nodes
    std::vector<PlanGraphNode*> rootNodes;
    _graph->getRoots(rootNodes);

    for (const auto& node : rootNodes) {
        // create a new materialize processor for every branch
        nodeStack.emplace(node, nullptr, MaterializeProcessor::create(_pipeline, _mem));
    }

    // Translate nodes in a DFS manner
    while (!nodeStack.empty()) {
        auto [node, prevIf, matProc] = nodeStack.top();
        nodeStack.pop();

        // Always set pending output from the previous Output Interface of the node
        // that inserted the current node into the stack
        _builder.getPendingOutput().setInterface(prevIf);

        // We also set the materialize processor from the previous materialize processor
        // of the node that inserted the current node into the stack
        _builder.setMaterializeProc(matProc);
        PipelineOutputInterface* outputIf = translateNode(node);

        // If a new mat proc could be created during the node transalation
        //(In the case of join/cartesian product) we need to retreive
        // it from _builder - otherwise this will hold the same pinter as
        // matProc
        auto* newMatProc = _builder.getMaterializeProc();

        if (newMatProc->isConnected()) {
            // If we have materialized during the transalate we have to make a new matProc
            newMatProc = MaterializeProcessor::createFromPrev(_pipeline, _mem, *newMatProc);
        }
        auto processNextNode = [&](PlanGraphNode* nextNode) {
            // Unary node case
            if (!nextNode->isBinary()) {
                nodeStack.emplace(nextNode, outputIf, newMatProc);
            } else {
                // Binary node case
                // We need to materialize the inputs of a binary node (for now)
                _builder.setMaterializeProc(newMatProc);
                // Check if we only have a single step in the current mat proc, if
                // so there is no need to materialize
                if (!_builder.isSingleMaterializeStep()) {
                    outputIf = &_builder.addMaterialize();
                    // reinitialise the materialise processor to pass on the descendant nodes
                    // newMatProc = MaterializeProcessor::createFromPrev(_pipeline, _mem, newMatProc);
                }

                const bool visited = _binaryVisitedMap.contains(nextNode);
                if (visited) {
                    // Binary Nodes have a null MatProc pointer initially
                    // There is no need to materialize before we add the node
                    // to the builder as we have materialized the inputs.
                    // All Binary Nodes must create a new MaterializeProcessor!
                    nodeStack.emplace(nextNode, outputIf, nullptr);
                    return;
                }

                const PendingOutputView& pendingOutput = _builder.getPendingOutput();
                const PlanGraphNode::Nodes& binaryNodeInputs = nextNode->inputs();
                const bool isLhs = (node == binaryNodeInputs.front());

                const BinaryNodeVisitInformation info {pendingOutput.getInterface(), isLhs};
                _binaryVisitedMap.emplace(nextNode, info);
            }
        };

        const auto& outputs = node->outputs();
        if (outputs.size() > 1) {
            auto& forkOutputs = _builder.addFork(outputs.size());
            for (size_t i = 0; i < outputs.size(); ++i) {
                auto* const nextNode = outputs[i];
                // set outputIf to each of the forks output ports, so each descendant
                // branch will have copy of the pipelineInterface to work with
                outputIf = &(forkOutputs[i]);

                // Copy MatProc from 2nd branch to the last branch (we can reuse the MatProc
                // for the first branch)
                if (i > 0) {
                    // clone the mat proc for each of the forks output ports, so each descendant
                    // branch will have copy of the marProc to work with
                    newMatProc = MaterializeProcessor::clone(_pipeline, _mem, *newMatProc);
                }
                processNextNode(nextNode);
            }
        } else if (outputs.size() == 1) {
            processNextNode(outputs.front());
        }
    }
}

PipelineOutputInterface* PipelineGenerator::translateNode(PlanGraphNode* node) {
    switch (node->getOpcode()) {
        case PlanGraphOpcode::VAR:
            return translateVarNode(static_cast<VarNode*>(node));
        break;

        case PlanGraphOpcode::SCAN_NODES:
            return translateScanNodesNode(static_cast<ScanNodesNode*>(node));
        break;

        case PlanGraphOpcode::SCAN_NODES_BY_LABEL:
            return translateScanNodesByLabelNode(static_cast<ScanNodesByLabelNode*>(node));
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
        case PlanGraphOpcode::JOIN:
            return translateJoinNode(static_cast<JoinNode*>(node));
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

        case PlanGraphOpcode::PROCEDURE_EVAL:
            return translateProcedureEvalNode(static_cast<ProcedureEvalNode*>(node));
        break;

        case PlanGraphOpcode::WRITE:
            return translateWriteNode(static_cast<WriteNode*>(node));
        break;

        case PlanGraphOpcode::LOAD_GRAPH:
            return translateLoadGraph(static_cast<LoadGraphNode*>(node));
        break;

        case PlanGraphOpcode::LOAD_NEO4J:
            return translateLoadNeo4j(static_cast<LoadNeo4jNode*>(node));
        break;

        case PlanGraphOpcode::CHANGE:
            return translateChangeNode(static_cast<ChangeNode*>(node));
        break;

        case PlanGraphOpcode::COMMIT:
            return translateCommitNode(static_cast<CommitNode*>(node));
        break;

        case PlanGraphOpcode::LIST_GRAPH:
            return translateListGraphNode(static_cast<ListGraphNode*>(node));
        break;

        case PlanGraphOpcode::LOAD_GML:
            return translateLoadGML(static_cast<LoadGMLNode*>(node));
        break;

        case PlanGraphOpcode::CREATE_GRAPH:
            return translateCreateGraphNode(static_cast<CreateGraphNode*>(node));
        break;

        case PlanGraphOpcode::S3_CONNECT:
            return translateS3ConnectNode(static_cast<S3ConnectNode*>(node));
        break;

        case PlanGraphOpcode::S3_TRANSFER:
            return translateS3TransferNode(static_cast<S3TransferNode*>(node));
        break;

        case PlanGraphOpcode::GET_ENTITY_TYPE:
        case PlanGraphOpcode::PROJECT_RESULTS:
        case PlanGraphOpcode::FUNC_EVAL:
        case PlanGraphOpcode::ORDER_BY:
        case PlanGraphOpcode::UNKNOWN:
        case PlanGraphOpcode::_SIZE:
            throw PlannerException(fmt::format("PipelineGenerator does not support PlanGraphNode: {}",
                                               PlanGraphOpcodeDescription::value(node->getOpcode())));
        break;
    }
    throw FatalException(
        fmt::format("Failed to match {} against PlanGraphOpcode",
                    PlanGraphOpcodeDescription::value(node->getOpcode())));
}

PipelineOutputInterface* PipelineGenerator::translateVarNode(VarNode* node) {
    const std::string_view varName = node->getVarDecl()->getName();
    if (varName.empty()) {
        throw PlannerException("VarNode with empty name");
    }

    const PipelineOutputInterface* output = _builder.getPendingOutputInterface();
    const EntityOutputStream& stream = output->getStream();

    Dataframe* outDf = output->getDataframe();

    const auto visitor = Overloaded {
        [&](const EntityOutputStream::NodeStream& stream) {
            bioassert(stream._nodeIDsTag.isValid(), "NodeStream does not have a nodeIDsTag");
            bioassert(outDf->getColumn(stream._nodeIDsTag), "NodeStream does not have a nodeIDs column");

            _declToColumn[node->getVarDecl()] = stream._nodeIDsTag;
            outDf->getColumn(stream._nodeIDsTag)->rename(varName);
        },
        [&](const EntityOutputStream::EdgeStream& stream) {
            bioassert(stream._edgeIDsTag.isValid(), "EdgeStream does not have a edgeIDsTag");
            bioassert(outDf->getColumn(stream._edgeIDsTag), "EdgeStream does not have a edgeIDs column");

            _declToColumn[node->getVarDecl()] = stream._edgeIDsTag;
            outDf->getColumn(stream._edgeIDsTag)->rename(varName);
        },
    };

    stream.visit(visitor);

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateScanNodesNode(ScanNodesNode* node) {
    _builder.addScanNodes();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateScanNodesByLabelNode(ScanNodesByLabelNode* node) {
    _builder.addScanNodesByLabel(&node->getLabelSet());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateGetOutEdgesNode(GetOutEdgesNode* node) {
    _builder.addGetOutEdges();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateGetInEdgesNode(GetInEdgesNode* node) {
    _builder.addGetInEdges();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateGetEdgesNode(GetEdgesNode* node) {
    _builder.addGetEdges();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateGetEdgeTargetNode(GetEdgeTargetNode* node) {
    _builder.projectEdgesOnOtherIDs();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateGetPropertyNode(GetPropertyNode* node) {
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
        break;
        case EvaluatedType::EdgePattern: {
            const auto process = [&]<SupportedType Type> {
                output = &_builder.addGetProperties<EntityType::Edge, Type>(*foundProp);
            };

            PropertyTypeDispatcher {foundProp->_valueType}.execute(process);
        }
        break;
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

PipelineOutputInterface* PipelineGenerator::translateGetPropertyWithNullNode(GetPropertyWithNullNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const VarDecl* entityDecl = node->getEntityVarDecl();
    if (!entityDecl) {
        throw PlannerException("GetPropertyWithNullNode does not have an entity variable declaration");
    }

    ColumnTag entityTag {};
    const auto foundDeclIt = _declToColumn.find(entityDecl);
    if (foundDeclIt != end(_declToColumn)) { // if the decl is registered, use that column
        entityTag = foundDeclIt->second;
    } else { // if it is not registered, it must be from our incoming stream
        const EntityOutputStream& stream = _builder.getPendingOutputInterface()->getStream();
        if (stream.isNodeStream()) {
            entityTag = stream.asNodeStream()._nodeIDsTag;
        } else if (stream.isEdgeStream()) {
            entityTag = stream.asEdgeStream()._edgeIDsTag;
        }
        else {
            throw FatalException("Attempted to add GetPropertiesWithNull to pipeline "
                                 "with unkown entity and empty stream.");
        }
    }

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

PipelineOutputInterface* PipelineGenerator::translateNodeFilterNode(NodeFilterNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    if (node->isEmpty()) {
        return _builder.getPendingOutputInterface();
    }

    const auto& predicates = node->getPredicates();
    const auto& labelConstrs = node->getLabelConstraints();

    PredicateProgram* predProg = PredicateProgram::create(_pipeline);
    ExprProgramGenerator exprGen(this, predProg, _builder.getPendingOutput());

    // Compile predicate expressions into an expression program
    for (const Predicate* pred : predicates) {
        exprGen.generatePredicate(pred);
    }

    if (!labelConstrs.empty()) {
        const PipelineValuesOutputInterface& lblsetIf = _builder.addGetLabelSetID();
        const NamedColumn* lblSetCol = lblsetIf.getValues();

        if (!lblSetCol) {
            throw FatalException("Could not get label set column for label filter from dataframe.");
        }
        if (!lblSetCol->getColumn()) {
            throw FatalException("Could not get label set column for label filter.");
        }

        exprGen.addLabelConstraint(lblSetCol->getColumn(), labelConstrs);
    }

    // Then add a filter processor, taking the built expression program to execute
    const auto& output = _builder.addFilter(predProg);

    // Explictly create a new @ref MaterializeProcessor which uses the output columns of
    // this filter as its base. This then overrides the behaviour in @ref
    // PipelineGenerator::generate which would otherwise create a MatProc pointing to the
    // input of this filter processor.
    _builder.setMaterializeProc(
        MaterializeProcessor::createFromDf(_pipeline, _mem, output.getDataframe()));

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateEdgeFilterNode(EdgeFilterNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    if (node->isEmpty()) {
        return _builder.getPendingOutputInterface();
    }

    const auto& predicates = node->getPredicates();
    const auto& typeConstraint = node->getEdgeTypeConstraints();

    PredicateProgram* predProg = PredicateProgram::create(_pipeline);
    ExprProgramGenerator exprGen(this, predProg, _builder.getPendingOutput());

    if (!predicates.empty()) {
        // Compile predicate expressions into an expression program
        for (const Predicate* pred : predicates) {
            exprGen.generatePredicate(pred);
        }
    }

    if (typeConstraint.size() > 1) {
        throw PlannerException("Edges can only have 1 type constraint.");
    }

    if (!typeConstraint.empty()) {
        const PipelineValuesOutputInterface& edgeTypeIf = _builder.addGetEdgeTypeID();
        const NamedColumn* edgeTypecol = edgeTypeIf.getValues();

        if (!edgeTypecol) {
            throw FatalException("Could not get label set column for label filter from dataframe.");
        }
        if (!edgeTypecol->getColumn()) {
            throw FatalException("Could not get label set column for label filter.");
        }
        // Above checks there is exactly 1 type contraint
        const EdgeTypeID edgeTypeConstr = typeConstraint.front();

        exprGen.addEdgeTypeConstraint(edgeTypecol->getColumn(), edgeTypeConstr);
    }

    const auto& output = _builder.addFilter(predProg);

    // Explictly create a new @ref MaterializeProcessor which uses the output columns of
    // this filter as its base. This then overrides the behaviour in @ref
    // PipelineGenerator::generate which would otherwise create a MatProc pointing to the
    // input of this filter processor.
    _builder.setMaterializeProc(
        MaterializeProcessor::createFromDf(_pipeline, _mem, output.getDataframe()));

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateProduceResultsNode(ProduceResultsNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const Projection* projNode = node->getProjection();

    // No projection can happen in the case of a Standalone call
    // in which case, we can simply output the whole dataframe
    if (projNode) {
        std::vector<ProjectionItem> items;
        for (const Expr* item : projNode->items()) {
            const VarDecl* decl = item->getExprVarDecl();

            if (!decl) {
                throw PlannerException("Projection item does not have a variable declaration");
            }

            const ColumnTag tag = _declToColumn.at(decl);
            const std::string_view name = item->getName();

            const std::string_view repr = name.empty()
                ? _sourceManager->getStringRepr((std::uintptr_t)item)
                : name;

            items.push_back({tag, repr});
        }

        _builder.addProjection(items);
    }

    if (node->isProduceNone()) {
        std::vector<ProjectionItem> noItems;
        _builder.addProjection(noItems);
    }

    auto lambdaCallback = [this](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        _callback(df);
    };

    _builder.addLambda(lambdaCallback);
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateJoinNode(JoinNode* node) {
    if (!_binaryVisitedMap.contains(node)) {
        throw PipelineException("Attempted to translate JoinNode which was "
                                "not already encountered.");
    }

    PipelineOutputInterface* inputA = _builder.getPendingOutputInterface();
    auto& [inputB, isBLhs] = _binaryVisitedMap.at(node);

    PipelineOutputInterface* lhs = isBLhs ? inputB : inputA;
    PipelineOutputInterface* rhs = isBLhs ? inputA : inputB;

    // LHS is implicit in @ref _pendingOutput
    _builder.getPendingOutput().updateInterface(lhs);

    const auto visitor = Overloaded {
        [](const EntityOutputStream::NodeStream& stream) -> ColumnTag {
            return stream._nodeIDsTag;
        },
        [](const EntityOutputStream::EdgeStream& stream) -> ColumnTag {
            return stream._otherIDsTag;
        },
    };

    ColumnTag leftJoinTag;
    ColumnTag rightJoinTag;

    switch (node->getJoinType()) {
        case JoinType::COMMON_ANCESTOR: {
            leftJoinTag = _declToColumn.find(node->getLeftVarDecl())->second;
            rightJoinTag = _declToColumn.find(node->getRightVarDecl())->second;
            break;
        }
        case JoinType::COMMON_SUCCESSOR: {
            leftJoinTag = lhs->getStream().visit(visitor);
            rightJoinTag = rhs->getStream().visit(visitor);
            break;
        }
        case JoinType::DIAMOND: {
            throw PlannerException("Common Successor Joins With Common Ancestor Unsupported");
        }
        case JoinType::PREDICATE: {
            throw PlannerException("Join On Predicates Not Been Supported");
        }
    }

    const auto& outputIf = _builder.addHashJoin(rhs, leftJoinTag, rightJoinTag);
    _builder.setMaterializeProc(MaterializeProcessor::createFromDf(_pipeline,
                                                                   _mem,
                                                                   outputIf.getDataframe()));

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateSkipNode(SkipNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
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

PipelineOutputInterface* PipelineGenerator::translateLimitNode(LimitNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
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

PipelineOutputInterface* PipelineGenerator::translateCartesianProductNode(CartesianProductNode* node) {
    if (!_binaryVisitedMap.contains(node)) {
        throw PipelineException("Attempted to translate CartesianProductNode which was "
                                "not already encountered.");
    }

    PipelineOutputInterface* inputA = _builder.getPendingOutputInterface();
    auto& [inputB, isBLhs] = _binaryVisitedMap.at(node);

    PipelineOutputInterface* lhs = isBLhs ? inputB : inputA;
    PipelineOutputInterface* rhs = isBLhs ? inputA : inputB;

    // LHS is implicit in @ref _pendingOutput
    _builder.getPendingOutput().updateInterface(lhs);

    const auto& outputIf = _builder.addCartesianProduct(rhs);
    _builder.setMaterializeProc(MaterializeProcessor::createFromDf(_pipeline,
                                                                   _mem,
                                                                   outputIf.getDataframe()));
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateAggregateEvalNode(AggregateEvalNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
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

PipelineOutputInterface* PipelineGenerator::translateProcedureEvalNode(ProcedureEvalNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const FunctionInvocationExpr* funcExpr = node->getFuncExpr();
    const YieldClause* yield = node->getYield();

    const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
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

    std::vector<const VarDecl*> yieldDecls;
    std::vector<ProcedureBlueprint::YieldItem> yieldItems;

    const ProcedureBlueprint* blueprint = _blueprints->getBlueprint(signature->_fullName);
    if (!blueprint) {
        throw PlannerException(fmt::format("Procedure '{}' does not exist", signature->_fullName));
    }

    if (!yield) {
        blueprint->returnAll(yieldItems);
    } else {
        for (const auto* item : *yield->getItems()) {
            const Symbol* symbol = item->getSymbol();

            yieldItems.emplace_back(symbol->getOriginalName(), symbol->getName());
            yieldDecls.push_back(item->getExprVarDecl());
        }
    }

    _builder.addDatabaseProcedure(*blueprint, yieldItems);

    for (size_t i = 0; i < yieldItems.size(); i++) {
        const auto& item = yieldItems[i];

        NamedColumn* col = item._col;

        if (col && i < yieldDecls.size()) {
            const VarDecl* decl = yieldDecls[i];
            _declToColumn[decl] = col->getTag();
        }
    }

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateWriteNode(WriteNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    ExprProgram* exprProg = ExprProgram::create(_pipeline);
    ExprProgramGenerator exprGen(this, exprProg, _builder.getPendingOutput());

    WriteProcessor::DeletedNodes delNodes;
    delNodes.reserve(node->toDeleteNodes().size());
    { // Add the columns containing deleted node variables
        for (const VarDecl* deletedVar : node->toDeleteNodes()) {
            const ColumnTag tag = getCol(deletedVar);
            delNodes.push_back(tag);
        }
    }

    WriteProcessor::DeletedEdges delEdges;
    delEdges.reserve(node->toDeleteEdges().size());
    { // Add the columns containing deleted edge variables
        for (const VarDecl* deletedVar : node->toDeleteEdges()) {
            const ColumnTag tag = getCol(deletedVar);
            delEdges.push_back(tag);
        }
    }

    WriteProcessor::PendingNodes penNodes;
    penNodes.reserve(node->pendingNodes().size());
    {
        for (const WriteNode::PendingNode& pendingPlanNode : node->pendingNodes()) {
            const NodePatternData* const data = pendingPlanNode._data;

            if (!pendingPlanNode._name) {
                throw FatalException(
                    "Attempted to create a pending node with no variable declaration.");
            }
            const std::string_view nodeVarName = pendingPlanNode._name->getName();

            const std::span planLabels = data->labelConstraints();
            std::vector<std::string_view> labels;
            labels.assign(begin(planLabels), end(planLabels));

            // Properties
            WriteProcessorTypes::PropertyConstraints props;
            props.reserve(data->exprConstraints().size());
            for (const EntityPropertyConstraint& propConstr : data->exprConstraints()) {
                const auto& [name, type, expr] = propConstr;
                Column* propCol = exprGen.registerPropertyConstraint(expr);

                props.emplace_back(name, type, propCol);
            }

            // Initialise with invalid column tag, then later update after @ref addWrite
            // allocs the column
            penNodes.emplace_back(std::move(labels), std::move(props), nodeVarName, ColumnTag {});
        }
    }

    WriteProcessor::PendingEdges penEdges;
    penNodes.reserve(node->pendingEdges().size());
    {
        for (const WriteNode::PendingEdge& pendingPlanEdge : node->pendingEdges()) {
            const EdgePatternData* const data = pendingPlanEdge._data;

            // Source and target nodes could be existing nodes (passed to WriteProcesor as
            // input), or pending nodes (produced by WriteProcessor as output). If they
            // are input, they will already have a registered column from a previous
            // processor => present in @ref _declToColumn => set the tag here. Otherwise,
            // call to @ref PipelineBuilder::addWrite will alloc column for the pending
            // node, and we pass an invalid (default initialised) ColumnTag for such nodes
            // to the @ref WriteProcessorTypes::PendingEdge to be later updated in the
            // builder call.
            const VarDecl* srcVar = pendingPlanEdge._src;
            ColumnTag srcTag;
            const std::string_view srcName = srcVar->getName();
            if (const auto it = _declToColumn.find(srcVar); it != end(_declToColumn)) {
                srcTag = it->second;
            }

            const VarDecl* tgtVar = pendingPlanEdge._tgt;
            ColumnTag tgtTag;
            const std::string_view tgtName = tgtVar->getName();
            if (const auto it = _declToColumn.find(tgtVar); it != end(_declToColumn)) {
                tgtTag = it->second;
            }

            const std::string_view edgeVarName = pendingPlanEdge._name->getName();

            const std::span edgeTypes = data->edgeTypeConstraints();
            bioassert(edgeTypes.size() == 1, "only one edge type is supported");
            const std::string_view edgeType = edgeTypes.front();

            // Properties
            WriteProcessorTypes::PropertyConstraints props;
            props.reserve(data->exprConstraints().size());
            for (const EntityPropertyConstraint& propConstr : data->exprConstraints()) {
                const auto& [name, type, expr] = propConstr;
                Column* propCol = exprGen.registerPropertyConstraint(expr);

                props.emplace_back(name, type, propCol);
            }

            // Initialise with invalid column tag, then later update after @ref addWrite
            // allocs the column
            const ColumnTag edgeTag {};
            penEdges.emplace_back(std::move(props), edgeType, edgeVarName, srcName,
                                  tgtName, edgeTag, srcTag, tgtTag);
        }
    }

    // Has the side effect of allocing columns, and modifying the @ref _tag field of
    // elements of @ref penNodes and @ref penEdges in-place
    _builder.addWrite(exprProg, delNodes, delEdges, penNodes, penEdges);

    // Above call to @ref addWrite alloc'd columns for the new nodes/edges, storing the
    // tag in the elements of @ref penNodes @ref penEdges. We may need to reference these
    // columns, so update the mapping from VarDecl (stored in the PlanGraph WriteNodes) to
    // the ColumnTag (stored in the WriteProcessor PendingNodes/Edges).
    for (const auto& [planPendingNode, procPendingNode] :
         rv::zip(node->pendingNodes(), penNodes)) {
        _declToColumn[planPendingNode._name] = procPendingNode._tag;
    }
    // All edge srcs/tgts are either already registered, or registered in the node loop
    // above
    for (const auto& [planPendingEdge, procPendingEdge] :
         rv::zip(node->pendingEdges(), penEdges)) {
        _declToColumn[planPendingEdge._name] = procPendingEdge._tag;
    }

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateLoadGraph(LoadGraphNode* loadGraph) {
    _builder.addLoadGraph(loadGraph->getGraphName());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateLoadNeo4j(LoadNeo4jNode* node) {
    _builder.addLoadNeo4j(node->getGraphName(), node->getFilePath());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateChangeNode(ChangeNode* node) {
    _builder.addChangeOp(node->getOp());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateCommitNode(CommitNode* node) {
    _builder.addCommit();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateLoadGML(LoadGMLNode* loadGML) {
    _builder.addLoadGML(loadGML->getGraphName(), loadGML->getFilePath());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateListGraphNode(ListGraphNode* node) {
    _builder.addListGraph();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateCreateGraphNode(CreateGraphNode* node) {
    _builder.addCreateGraph(node->getGraphName());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateS3ConnectNode(S3ConnectNode* node) {
    _builder.addS3Connect(node->getAccessId(), node->getSecretKey(), node->getRegion());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateS3TransferNode(S3TransferNode* node) {
    if (node->getDirection() == S3TransferNode::Direction::PULL) {
        _builder.addS3Pull(node->getS3Bucket(),
                           node->getS3Prefix(),
                           node->getS3File(),
                           node->getLocalPath());
    } else {
        _builder.addS3Push(node->getS3Bucket(),
                           node->getS3Prefix(),
                           node->getS3File(),
                           node->getLocalPath());
    }
    return _builder.getPendingOutputInterface();
}
