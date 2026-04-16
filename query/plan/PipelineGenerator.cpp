#include "PipelineGenerator.h"

#include <stack>
#include <string_view>

#include <range/v3/view/zip.hpp>
#include <spdlog/fmt/fmt.h>

#include "EntityOutputStream.h"
#include "ExecutionContext.h"
#include "FunctionInvocation.h"
#include "FunctionSignature.h"
#include "ID.h"
#include "PendingOutputView.h"
#include "PlanGraph.h"
#include "Predicate.h"
#include "Symbol.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "dataframe/ColumnTag.h"
#include "dataframe/NamedColumn.h"
#include "decl/EvaluatedType.h"
#include "decl/PatternData.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "interfaces/PipelineNodeOutputInterface.h"
#include "interfaces/PipelineOutputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"
#include "metadata/SupportedType.h"
#include "procedures/ProcedureManager.h"
#include "processors/OrderByProcessor.h"
#include "processors/PathExplorerProcessor.h"
#include "processors/PredicateProgram.h"
#include "processors/WriteProcessor.h"
#include "processors/WriteProcessorTypes.h"
#include "reader/GraphReader.h"

#include "processors/MaterializeProcessor.h"
#include "processors/CountProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "columns/ColumnConst.h"

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
#include "nodes/LoadJsonlNode.h"
#include "nodes/S3ConnectNode.h"
#include "nodes/S3TransferNode.h"
#include "nodes/ShowProceduresNode.h"
#include "nodes/ShortestPathNode.h"
#include "nodes/LoadCSVNode.h"
#include "nodes/LoadCommitNode.h"
#include "nodes/ExprEvalNode.h"
#include "nodes/CreateVectorIndexNode.h"
#include "nodes/LoadVectorNode.h"
#include "nodes/VectorSearchNode.h"
#include "nodes/DeleteVectorIndexNode.h"
#include "nodes/ShowVectorIndexesNode.h"
#include "nodes/InstallExtensionNode.h"
#include "nodes/ShowExtensionsNode.h"
#include "nodes/OrderByNode.h"
#include "nodes/FuncEvalNode.h"
#include "nodes/PathExplorerNode.h"
#include "nodes/ConstScanNode.h"
#include "nodes/ConstWriteSourceNode.h"
#include "nodes/CreatePropertyIndexNode.h"
#include "nodes/IndexLookupNode.h"
#include "nodes/DropIndexNode.h"
#include "nodes/MergeDataPartsNode.h"

#include "TranslateJoinHelpers.h"

#include "processors/CreateVectorIndexProcessor.h"
#include "processors/LoadVectorProcessor.h"
#include "processors/VectorSearchProcessor.h"
#include "processors/DeleteVectorIndexProcessor.h"
#include "processors/ShowVectorIndexesProcessor.h"

#include "Projection.h"
#include "decl/VarDecl.h"
#include "expr/LiteralExpr.h"
#include "Literal.h"

#include "Overloaded.h"
#include "processors/ExprProgram.h"
#include "ExprProgramGenerator.h"
#include "PredicateProgramGenerator.h"

#include "SystemManager.h"
#include "TuringConfig.h"
#include "PipelineException.h"
#include "PlannerException.h"
#include "FatalException.h"
#include "BioAssert.h"
#include "stmt/OrderByItem.h"

#include "processors/CSVSourceProcessor.h"
#include "columns/ColumnStringTable.h"
#include "CSVParser.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

ValueType evaluatedToValueType(EvaluatedType type) {
    switch (type) {
        case EvaluatedType::Bool:
            return ValueType::Bool;
        case EvaluatedType::Char:
        case EvaluatedType::String:
            return ValueType::String;
        case EvaluatedType::Double:
            return ValueType::Double;
        case EvaluatedType::Integer:
            return ValueType::Int64;
        case EvaluatedType::Embedding:
            return ValueType::Embedding;
        case EvaluatedType::Null:
        case EvaluatedType::NodePattern:
        case EvaluatedType::EdgePattern:
        case EvaluatedType::StringTable:
        case EvaluatedType::List:
        case EvaluatedType::Map:
        case EvaluatedType::Invalid:
        case EvaluatedType::Wildcard:
        case EvaluatedType::GraphPath:
        case EvaluatedType::Tuple:
        case EvaluatedType::ValueType:
        case EvaluatedType::Label:
        case EvaluatedType::LabelSet:
        case EvaluatedType::PropertyType:
        case EvaluatedType::EdgeType:
        case EvaluatedType::_SIZE:
            return ValueType::Invalid;
    }

    return ValueType::Invalid;
}

struct TranslateNodeToken {
    PlanGraphNode* _node {nullptr};
    PipelineOutputInterface* _previousInterface {nullptr};
    MaterializeProcessor* _matProc {nullptr};
};

using TranslateTokenStack = std::stack<TranslateNodeToken>;

struct PropertyTypeDispatcher {
    db::ValueType _valueType {db::ValueType::Invalid};

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
            case db::ValueType::Embedding:
                executor.template operator()<db::types::Embedding>();
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
        const auto processNextNode = [&](PlanGraphNode* nextNode) {
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

                const BinaryNodeVisitInformation info(pendingOutput.getInterface(), isLhs);
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

        case PlanGraphOpcode::CONST_SCAN:
            return translateConstScanNode(static_cast<ConstScanNode*>(node));
        break;

        case PlanGraphOpcode::CONST_WRITE_SOURCE:
            return translateConstWriteSourceNode(static_cast<ConstWriteSourceNode*>(node));
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

        case PlanGraphOpcode::FILTER_DATAFRAME:
            return translateDataframeFilterNode(static_cast<DataframeFilterNode*>(node));
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

        case PlanGraphOpcode::EXPR_EVAL:
            return translateExprEvalNode(static_cast<ExprEvalNode*>(node));
        break;

        case PlanGraphOpcode::WRITE:
            return translateWriteNode(static_cast<WriteNode*>(node));
        break;

        case PlanGraphOpcode::LOAD_GRAPH:
            return translateLoadGraph(static_cast<LoadGraphNode*>(node));
        break;

        case PlanGraphOpcode::LOAD_JSONL:
            return translateLoadJsonl(static_cast<LoadJsonlNode*>(node));
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

        case PlanGraphOpcode::SHOW_PROCEDURES:
            return translateShowProceduresNode(static_cast<ShowProceduresNode*>(node));
        break;

        case PlanGraphOpcode::SHORTEST_PATH:
            return translateShortestPathNode(static_cast<ShortestPathNode*>(node));
        break;

        case PlanGraphOpcode::ORDER_BY:
            return translateOrderByNode(static_cast<OrderByNode*>(node));
        break;

        case PlanGraphOpcode::LOAD_CSV:
            return translateLoadCSVNode(static_cast<LoadCSVNode*>(node));
        break;

        case PlanGraphOpcode::CREATE_VECTOR_INDEX:
            return translateCreateVectorIndexNode(static_cast<CreateVectorIndexNode*>(node));
        break;

        case PlanGraphOpcode::LOAD_VECTOR:
            return translateLoadVectorNode(static_cast<LoadVectorNode*>(node));
        break;

        case PlanGraphOpcode::VECTOR_SEARCH:
            return translateVectorSearchNode(static_cast<VectorSearchNode*>(node));
        break;

        case PlanGraphOpcode::DELETE_VECTOR_INDEX:
            return translateDeleteVectorIndexNode(static_cast<DeleteVectorIndexNode*>(node));
        break;

        case PlanGraphOpcode::SHOW_VECTOR_INDEXES:
            return translateShowVectorIndexesNode(static_cast<ShowVectorIndexesNode*>(node));
        break;

        case PlanGraphOpcode::LOAD_COMMIT:
            return translateLoadCommit(static_cast<LoadCommitNode*>(node));
        break;

        case PlanGraphOpcode::INSTALL_EXTENSION:
            return translateInstallExtensionNode(static_cast<InstallExtensionNode*>(node));
        break;

        case PlanGraphOpcode::SHOW_EXTENSIONS:
            return translateShowExtensionsNode(static_cast<ShowExtensionsNode*>(node));
        break;

        case PlanGraphOpcode::PATH_EXPLORER:
            return translatePathExplorerNode(static_cast<PathExplorerNode*>(node));
        break;

        case PlanGraphOpcode::CREATE_PROPERTY_INDEX:
            return translateCreatePropertyIndexNode(static_cast<CreatePropertyIndexNode*>(node));
        break;

        case PlanGraphOpcode::INDEX_LOOKUP:
            return translateIndexLookupNode(static_cast<IndexLookupNode*>(node));
        break;

        case PlanGraphOpcode::DROP_INDEX:
            return translateDropIndexNode(static_cast<DropIndexNode*>(node));
        break;

        case PlanGraphOpcode::MERGE_DATAPARTS:
            return translateMergeDataPartsNode(static_cast<MergeDataPartsNode*>(node));
        break;

        case PlanGraphOpcode::FUNC_EVAL:
        case PlanGraphOpcode::GET_ENTITY_TYPE:
        case PlanGraphOpcode::PROJECT_RESULTS:
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

PipelineOutputInterface* PipelineGenerator::translateConstScanNode(ConstScanNode* node) {
    const VarDecl* var = node->var();
    bioassert(var, "Invalid variable declaration");
    Column* values = node->values();

    const PipelineValuesOutputInterface& output = _builder.addConstScan(values);

    // Update the var -> col map, so that the column produced by this node may be used in
    // e.g. return projections
    const NamedColumn* outputColumn = output.getValues();
    _declToColumn[var] = outputColumn->getTag();

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateConstWriteSourceNode(ConstWriteSourceNode* node) {
    auto [nodeIDCol, valuesCol] = _builder.addConstWriteSource(node->nodeIDs(),
                                                               node->values());

    _declToColumn[node->nodeIDDecl()] = nodeIDCol->getTag();
    _declToColumn[node->valuesDecl()] = valuesCol->getTag();

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

    const std::string propName(node->getPropName());

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

    ColumnTag entityTag;
    const auto foundDeclIt = _declToColumn.find(entityDecl);
    if (foundDeclIt != end(_declToColumn)) { // if the decl is registered, use that column
        entityTag = foundDeclIt->second;
    } else { // if it is not registered, it must be from our incoming stream
        const EntityOutputStream& stream = _builder.getPendingOutputInterface()->getStream();
        if (stream.isNodeStream()) {
            entityTag = stream.asNodeStream()._nodeIDsTag;
        } else if (stream.isEdgeStream()) {
            entityTag = stream.asEdgeStream()._edgeIDsTag;
        } else {
            throw FatalException("Attempted to add GetPropertiesWithNull to pipeline "
                                 "with unkown entity and empty stream.");
        }
    }

    const std::string propName(node->getPropName());

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

PipelineOutputInterface* PipelineGenerator::translateDataframeFilterNode(DataframeFilterNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    if (node->isEmpty()) {
        return _builder.getPendingOutputInterface();
    }

    const auto& predicates = node->getPredicates();

    PredicateProgram* predProg = PredicateProgram::create(_pipeline);
    PredicateProgramGenerator predGen(this, predProg, _builder.getPendingOutput());

    for (const Predicate* pred : predicates) {
        predGen.generatePredicate(pred);
    }

    const auto& output = _builder.addFilter(predProg);
    Dataframe* outputDf = output.getDataframe();

    auto* newMatProc = MaterializeProcessor::createFromDf(_pipeline, _mem, outputDf);
    _builder.setMaterializeProc(newMatProc);

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
    PredicateProgramGenerator predGen(this, predProg, _builder.getPendingOutput());

    // Compile predicate expressions into an expression program
    for (const Predicate* pred : predicates) {
        predGen.generatePredicate(pred);
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

        predGen.addLabelConstraint(lblSetCol->getColumn(), labelConstrs);
    }

    // Then add a filter processor, taking the built expression program to execute
    const auto& output = _builder.addFilter(predProg);
    Dataframe* outputDf = output.getDataframe();

    // Explictly create a new @ref MaterializeProcessor which uses the output columns of
    // this filter as its base. This then overrides the behaviour in @ref
    // PipelineGenerator::generate which would otherwise create a MatProc pointing to the
    // input of this filter processor.
    auto* newMatProc = MaterializeProcessor::createFromDf(_pipeline, _mem, outputDf);
    _builder.setMaterializeProc(newMatProc);

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
    PredicateProgramGenerator predGen(this, predProg, _builder.getPendingOutput());

    if (!predicates.empty()) {
        // Compile predicate expressions into an expression program
        for (const Predicate* pred : predicates) {
            predGen.generatePredicate(pred);
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

        predGen.addEdgeTypeConstraint(edgeTypecol->getColumn(), edgeTypeConstr);
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
        // Resolve expressions not yet registered in _declToColumn
        // (e.g., CSV field access, type conversions on CSV data)
        Dataframe* df = _builder.getPendingOutputInterface()->getDataframe();
        DataframeManager* dfMan = _pipeline->getDataframeManager();

        ExprProgram* exprProg = ExprProgram::create(_pipeline);
        ExprProgramGenerator exprGen(this, exprProg, _builder.getPendingOutput());

        // Generate columns for projection expressions not yet registered
        // (e.g. CSV field access, type conversions on CSV data)
        for (const Projection::ReturnItem& item : projNode->items()) {
            const auto* exprPtr = std::get_if<Expr*>(&item);
            if (!exprPtr) {
                continue;
            }

            const Expr* expr = *exprPtr;
            const VarDecl* decl = expr->getExprVarDecl();
            if (!decl || _declToColumn.contains(decl)) {
                continue;
            }

            if (ExprEvalNode::needsEvaluation(expr)) {
                Column* col = exprGen.generateExpr(expr);
                ColumnTag tag = dfMan->allocTag();
                NamedColumn* namedCol = NamedColumn::create(dfMan, col, tag);
                df->addColumn(namedCol);
                _declToColumn[decl] = tag;
            }
        }

        if (!exprProg->instrs().empty()) {
            _builder.addExprEval(exprProg);
        }

        std::vector<ProjectionItem> items;
        for (const Projection::ReturnItem& item : projNode->items()) {
            if (const auto* exprPtr = std::get_if<Expr*>(&item)) {
                const Expr* expr = *exprPtr;
                const VarDecl* decl = expr->getExprVarDecl();

                const std::optional<std::string_view> name = projNode->getName(expr);
                if (!name) {
                    continue;
                }

                if (!decl) {
                    throw PlannerException(
                        "Projection item does not have a variable declaration");
                }

                const auto findColIt = _declToColumn.find(decl);
                if (findColIt == _declToColumn.end()) {
                    throw PlannerException(fmt::format("Unregistered variable {}.", decl->getName()));
                }
                const ColumnTag tag = findColIt->second;

                items.push_back({tag, *name});
            } else if (const auto* declPtr = std::get_if<VarDecl*>(&item)) {
                const VarDecl* decl = *declPtr;

                const std::optional<std::string_view> name = projNode->getName(decl);
                if (!name) {
                    continue;
                }

                const auto findColIt = _declToColumn.find(decl);
                if (findColIt == _declToColumn.end()) {
                    throw PlannerException(
                        fmt::format("Unregistered variable {}.", decl->getName()));
                }
                const ColumnTag tag = findColIt->second;

                items.push_back({tag, *name});
            }
        }

        _builder.addProjection(items);
    }

    if (node->isProduceNone()) {
        std::vector<ProjectionItem> noItems;
        _builder.addProjection(noItems);
    }

    auto lambdaCallback = [callbacks = _callbacks](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        callbacks->onOutputData(df);
    };

    const Dataframe* df = _builder.getPendingOutputInterface()->getDataframe();

    _builder.addLambda(lambdaCallback);
    _builder.setOutputDataframe(df);

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

    // Determine what the join tags need to be
    // leftJoinTag - will be used to identify the join key in the lhs input
    // rightJoinTag - will be used to identify the join key in the rhs input

    auto [leftJoinTag, rightJoinTag] = TranslateJoinHelpers::getJoinKeyTags(node,
                                                                            lhs,
                                                                            rhs,
                                                                            _declToColumn);

    auto& outputIf = _builder.addHashJoin(rhs, leftJoinTag, rightJoinTag);

    const auto& outputCols = outputIf.getDataframe()->cols();
    bioassert(!outputCols.empty(), "Join output column is empty");
    ColumnTag joinTag = outputCols.back()->getTag();

    const auto stream = TranslateJoinHelpers::resolveStream(node,
                                                            lhs,
                                                            rhs,
                                                            joinTag,
                                                            _declToColumn);
    outputIf.setStream(stream);

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

        if (!signature->isAggregate()) [[unlikely]] {
            throw PlannerException("FunctionInvocation is not an aggregate function");
        }

        if (signature->getFullName() == "count") {
            const VarDecl* exprDecl = func->getExprVarDecl();

            if (!exprDecl) [[unlikely]] {
                throw PlannerException("Count() expression does not have an expression variable declaration");
            }

            const bool hasInput = _builder.getPendingOutputInterface() != nullptr;

            // Standalone COUNT (e.g. `RETURN COUNT(*)`) has no input rows.
            // Produce a lambda source that emits a single ColumnConst<UInt64>
            // with value 0, since count over zero rows is always zero.
            if (!hasInput) {
                using CountType = CountProcessor::CountType;
                _builder.addLambdaSource(
                    [](Dataframe* df, bool& isFinished, LambdaSourceProcessor::Operation op) {
                        if (op == LambdaSourceProcessor::Operation::EXECUTE) {
                            isFinished = true;
                        }
                    });

                auto* zeroCol = _mem->alloc<ColumnConst<CountType>>();
                zeroCol->set(0);
                NamedColumn* countColumn = _builder.addColumnToOutput(zeroCol);

                _declToColumn[exprDecl] = countColumn->getTag();

            } else {
                PipelineValueOutputInterface* output = nullptr;
                if (args->empty()) {
                    // e.g. count()
                    output = &_builder.addCount();

                } else if (args->size() == 1) {
                    // e.g. count(expr)
                    const Expr* arg = args->front();
                    const VarDecl* argDecl = arg->getExprVarDecl();
                    if (arg->getType() == EvaluatedType::Wildcard) {
                        // count(*)
                        output = &_builder.addCount();
                    } else {
                        // count(<some var>)
                        const auto findIt = _declToColumn.find(argDecl);
                        if (findIt == _declToColumn.end()) {
                            throw FatalException(fmt::format(
                                "Failed to get column for variable {}.", argDecl->getName()));
                        }
                        const ColumnTag argTag = findIt->second;
                        output = &_builder.addCount(argTag);
                    }

                } else [[unlikely]] {
                    // Already checked in the planner
                    throw PlannerException("Invalid arguments for count()");
                }

                _declToColumn[exprDecl] = output->getValue()->getTag();
            }
        } else {
            throw PlannerException(fmt::format("Aggregate function '{}' is not implemented yet", signature->getFullName()));
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
    const ExprChain* argExprs = invocation->getArguments();
    const FunctionSignature* signature = invocation->getSignature();

    if (!invocation) [[unlikely]] {
        throw PlannerException("FunctionInvocationExpr does not have a FunctionInvocation");
    }

    if (!argExprs) [[unlikely]] {
        throw PlannerException("FunctionInvocation does not have arguments");
    }

    if (!signature) [[unlikely]] {
        throw PlannerException("FunctionInvocation does not have a FunctionSignature");
    }

    std::vector<const VarDecl*> yieldDecls;
    std::vector<Procedure::Argument> inputItems;
    std::vector<Procedure::YieldItem> yieldItems;

    const Procedure* procedure = _procedures->getProcedure(signature->getFullName());
    if (!procedure) {
        throw PlannerException(fmt::format("Procedure '{}' does not exist", signature->getFullName()));
    }

    if (!yield || !yield->getItems()) {
        procedure->returnAll(yieldItems);
    } else {
        for (const auto* item : *yield->getItems()) {
            const Symbol* symbol = item->getSymbol();

            yieldItems.emplace_back(symbol->getOriginalName(), symbol->getName());
            yieldDecls.push_back(item->getExprVarDecl());
        }
    }

    PipelineOutputInterface* prevOutput = _builder.getPendingOutputInterface();
    Dataframe* inDf = nullptr;

    if (prevOutput) {
        inDf = prevOutput->getDataframe();
    }

    for (size_t i = 0; const auto* argExpr : *argExprs) {
        const Column* col = nullptr;

        const VarDecl* argDecl = argExpr->getExprVarDecl();

        if (!argDecl) {
            if (argExpr->getKind() != Expr::Kind::LITERAL && argExpr->getKind() != Expr::Kind::SYMBOL) {
                // TODO: replace this with an expression evaluation processor
                throw PlannerException("Procedure arguments must be literals or symbols");
            }

            ExprProgram* exprProg = ExprProgram::create(_pipeline);
            ExprProgramGenerator exprGen(this, exprProg, _builder.getPendingOutput());
            col = exprGen.generateExpr(argExpr);
        } else {
            auto it = _declToColumn.find(argDecl);
            bioassert(it != _declToColumn.end(),
                      "Argument does not have a variable declaration");
            const ColumnTag tag = it->second;
            const NamedColumn* namedCol = inDf->getColumn(tag);

            if (!namedCol) [[unlikely]] {
                throw PlannerException("Column not found");
            }

            col = namedCol->getColumn();
        }

        bioassert(col, "Column not found");

        inputItems.emplace_back(i++, col);
    }

    _builder.addCallProcedure(procedure, inputItems, yieldItems);

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

PipelineOutputInterface* PipelineGenerator::translateExprEvalNode(ExprEvalNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const ExprEvalNode::Expressions& exprs = node->getExprs();

    if (exprs.empty()) {
        return _builder.getPendingOutputInterface();
    }

    ExprProgram* prog = ExprProgram::create(_pipeline);
    ExprProgramGenerator progGen(this, prog, _builder.getPendingOutput());

    // Add the evaluating processor to the pipeline. It takes a pointer to the above
    // @ref ExprProg (@ref prog), and the below loops over expressions modifies @ref prog
    // via that same pointer, in place.
    _builder.addExprEval(prog);

    for (const Expr* expr : exprs) {
        const VarDecl* var = expr->getExprVarDecl();
        bioassert(var, "Expression to evaluate had null variable declaration.");

        // Tree walk the expression to allocate result columns as raw @ref Column*s
        Column* resultantColumn = progGen.generateExpr(expr);
        // Create a @ref NamedColumn which wraps the result @ref Column* produced by the
        // @ref ExprProgramGenerator, and add that wrapped NamedCol to output of the newly
        // added @ref ExprEvalProcessor.
        NamedColumn* resultNCol = _builder.addColumnToOutput(resultantColumn);

        // Map back this variable to the new column for return projections, etc.
        _declToColumn[var] = resultNCol->getTag();
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
            for (const auto& [name, type, expr] : data->exprConstraints()) {
                Column* propCol = exprGen.registerPropertyConstraint(expr);

                props.emplace_back(name, type, propCol);
            }

            // Initialise with invalid column tag, then later update after @ref addWrite
            // allocs the column
            penNodes.emplace_back(std::move(labels), std::move(props), nodeVarName, ColumnTag {});
        }
    }

    WriteProcessor::PendingEdges penEdges;
    penEdges.reserve(node->pendingEdges().size());
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
            const ColumnTag edgeTag;
            penEdges.emplace_back(std::move(props), edgeType, edgeVarName, srcName,
                                  tgtName, edgeTag, srcTag, tgtTag);
        }
    }

    const PropertyTypeMap& propMap = _view.metadata().propTypes();

    WriteProcessor::UpdatedNodes updatedNodes;
    updatedNodes.reserve(node->nodeUpdates().size());
    {
        for (const auto& [var, name, valueExpr] : node->nodeUpdates()) {
            bioassert(var && valueExpr, "Invalid variable and value for updated node.");
            Column* propCol = exprGen.registerPropertyConstraint(valueExpr);
            bioassert(propCol, "Failed to generate value column for node property update.");

            const auto findIt = _declToColumn.find(var);
            if (findIt == end(_declToColumn)) {
                throw FatalException(fmt::format(
                    "Failed to get column to update node {}.", var->getName()));
            }

            const ColumnTag tagToUpdate = findIt->second;

            const std::optional<PropertyType> maybePropType = propMap.get(name);

            ValueType valType {_SIZE};
            PropertyTypeID propID;
            if (maybePropType.has_value()) { // Property already exists: use metadata
                valType = maybePropType->_valueType;
                propID = maybePropType->_id;
            } else { // New property: get its type, use an invalid PropID (later updated
                     // in WriteProcessor::updateNodes)
                const EvaluatedType propertyType = valueExpr->getType();
                valType = evaluatedToValueType(propertyType);
                propID = PropertyTypeID::max();
            }

            const WriteProcessorTypes::PropertyUpdate propUpdated(name, valType, propCol, propID);
            updatedNodes.emplace_back(propUpdated, tagToUpdate);
        }
    }

    WriteProcessor::UpdatedEdges updatedEdges;
    {
        for (const auto& [var, name, valueExpr] : node->edgeUpdates()) {
            bioassert(var && valueExpr, "Invalid variable and value for updated edge.");

            Column* propCol = exprGen.registerPropertyConstraint(valueExpr);
            bioassert(propCol, "Failed to generate value column for edge property update.");

            const auto findIt = _declToColumn.find(var);
            if (findIt == end(_declToColumn)) {
                throw FatalException(fmt::format(
                    "Failed to get column to update edge {}.", var->getName()));
            }

            const ColumnTag tagToUpdate = findIt->second;

            std::optional<PropertyType> maybePropType = propMap.get(name);

            ValueType valType {_SIZE};
            PropertyTypeID propID;
            if (maybePropType.has_value()) { // Property already exists: use metadata
                valType = maybePropType->_valueType;
                propID = maybePropType->_id;
            } else { // New property: get its type, use an invalid PropID (later updated
                     // in WriteProcessor::updateEdges)
                const EvaluatedType propertyType = valueExpr->getType();
                valType = evaluatedToValueType(propertyType);
                propID = PropertyTypeID::max();
            }

            const WriteProcessorTypes::PropertyUpdate propUpdated(name, valType, propCol, propID);
            updatedEdges.emplace_back(propUpdated, tagToUpdate);
        }
    }

    // Has the side effect of allocing columns, and modifying the @ref _tag field of
    // elements of @ref penNodes and @ref penEdges in-place
    _builder.addWrite(exprProg, delNodes, delEdges, penNodes, penEdges, updatedNodes, updatedEdges);

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

PipelineOutputInterface* PipelineGenerator::translateLoadGraph(LoadGraphNode* node) {
    _builder.addLoadGraph(node->getGraphName());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateLoadJsonl(LoadJsonlNode* node) {
    _builder.addLoadJsonl(node->getGraphName(), node->getFilePath());
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

PipelineOutputInterface* PipelineGenerator::translateMergeDataPartsNode(MergeDataPartsNode* node) {
    _builder.addMergeDataParts();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateLoadGML(LoadGMLNode* node) {
    _builder.addLoadGML(node->getGraphName(), node->getFilePath());
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

PipelineOutputInterface* PipelineGenerator::translateShowProceduresNode(ShowProceduresNode* node) {
    _builder.addShowProcedures();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateShortestPathNode(ShortestPathNode* node) {
    if (!_binaryVisitedMap.contains(node)) {
        throw PipelineException("Attempted to translate ShortestPath Node which was "
                                "not already encountered.");
    }

    PipelineOutputInterface* inputA = _builder.getPendingOutputInterface();
    auto& [inputB, isBLhs] = _binaryVisitedMap.at(node);

    PipelineOutputInterface* lhs = isBLhs ? inputB : inputA;
    PipelineOutputInterface* rhs = isBLhs ? inputA : inputB;

    // LHS is implicit in @ref _pendingOutput
    _builder.getPendingOutput().updateInterface(lhs);

    NamedColumn* distCol = nullptr;
    NamedColumn* pathCol = nullptr;

    PipelineBlockOutputInterface* output = nullptr;

    const PropertyType edgeType = node->getEdgeType();
    const auto process = [&]<SupportedType Type>() {
        if constexpr (std::is_arithmetic_v<typename Type::Primitive>) {
            output = &_builder.addShortestPath<Type>(rhs,
                                                     _declToColumn[node->getSource()],
                                                     _declToColumn[node->getTarget()],
                                                     node->getEdgeType(),
                                                     distCol,
                                                     pathCol);
            distCol->rename(node->getDistance()->getName());
            pathCol->rename(node->getPath()->getName());
        } else {
            throw PlannerException("Unsupported Edge Weight Type");
        }
    };
    PropertyTypeDispatcher {edgeType._valueType}.execute(process);

    _declToColumn[node->getDistance()] = distCol->getTag();
    _declToColumn[node->getPath()] = pathCol->getTag();

    _builder.setMaterializeProc(MaterializeProcessor::createFromDf(_pipeline,
                                                                   _mem,
                                                                   output->getDataframe()));
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateLoadCSVNode(LoadCSVNode* node) {
    // Resolve file path relative to the data directory
    const fs::Path& dataDir = _sysMan->getConfig()->getDataDir();
    const fs::Path filePath = dataDir / node->getFilePath().get();

    if (!filePath.isSubDirectory(dataDir)) {
        throw PipelineException(fmt::format(
            "Invalid file path: path must be relative to '{}'",
            dataDir.get()));
    }

    // Peek at file structure to discover field count and headers
    CSVFileInfo fileInfo;
    CSVParser::peekFileStructure(filePath, node->hasHeaders(), fileInfo);

    const size_t fieldCount = fileInfo._fieldCount;

    // Allocate ColumnStringTable with field columns
    auto* table = _mem->alloc<ColumnStringTable>();
    table->setHeaders(fileInfo._headers);
    for (size_t i = 0; i < fieldCount; i++) {
        table->addFieldColumn(_mem->alloc<ColumnStringTable::StringColumn>());
    }

    const CSVErrorMode errorMode = node->skipOnError() ? CSVErrorMode::Skip : CSVErrorMode::Fail;

    auto* csvSource = CSVSourceProcessor::create(_pipeline,
                                                 filePath,
                                                 node->hasHeaders(),
                                                 errorMode,
                                                 fieldCount,
                                                 table);

    // Register the ColumnStringTable in the output dataframe
    PipelineBlockOutputInterface& output = csvSource->output();
    Dataframe* outDf = output.getDataframe();
    DataframeManager* dfMan = _pipeline->getDataframeManager();
    const ColumnTag stringTblTag = dfMan->allocTag();
    NamedColumn* stringTblCol = NamedColumn::create(dfMan, table, stringTblTag);
    outDf->addColumn(stringTblCol);

    _declToColumn[node->getAliasDecl()] = stringTblTag;

    _builder.getPendingOutput().setInterface(&output);
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateCreateVectorIndexNode(CreateVectorIndexNode* node) {
    _builder.addCreateVectorIndex(node->getIndexName(), node->getDimension(), node->getMetric());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateLoadVectorNode(LoadVectorNode* node) {
    _builder.addLoadVector(node->getFilePath(), node->getIndexName());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateVectorSearchNode(VectorSearchNode* node) {
    PipelineValuesOutputInterface& output = _builder.addVectorSearch(
        node->getIndexName(), node->getK(), node->getQueryVector());

    // Register the 'ids' column so RETURN can find it
    const VarDecl* idsDecl = node->getIDsVarDecl();
    if (idsDecl) {
        NamedColumn* idsCol = output.getValues();
        if (idsCol) {
            _declToColumn[idsDecl] = idsCol->getTag();
        }
    }

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateDeleteVectorIndexNode(DeleteVectorIndexNode* node) {
    _builder.addDeleteVectorIndex(node->getIndexName());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateShowVectorIndexesNode(ShowVectorIndexesNode* node) {
    _builder.addShowVectorIndexes();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translatePathExplorerNode(PathExplorerNode* node) {
    const uint64_t minHops = node->getMinHops();
    const uint64_t maxHops = node->getMaxHops();

    const auto makeProcessor = [&]<PathExplorationDir Dir>() {
        _builder.addPathExplorer<Dir>(minHops, maxHops);

        const auto* proc = dynamic_cast<PathExplorerProcessor<Dir>*>(_builder.getLastProc());
        bioassert(proc, "Failed to cast last proc to PathExplorerProcessor");

        const VarDecl* edgeDecl = node->getEdgeDecl();
        const VarDecl* targetDecl = node->getTargetDecl();

        NamedColumn* pathsCol = proc->getOutputPathsColumn();
        pathsCol->rename(edgeDecl->getName());
        _declToColumn[edgeDecl] = pathsCol->getTag();

        NamedColumn* sourceCol = proc->getOutputTargetsColumn();
        sourceCol->rename(targetDecl->getName());
        _declToColumn[targetDecl] = sourceCol->getTag();
    };

    switch (node->getDir()) {
        case PathExplorationDir::BOTH:
            makeProcessor.template operator()<PathExplorationDir::BOTH>();
        break;
        case PathExplorationDir::FORWARD:
            makeProcessor.template operator()<PathExplorationDir::FORWARD>();
        break;
        case PathExplorationDir::BACKWARD:
            makeProcessor.template operator()<PathExplorationDir::BACKWARD>();
        break;
    }

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateLoadCommit(LoadCommitNode* node) {
    _builder.addLoadCommit(node->getHashStr());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateInstallExtensionNode(InstallExtensionNode* node) {
    _builder.addInstallExtension(node->getExtensionName());
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateShowExtensionsNode(ShowExtensionsNode* node) {
    _builder.addShowExtensions();
    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateOrderByNode(OrderByNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const OrderByNode::ItemVector& keys = node->items();

    // Nothing to sort by: skip
    if (keys.empty()) {
        return _builder.getPendingOutputInterface();
    }

    OrderByProcessor::OrderByKeys orderbyKeys;
    orderbyKeys.reserve(keys.size());

    for (const OrderByItem* key : keys) {
        bioassert(key, "Found null OrderByItem.");

        const Expr* keyExpr = key->getExpr();
        bioassert(keyExpr, "OrderByItem had null expression.");

        const VarDecl* keyDecl = keyExpr->getExprVarDecl();
        bioassert(keyDecl, "OrderByItem had null variable declaration.");

        const auto foundIt = _declToColumn.find(keyDecl);
        if (foundIt == end(_declToColumn)) {
            throw PlannerException(
                fmt::format("Variable {} had no associated column.", keyDecl->getName()));
        }

        const ColumnTag keyTag = foundIt->second;
        const Dataframe* incomingDf = _builder.getPendingOutput().getDataframe();

        const NamedColumn* orderedNamedColumn = incomingDf->getColumn(keyTag);
        bioassert(orderedNamedColumn,
                  "Dataframe did not have column required by ORDER BY.");

        const bool asc = key->getType() == OrderByType::ASC;
        orderbyKeys.emplace_back(keyTag, asc);
    }

    const PipelineBlockOutputInterface& output = _builder.addOrderBy(orderbyKeys);
    Dataframe* outputDf = output.getDataframe();

    // Explictly create a new @ref MaterializeProcessor which uses the sorted output
    // columns of this ORDER BY as its base. This then overrides the behaviour in @ref
    // PipelineGenerator::generate which would otherwise create a MatProc pointing to the
    // input of this processor.
    auto* newMatProc = MaterializeProcessor::createFromDf(_pipeline, _mem, outputDf);
    _builder.setMaterializeProc(newMatProc);

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateCreatePropertyIndexNode(CreatePropertyIndexNode* node) {
    const std::string_view indexName = node->indexName();
    const PropertyExpr* propExpr = node->propExpr();
    bioassert(propExpr, "Invalid property expression.");

    const std::string_view propertyName = propExpr->getPropName();
    const bool isNodeIndex = node->entityKind() == IndexEntityKind::Node;

    _builder.addCreatePropertyIndex(indexName, propertyName, isNodeIndex);

    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateIndexLookupNode(IndexLookupNode* node) {
    if (!_builder.isSingleMaterializeStep()) {
        _builder.addMaterialize();
    }

    const Index* index = node->index();
    bioassert(index, "Null index.");

    const PropertyExpr* propExpr = node->property();
    bioassert(propExpr, "Null property.");

    const VarDecl* entityDecl = propExpr->getEntityVarDecl();
    bioassert(entityDecl, "Null entity.");

    const ValueType propType = node->valueType();
    const EvaluatedType evaluatedType = entityDecl->getType();

    switch (evaluatedType) {
        case EvaluatedType::NodePattern: {
            const auto process = [&]<SupportedType Type> {
                const PipelineValuesOutputInterface& output =
                    _builder.addIndexLookup<typename Type::Primitive, NodeID>(index);
                const NamedColumn* nodeOutput = output.getValues();
                const ColumnTag nodeTag = nodeOutput->getTag();

                const EntityOutputStream nodeStream =
                    EntityOutputStream::createNodeStream(nodeTag);

                _declToColumn[entityDecl] = nodeTag;

                _builder.getPendingOutputInterface()->setStream(nodeStream);
            };
            PropertyTypeDispatcher {propType}.execute(process);
        }
        break;

        case EvaluatedType::EdgePattern: {
            const auto process = [&]<SupportedType Type> {
                [[maybe_unused]] const PipelineValuesOutputInterface& output =
                    _builder.addIndexLookup<typename Type::Primitive, EdgeID>(index);
                // TODO: Make stream
            };
            PropertyTypeDispatcher {propType}.execute(process);
        }
        break;

        default: {
            const std::string_view typeName = EvaluatedTypeName::value(evaluatedType);
            throw PlannerException(fmt::format(
                "IndexLookup must act on a Node/EdgePattern. Instead acting on {}.",
                typeName));
        }
        break;
    }


    return _builder.getPendingOutputInterface();
}

PipelineOutputInterface* PipelineGenerator::translateDropIndexNode(DropIndexNode* node) {
    const std::string_view indexName = node->indexName();
    _builder.addDropIndex(indexName);
    return _builder.getPendingOutputInterface();
}
