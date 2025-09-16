#include "QueryPlanner.h"

#include <span>
#include <spdlog/spdlog.h>
#include <range/v3/view/drop.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/all.hpp>

#include "DataPart.h"
#include "Expr.h"
#include "FilterStep.h"
#include "GetPropertyStep.h"
#include "HistoryStep.h"
#include "ID.h"
#include "MatchTarget.h"
#include "MatchTargets.h"
#include "CreateTarget.h"
#include "LocalMemory.h"
#include "PathPattern.h"
#include "Pipeline.h"
#include "Profiler.h"
#include "QueryCommand.h"
#include "LookupStringIndexStep.h"
#include "ReturnField.h"
#include "ASTContext.h"
#include "ReturnProjection.h"
#include "ScanNodesByPropertyStep.h"
#include "ScanNodesStringApproxStep.h"
#include "TypeConstraint.h"
#include "InjectedIDs.h"
#include "VarDecl.h"

#include "columns/Block.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnMask.h"

#include "columns/ColumnSet.h"
#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
#include "reader/GraphReader.h"
#include "PlannerException.h"

using namespace db;

struct PropertyTypeDispatcher {
    ValueType _valueType;

    auto execute(const auto& executor) const {
        switch (_valueType) {
            case ValueType::Int64:
                return executor.template operator()<types::Int64>();
            case ValueType::UInt64:
                return executor.template operator()<types::UInt64>();
            case ValueType::Double:
                return executor.template operator()<types::Double>();
            case ValueType::String:
                return executor.template operator()<types::String>();
            case ValueType::Bool:
                return executor.template operator()<types::Bool>();
            case ValueType::_SIZE:
            case ValueType::Invalid: {
                throw PlannerException("Unsupported property type");
            }
        }
    }
};

namespace rv = ranges::views;

QueryPlanner::QueryPlanner(const GraphView& view,
                           LocalMemory* mem,
                           QueryCallback callback)
    : _view(view),
    _mem(mem),
    _queryCallback(std::move(callback)),
    _pipeline(std::make_unique<Pipeline>()),
    _output(std::make_unique<Block>()),
    _transformData(std::make_unique<TransformData>(mem))
{
}

QueryPlanner::~QueryPlanner() {
}

bool QueryPlanner::plan(const QueryCommand* query) {
    Profile profile {"QueryPlanner::plan"};

    const auto kind = query->getKind();

    switch (kind) {
        case QueryCommand::Kind::MATCH_COMMAND:
            return planMatch(static_cast<const MatchCommand*>(query));

        case QueryCommand::Kind::CREATE_COMMAND:
            return planCreate(static_cast<const CreateCommand*>(query));

        case QueryCommand::Kind::CREATE_GRAPH_COMMAND:
            return planCreateGraph(static_cast<const CreateGraphCommand*>(query));

        case QueryCommand::Kind::LIST_GRAPH_COMMAND:
            return planListGraph(static_cast<const ListGraphCommand*>(query));

        case QueryCommand::Kind::LOAD_GRAPH_COMMAND:
            return planLoadGraph(static_cast<const LoadGraphCommand*>(query));

        case QueryCommand::Kind::IMPORT_GRAPH_COMMAND:
            return planImportGraph(static_cast<const ImportGraphCommand*>(query));

        case QueryCommand::Kind::EXPLAIN_COMMAND:
            return planExplain(static_cast<const ExplainCommand*>(query));

        case QueryCommand::Kind::HISTORY_COMMAND:
            return planHistory(static_cast<const HistoryCommand*>(query));

        case QueryCommand::Kind::CHANGE_COMMAND:
            return planChange(static_cast<const ChangeCommand*>(query));

        case QueryCommand::Kind::COMMIT_COMMAND:
            return planCommit(static_cast<const CommitCommand*>(query));

        case QueryCommand::Kind::CALL_COMMAND:
            return planCall(static_cast<const CallCommand*>(query));

        case QueryCommand::Kind::S3CONNECT_COMMAND:
            return planS3Connect(static_cast<const S3ConnectCommand*>(query));

        case QueryCommand::Kind::S3TRANSFER_COMMAND:
            return planS3Transfer(static_cast<const S3TransferCommand*>(query));

        default:
            spdlog::error("Unsupported query of kind {}", (unsigned)kind);
            return false;
    }

    panic("Unsupported query command");
}

bool QueryPlanner::planMatch(const MatchCommand* matchCmd) {
    const auto& matchTargets = matchCmd->getMatchTargets()->targets();
    if (matchTargets.size() != 1) {
        spdlog::error("Unsupported MATCH queries with more than one target");
        return false;
    }

    const MatchTarget* matchTarget = matchTargets.front();
    const PathPattern* path = matchTarget->getPattern();
    const auto& pathElements = path->elements();
    const auto pathSize = pathElements.size();

    if (pathSize == 0) {
        spdlog::error("Unsupported path pattern with zero elements");
        return false;
    }

    // Add STOP step in front of the pipeline
    // This is necessary by convention for Executor
    _pipeline->add<StopStep>();

    if (pathElements[0]->getInjectedIDs()) {
        planInjectNodes(pathElements);
    } else {
        planPath(pathElements);
    }
    planTransformStep();
    planProjection(matchCmd);
    planOutputLambda();

    // Add END step
    _pipeline->add<EndStep>();

    return true;
}

void QueryPlanner::planCreateNode(const EntityPattern* nodePattern) {
    VarDecl* nodeDcl = nodePattern->getVar()->getDecl();

    if (!nodeDcl->getColumn()) {
        auto* nodeIDCol = _mem->alloc<ColumnNodeID>();
        nodeDcl->setColumn(nodeIDCol);

        const NodeID nodeID = nodePattern->getEntityID();

        // If there is no previous decl, and no injected ID, create node
        if (!nodeID.isValid() && !nodePattern->getInjectedIDs()) {
            _pipeline->add<CreateNodeStep>(nodePattern);
        } else if (nodeID.isValid()) { // If there is previous decl, use that
            nodeIDCol->set(nodeID);
        } else if (nodePattern->getInjectedIDs()) { // If there is injected ID, use that
            // @ref QueryPlanner::analyzeEntityPattern ensures there is exactly 1
            // injected ID (if any), set this node to have that injected ID
            nodeIDCol->set(nodePattern->getInjectedIDs()->getIDs().front());
        } else {
            throw PlannerException(
                "Could not find previous declaration nor injected ID for node.");
        }
    }
}

void QueryPlanner::planCreateEdges(const PathPattern* pathPattern) {
    std::span pathElements {pathPattern->elements()};

    EntityPattern* src = pathElements[0];
    planCreateNode(src);

    for (auto step : pathElements | rv::drop(1) | rv::chunk(2)) {
        // Create the target + the edge (the source is already created)
        const EntityPattern* edge = step[0];
        VarDecl* edgeDecl = edge->getVar()->getDecl();

        auto edgeID = _mem->alloc<ColumnEdgeID>();
        edgeDecl->setColumn(edgeID);

        EntityPattern* tgt = step[1];
        planCreateNode(tgt);

        _pipeline->add<CreateEdgeStep>(src, edge, tgt);
        src = tgt; // Assign the current target as the source for the next edge
    }
}

bool QueryPlanner::planCreate(const CreateCommand* createCmd) {
    const auto& targets = createCmd->createTargets();
    if (targets.size() == 0) {
        spdlog::error("Unsupported CREATE queries without targets");
        return false;
    }

    // Add STOP step in front of the pipeline
    // This is necessary by convention for Executor
    _pipeline->add<StopStep>();

    for (const CreateTarget* target : targets) {
        const PathPattern* path = target->getPattern();
        std::span pathElements {path->elements()};

        if (pathElements.empty()) {
            spdlog::error("Unsupported path pattern with zero elements");
            return false;
        }
    }

    _pipeline->add<WriteStep>(&targets);

    // Add END step
    _pipeline->add<EndStep>();
    return true;
}

void QueryPlanner::planInjectNodes(const std::vector<EntityPattern*>& path) {
    const auto& injectedNodes = path[0]->getInjectedIDs();
    _result = _mem->alloc<ColumnNodeIDs>(injectedNodes->getIDs());

    const TypeConstraint* injectTypeConstr = path[0]->getTypeConstraint();
    const ExprConstraint* injectExprConstr = path[0]->getExprConstraint();

    if (injectTypeConstr) {
        const LabelSet* injectLabelSet = getOrCreateLabelSet(injectTypeConstr);

        getMatchingLabelSets(_tmpLabelSetIDs, injectLabelSet);

        // If no matching LabelSet, empty result
        if (_tmpLabelSetIDs.empty()) {
            _result = _mem->alloc<ColumnNodeIDs>();
            return;
        }

        auto* nodesLabelSetIDs = _mem->alloc<ColumnVector<LabelSetID>>();
        _pipeline->add<GetLabelSetIDStep>(_result, nodesLabelSetIDs);

        auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();

        // Build filter expression to compute filter for each LabelSetID
        ColumnMask* filterMask {nullptr};
        for (LabelSetID labelSetID : _tmpLabelSetIDs) {
            auto* targetLabelSetID = _mem->alloc<ColumnConst<LabelSetID>>();
            targetLabelSetID->set(labelSetID);

            auto* newMask = _mem->alloc<ColumnMask>();

            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_EQUAL,
                ._mask = newMask,
                ._lhs = nodesLabelSetIDs,
                ._rhs = targetLabelSetID,
            });

            if (filterMask) {
                filter.addExpression(FilterStep::Expression {
                    ._op = ColumnOperator::OP_OR,
                    ._mask = newMask,
                    ._lhs = filterMask,
                    ._rhs = newMask,
                });
            }

            filterMask = newMask;
        }

        // Apply filter to target node IDs
        auto* filterOutNodes = _mem->alloc<ColumnNodeIDs>();
        filter.addOperand(FilterStep::Operand {
            ._mask = filterMask,
            ._src = _result,
            ._dest = filterOutNodes,
        });


        // Assign the filtered nodes column to the general node column
        // pointer so we can iteratively apply filters.
        _result = filterOutNodes;
    }

    if (injectExprConstr) {
        const auto& expressions = injectExprConstr->getExpressions();

        std::vector<ColumnMask*> masks(expressions.size());
        for (auto& mask : masks) {
            mask = _mem->alloc<ColumnMask>();
        }

        generateNodePropertyFilterMasks(masks, expressions, _result);

        auto* filterOutNodes = _mem->alloc<ColumnNodeIDs>();
        auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();

        for (auto* mask : masks) {
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_AND,
                ._mask = masks[0],
                ._lhs = masks[0],
                ._rhs = mask});
        }
        filter.addOperand(FilterStep::Operand {
            ._mask = masks[0],
            ._src = _result,
            ._dest = filterOutNodes});

        _result = filterOutNodes;
    }

    const VarExpr* nodeVar = path[0]->getVar();
    if (nodeVar) {
        VarDecl* nodeDecl = nodeVar->getDecl();
        if (nodeDecl->isUsed()) {
            _transformData->addColumn(_result, nodeDecl);
        }
    }

    const auto expandSteps = path | rv::drop(1) | rv::chunk(2);
    for (auto step : expandSteps) {
        const EntityPattern* edge = step[0];
        const EntityPattern* target = step[1];
        planExpandEdge(edge, target);
    }
}

void QueryPlanner::planPath(const std::vector<EntityPattern*>& path) {
    // We can only do ScanOutEdges/ScanInEdges if we have no edge constraint
    // and there is only a constraint on the target or the source but not both.
    // Otherwise fallback on the scan nodes and expand edges technique
    const auto pathSize = path.size();
    if (pathSize >= 3) {
        const EntityPattern* source = path[0];
        const EntityPattern* edge = path[1];
        const EntityPattern* target = path[2];
        const TypeConstraint* sourceTypeConstr = source->getTypeConstraint();
        const TypeConstraint* edgeTypeConstr = edge->getTypeConstraint();
        const TypeConstraint* targetTypeConstr = target->getTypeConstraint();
        const ExprConstraint* sourceExprConstr = source->getExprConstraint();
        const ExprConstraint* targetExprConstr = target->getExprConstraint();
        const ExprConstraint* edgeExprConstr = edge->getExprConstraint();

        if (!(edgeTypeConstr || edgeExprConstr) && (!sourceTypeConstr || !targetTypeConstr)
            && !(sourceExprConstr || targetExprConstr)) {
            return planPathUsingScanEdges(path);
        }
    }

    // General case: scan nodes for the origin and expand edges afterward
    planScanNodes(path[0]);

    const auto expandSteps = path | rv::drop(1) | rv::chunk(2);
    for (auto step : expandSteps) {
        const EntityPattern* edge = step[0];
        const EntityPattern* target = step[1];
        planExpandEdge(edge, target);
    }
}

void QueryPlanner::planScanNodes(const EntityPattern* entity) {
    auto* nodes = _mem->alloc<ColumnNodeIDs>();

    const TypeConstraint* typeConstr = entity->getTypeConstraint();
    const ExprConstraint* exprConstr = entity->getExprConstraint();

    if (exprConstr && typeConstr) {
        const LabelSet* labelSet = getOrCreateLabelSet(typeConstr);
        planScanNodesWithPropertyAndLabelConstraints(nodes, labelSet, exprConstr);
    } else if (exprConstr) {
        planScanNodesWithPropertyConstraints(nodes, exprConstr);
    } else if (typeConstr) {
        const LabelSet* labelSet = getOrCreateLabelSet(typeConstr);
        _pipeline->add<ScanNodesByLabelStep>(nodes, labelSet->handle());
    } else {
        _pipeline->add<ScanNodesStep>(nodes);
    }

    // Decide if nodes must be written
    const VarExpr* nodeVar = entity->getVar();
    if (nodeVar) {
        VarDecl* nodeDecl = nodeVar->getDecl();
        if (nodeDecl->isUsed()) {
            _transformData->addColumn(nodes, nodeDecl);
        }
    }

    _result = nodes;
}

void QueryPlanner::caseScanNodesStringConstraint(const std::vector<const BinExpr*>& exprs,
                                                 PropertyType propType,
                                                 ColumnNodeIDs* outNodes) {
    auto* filteredScannedNodes = _mem->alloc<ColumnNodeIDs>();
    // If there is a single constraint, the result of the first filter is the output.
    // Otherwise, we use the intermediary @ref filteredScannedNodes to pass to filter
    ColumnNodeIDs* firstStageOutput = exprs.size() == 1 ? outNodes : filteredScannedNodes;

    // Get the value we match against
    const types::String::Primitive queryString =
        static_cast<StringExprConst*>(exprs[0]->getRightExpr())->getVal();

    // We examine the first expression
    BinExpr::OpType firstOperator = exprs[0]->getOpType();

    switch (firstOperator) {
        case BinExpr::OP_EQUAL: { // Scan nodes by property -> filter.
            auto* scannedNodes = _mem->alloc<ColumnNodeIDs>();
            auto* propValues = _mem->alloc<ColumnVector<types::String::Primitive>>();

            auto* filterConstVal = _mem->alloc<ColumnConst<types::String::Primitive>>();
            filterConstVal->set(queryString);

            // Gets nodes that have the property of the type in `propType`, putting the values
            // of these properties into `propValues`
            _pipeline->add<ScanNodesByPropertyStringStep>(scannedNodes, propType, propValues);

            ColumnNodeIDs* firstStageOutput = exprs.size() == 1 ? outNodes : filteredScannedNodes;

            // Mask for the first constraint
            auto* filterMask = _mem->alloc<ColumnMask>();

            // Apply a filter: returning only those nodes who match the constraint string
            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
            // Filter by comparing each node's property value with the queried value
            // Produced output mask in @ref filterMask
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_EQUAL,
                ._mask = filterMask,
                ._lhs = propValues,
                ._rhs = filterConstVal});
            // Apply the mask in @ref filterMask to all nodes (stored in @ref
            // scannedNodes)
            filter.addOperand(FilterStep::Operand {
                ._mask = filterMask,
                ._src = scannedNodes,
                ._dest = firstStageOutput}); // Output to @ref outNodes in the case of single expression
        break;
        }

        case BinExpr::OP_STR_APPROX: { // Scan nodes with index.
            // Only returns nodes which match (no need to filter after scan as above)
            _pipeline->add<ScanNodesStringApproxStep>(firstStageOutput, _view,
                                                     propType._id, queryString);
        break;
        }

        default:
            throw PlannerException("Unsupported operator for type 'String'");
        break;
    }
    // Single expression case: we are done
    if (exprs.size() == 1 ) {
        return;
    }
    // If we are here then we have filtered the nodes based on the first property
    // constraint, but there are more to filter on (i.e. exprs.size() > 1)

    // Allocate masks for the remaining constraints
    std::vector<ColumnMask*> masks(exprs.size() - 1);
    for (auto& mask : masks) {
        mask = _mem->alloc<ColumnMask>();
    }

    // Generate the masks for remaining constraints (operator and type logic performed in
    // here)
    generateNodePropertyFilterMasks(masks,
                                    std::span<const BinExpr* const>(exprs.data() + 1,
                                                                    exprs.size() - 1),
                                    firstStageOutput); // Nodes which match first filter

    // Filter based on all constraints
    auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
    /* Loop over the mask columns, combining all the masks onto the first mask column */
    for (auto* mask : masks | rv::drop(1)) { // Drop first as already filtered
        filter.addExpression(FilterStep::Expression {
            ._op = ColumnOperator::OP_AND,
            ._mask = masks[0],
            ._lhs = masks[0],
            ._rhs = mask});
    }
    // Apply newly created filter masks to the result of the first filter step
    filter.addOperand(FilterStep::Operand {
        ._mask = masks[0],
        ._src = firstStageOutput,
        ._dest = outNodes}); // Final destination is output
}

void QueryPlanner::planScanNodesWithPropertyConstraints(ColumnNodeIDs* const& outputNodes,
                                                        const ExprConstraint* exprConstraint) {
    const auto reader = _view.read();

    const auto& expressions = exprConstraint->getExpressions();

    const VarExpr* leftExpr = static_cast<VarExpr*>(expressions[0]->getLeftExpr());
    ExprConst* rightExpr = static_cast<ExprConst*>(expressions[0]->getRightExpr());
    const std::string& varExprName = leftExpr->getName();

    const auto propTypeRes = reader.getMetadata().propTypes().get(varExprName);
    if (!propTypeRes) {
        throw PlannerException("Property type does not exist");
    }

    // Get the PropertyType (ID and type) which we are scanning for
    const PropertyType propType = propTypeRes.value();

    const auto caseScanNodesPropertyValueType = [&]<SupportedType T>() {
        using Step = ScanNodesByPropertyStep<T>;
        using Val = typename T::Primitive;

        auto* scannedNodes = _mem->alloc<ColumnNodeIDs>();
        auto* filterMask = _mem->alloc<ColumnMask>();
        auto* propValues = _mem->alloc<ColumnVector<Val>>();

        const auto valueType = rightExpr->getType();
        if (valueType != T::_valueType) {
            throw PlannerException("Invalid value type for property member \""
                                   + varExprName + "\"");
        }
        const Val constVal = static_cast<PropertyTypeExprConst<T>::ExprConstType*>(rightExpr)->getVal();
        auto* filterConstVal = _mem->alloc<ColumnConst<Val>>();
        filterConstVal->set(constVal);

        // Gets nodes that have the property of the type in `propType`, putting the values
        // of these properties into `propValues`
        _pipeline->add<Step>(scannedNodes,
                             propType,
                             propValues);

        /* Checking whether we are in the multi-expression
         * constraint case so we can appropriately
         * assign the outputNodes column to the correct step */
        if (expressions.size() > 1) {
            auto* scannedMatchingNodes = _mem->alloc<ColumnNodeIDs>();
            std::vector<ColumnMask*> masks(expressions.size() - 1);
            for (auto& mask : masks) {
                mask = _mem->alloc<ColumnMask>();
            }

            auto& filterScannedNodes = _pipeline->add<FilterStep>().get<FilterStep>();
            filterScannedNodes.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_EQUAL,
                ._mask = filterMask,
                ._lhs = propValues,
                ._rhs = filterConstVal});
            filterScannedNodes.addOperand(FilterStep::Operand {
                ._mask = filterMask,
                ._src = scannedNodes,
                ._dest = scannedMatchingNodes});

            generateNodePropertyFilterMasks(masks,
                                            std::span<const BinExpr* const>(expressions.data() + 1,
                                                                            expressions.size() - 1),
                                            scannedMatchingNodes);

            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();

            /* Loop over the mask columns, combinig all the masks onto the first mask column*/
            for (auto* mask : masks | rv::drop(1)) {
                filter.addExpression(FilterStep::Expression {
                    ._op = ColumnOperator::OP_AND,
                    ._mask = masks[0],
                    ._lhs = masks[0],
                    ._rhs = mask});
            }
            filter.addOperand(FilterStep::Operand {
                ._mask = masks[0],
                ._src = scannedMatchingNodes,
                ._dest = outputNodes});
        } else {
            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
            // Filter by comparing each node's property value with the queried value
            // Produced output mask in @ref filterMask
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_EQUAL,
                ._mask = filterMask,
                ._lhs = propValues,
                ._rhs = filterConstVal});
            // Apply the mask in @ref filterMask to all nodes (stored in @ref
            // scannedNodes), outputting to @ref outputNodes
            filter.addOperand(FilterStep::Operand {
                ._mask = filterMask,
                ._src = scannedNodes,
                ._dest = outputNodes});
        }
    };

    switch (propType._valueType) {
        case ValueType::Int64: {
            caseScanNodesPropertyValueType.operator()<types::Int64>();
        break;
        }

        case ValueType::UInt64: {
            caseScanNodesPropertyValueType.operator()<types::UInt64>();
        break;
        }

        case ValueType::Double: {
            caseScanNodesPropertyValueType.operator()<types::Double>();
        break;
        }

        case ValueType::Bool: {
            caseScanNodesPropertyValueType.operator()<types::Bool>();
        break;
        }

        case ValueType::String: {
            caseScanNodesStringConstraint(expressions, propType, outputNodes);
        break;
        }

        default: {
            throw PlannerException("Unsupported property type for property member");
        }
    }
}

void QueryPlanner::planScanNodesWithPropertyAndLabelConstraints(ColumnNodeIDs* const& outputNodes,
                                                                const LabelSet* labelSet,
                                                                const ExprConstraint* exprConstraint) {
    const auto reader = _view.read();
    auto* scannedNodes = _mem->alloc<ColumnNodeIDs>();

    const auto& expressions = exprConstraint->getExpressions();

    const VarExpr* leftExpr = static_cast<VarExpr*>(expressions[0]->getLeftExpr());
    ExprConst* rightExpr = static_cast<ExprConst*>(expressions[0]->getRightExpr());
    const std::string& varExprName = leftExpr->getName();
    const BinExpr::OpType op = expressions[0]->getOpType();

    const auto propTypeRes = reader.getMetadata().propTypes().get(varExprName);
    if (!propTypeRes) {
        throw PlannerException("Property type does not exist");
    }

    const PropertyType propType = propTypeRes.value();
    auto* filterMask = _mem->alloc<ColumnMask>();

    const auto process = [&]<SupportedType Type> {
        using StepType = ScanNodesByPropertyAndLabel<Type>;
        using Primitive = typename Type::Primitive;

        auto propValues = _mem->alloc<ColumnVector<Primitive>>();
        const Primitive& constVal = static_cast<PropertyTypeExprConst<Type>::ExprConstType*>(rightExpr)->getVal();
        const auto filterConstVal = _mem->alloc<ColumnConst<Primitive>>();
        filterConstVal->set(constVal);

        if (propType._valueType != Type::_valueType) {
            throw PlannerException("Invalid value type for property member \""
                                   + varExprName + "\"");
        }

        _pipeline->add<StepType>(scannedNodes,
                                 propType,
                                 LabelSetHandle {*labelSet},
                                 propValues);

        // Optimisation: if we have more than one expression constraint, then we use the
        // result of the above filter step, which fills the properties and labels into
        // columns, and filter based on those columns. This is only possible for the first
        // expression, because we can only pull the property values for a single property
        // type. For subsequent expression constraints, we generate the masks and filter
        // in @ref generateNodePropertyFilterMasks, which uses an additional step to get
        // the nodes' other property values. We save a single GetProperty step.
        if (expressions.size() > 1) {
            auto* scannedMatchingNodes = _mem->alloc<ColumnNodeIDs>();
            std::vector<ColumnMask*> masks(expressions.size() - 1);
            for (auto& mask : masks) {
                mask = _mem->alloc<ColumnMask>();
            }

            auto& filterScannedNodes = _pipeline->add<FilterStep>().get<FilterStep>();
            if (op == BinExpr::OP_EQUAL) {
                filterScannedNodes.addExpression(
                    FilterStep::Expression {._op = ColumnOperator::OP_EQUAL,
                                            ._mask = filterMask,
                                            ._lhs = propValues,
                                            ._rhs = filterConstVal});
                filterScannedNodes.addOperand(
                    FilterStep::Operand {._mask = filterMask,
                                         ._src = scannedNodes,
                                         ._dest = scannedMatchingNodes});
            } else if (op == BinExpr::OP_STR_APPROX) {
                const std::string& queryString = static_cast<StringExprConst*>(rightExpr)->getVal();
                auto* lookupSet = _mem->alloc<ColumnSet<NodeID>>();
                _pipeline->add<LookupNodeIndexStep>(lookupSet, _view, propType._id, queryString);

                // Fill filtermask[i] with a mask for the Nodes that match approx
                auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
                filter.addExpression(FilterStep::Expression {._op = ColumnOperator::OP_IN,
                                                             ._mask = filterMask,
                                                             ._lhs = scannedNodes,
                                                             ._rhs = lookupSet});
                filter.addOperand(FilterStep::Operand {
                    ._mask = filterMask,
                    ._src = scannedNodes,
                    ._dest = scannedMatchingNodes});
            }
            generateNodePropertyFilterMasks(masks,
                                            std::span<const BinExpr* const>(expressions.data() + 1,
                                                                            expressions.size() - 1),
                                            scannedMatchingNodes);

            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
            for (auto* mask : masks) {
                filter.addExpression(FilterStep::Expression {
                    ._op = ColumnOperator::OP_AND,
                    ._mask = masks[0],
                    ._lhs = masks[0],
                    ._rhs = mask});
            }
            filter.addOperand(FilterStep::Operand {
                ._mask = masks[0],
                ._src = scannedMatchingNodes,
                ._dest = outputNodes});
        } else {
            // Special case: use string approx operator with single expression
            if (op == BinExpr::OP_STR_APPROX) {
                const std::string& queryString = static_cast<StringExprConst*>(rightExpr)->getVal();
                // Step to generate lookup set with Nodes that match
                auto* lookupSet = _mem->alloc<ColumnSet<NodeID>>();
                _pipeline->add<LookupNodeIndexStep>(lookupSet, _view, propType._id, queryString);

                // Fill filtermask[i] with a mask for the Nodes that match approx
                auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
                filter.addExpression(FilterStep::Expression {._op = ColumnOperator::OP_IN,
                                                             ._mask = filterMask,
                                                             ._lhs = scannedNodes,
                                                             ._rhs = lookupSet});
                filter.addOperand(FilterStep::Operand {
                    ._mask = filterMask,
                    ._src = scannedNodes,
                    ._dest = outputNodes});
                return;
            }
            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_EQUAL,
                ._mask = filterMask,
                ._lhs = propValues,
                ._rhs = filterConstVal});
            filter.addOperand(FilterStep::Operand {
                ._mask = filterMask,
                ._src = scannedNodes,
                ._dest = outputNodes});
        }
    };

    PropertyTypeDispatcher {propType._valueType}.execute(process);
}

void QueryPlanner::generateNodePropertyFilterMasks(std::vector<ColumnMask*> filterMasks,
                                                   const std::span<const BinExpr* const> expressions,
                                                   const ColumnNodeIDs* entities) {
    const auto reader = _view.read();

    for (size_t i = 0; i < filterMasks.size(); i++) {
        auto* indices = _mem->alloc<ColumnVector<size_t>>();

        const VarExpr* leftExpr = static_cast<VarExpr*>(expressions[i]->getLeftExpr());
        ExprConst* rightExpr = static_cast<ExprConst*>(expressions[i]->getRightExpr());
        const BinExpr::OpType op = expressions[i]->getOpType();

        const std::string& varExprName = leftExpr->getName();
        const auto propTypeRes = reader.getMetadata().propTypes().get(varExprName);
        if (!propTypeRes) {
            throw PlannerException("Property type does not exist");
        }

        const PropertyType propType = propTypeRes.value();
        auto* mask = filterMasks[i];
        
        if (op == BinExpr::OP_STR_APPROX) {
            const std::string& queryString = static_cast<StringExprConst*>(rightExpr)->getVal();

            // Step to generate lookup set with Node/EdgeIDs that match
            auto* lookupSet = _mem->alloc<ColumnSet<NodeID>>();
            _pipeline->add<LookupNodeIndexStep>(lookupSet, _view, propType._id, queryString);

            // Fill filtermask[i] with a mask for the Nodes/Edges that match approx
            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
            filter.addExpression(FilterStep::Expression {._op = ColumnOperator::OP_IN,
                                                         ._mask = filterMasks[i],
                                                         ._lhs = entities,
                                                         ._rhs = lookupSet});
            return;
        }

        const auto process = [&]<SupportedType Type>() {
            using StepType = GetFilteredNodePropertyStep<Type>;
            using Primitive = typename Type::Primitive;

            auto* propValFilterMask = _mem->alloc<ColumnMask>();
            auto* propValues = _mem->alloc<ColumnVector<Primitive>>();
            auto* filterConstVal = _mem->alloc<ColumnConst<Primitive>>();

            const Primitive& constVal = static_cast<PropertyTypeExprConst<Type>::ExprConstType*>(rightExpr)->getVal();
            filterConstVal->set(constVal);

            _pipeline->add<StepType>(entities,
                                     propType,
                                     propValues,
                                     indices,
                                     mask);
            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_EQUAL,
                ._mask = propValFilterMask,
                ._lhs = propValues,
                ._rhs = filterConstVal});
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_PROJECT,
                ._mask = mask,
                ._lhs = indices,
                ._rhs = propValFilterMask});
        };

        PropertyTypeDispatcher {propType._valueType}.execute(process);
    }
}


void QueryPlanner::generateEdgePropertyFilterMasks(std::vector<ColumnMask*> filterMasks,
                                                   const std::span<const BinExpr* const> expressions,
                                                   const ColumnEdgeIDs* entities) {
    const auto reader = _view.read();

    for (size_t i = 0; i < filterMasks.size(); i++) {
        auto* indices = _mem->alloc<ColumnVector<size_t>>();

        const VarExpr* leftExpr = static_cast<VarExpr*>(expressions[i]->getLeftExpr());
        ExprConst* rightExpr = static_cast<ExprConst*>(expressions[i]->getRightExpr());
        BinExpr::OpType op = expressions[i]->getOpType();

        const std::string& varExprName = leftExpr->getName();
        const auto propTypeRes = reader.getMetadata().propTypes().get(varExprName);
        if (!propTypeRes) {
            throw PlannerException("Property type does not exist");
        }

        const PropertyType propType = propTypeRes.value();

        if (op == BinExpr::OP_STR_APPROX) {
            const std::string& queryString = static_cast<StringExprConst*>(rightExpr)->getVal();

            // Get the set of matching nodes/edges
            auto* lookupSet = _mem->alloc<ColumnSet<EdgeID>>();

            // Step to generate lookup set with Node/EdgeIDs that match
            _pipeline->add<LookupEdgeIndexStep>(lookupSet, _view, propType._id, queryString);

            // Fill filtermask[i] with a mask for the Nodes/Edges that match approx
            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
            filter.addExpression(FilterStep::Expression {._op = ColumnOperator::OP_IN,
                                                         ._mask = filterMasks[i],
                                                         ._lhs = entities,
                                                         ._rhs = lookupSet});
            return;
        }

        const auto process = [&]<SupportedType Type>() {
            using StepType = GetFilteredEdgePropertyStep<Type>;
            using Primitive = typename Type::Primitive;

            auto* propValues = _mem->alloc<ColumnVector<Primitive>>();
            auto* propValFilterMask = _mem->alloc<ColumnMask>();
            auto* filterConstVal = _mem->alloc<ColumnConst<Primitive>>();

            const Primitive& constVal = static_cast<PropertyTypeExprConst<Type>::ExprConstType*>(rightExpr)->getVal();
            filterConstVal->set(constVal);

            _pipeline->add<StepType>(entities,
                                     propType,
                                     propValues,
                                     indices,
                                     filterMasks[i]);

            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_EQUAL,
                ._mask = propValFilterMask,
                ._lhs = propValues,
                ._rhs = filterConstVal});
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_PROJECT,
                ._mask = filterMasks[i],
                ._lhs = indices,
                ._rhs = propValFilterMask});
        };

        PropertyTypeDispatcher {propType._valueType}.execute(process);
    }
}

void QueryPlanner::planExpandEdge(const EntityPattern* edge, const EntityPattern* target) {
    const TypeConstraint* edgeTypeConstr = edge->getTypeConstraint();
    const TypeConstraint* targetTypeConstr = target->getTypeConstraint();
    if (edgeTypeConstr && targetTypeConstr) {
        planExpandEdgeWithEdgeAndTargetConstraint(edge, target);
    } else if (edgeTypeConstr) {
        planExpandEdgeWithEdgeConstraint(edge, target);
    } else if (targetTypeConstr) {
        planExpandEdgeWithTargetConstraint(edge, target);
    } else {
        planExpandEdgeWithNoConstraint(edge, target);
    }
}

void QueryPlanner::planPathUsingScanEdges(const std::vector<EntityPattern*>& path) {
    // Initial step
    const EntityPattern* source = path[0];
    const EntityPattern* edge = path[1];
    const EntityPattern* target = path[2];
    planScanEdges(source, edge, target);

    // Expand steps
    const auto expandSteps = path | rv::drop(3) | rv::chunk(2);
    for (auto step : expandSteps) {
        const EntityPattern* edge = step[0];
        const EntityPattern* target = step[1];
        planExpandEdge(edge, target);
    }
}

void QueryPlanner::planScanEdges(const EntityPattern* source,
                                 const EntityPattern* edge,
                                 const EntityPattern* target) {
    const TypeConstraint* sourceTypeConstr = source->getTypeConstraint();
    const TypeConstraint* targetTypeConstr = target->getTypeConstraint();

    // Add target node IDs to writer
    EdgeWriteInfo edgeWriteInfo;
    auto* targets = _mem->alloc<ColumnNodeIDs>();
    edgeWriteInfo._targetNodes = targets;

    const VarExpr* targetVar = target->getVar();
    if (targetVar) {
        VarDecl* targetDecl = targetVar->getDecl();
        if (targetDecl->isUsed()) {
            _transformData->addColumn(targets, targetDecl);
        }
    }

    // Generate sourceIDs only if the source is returned by projection
    const VarExpr* sourceVar = source->getVar();
    if (sourceVar) {
        VarDecl* sourceDecl = sourceVar->getDecl();
        if (sourceDecl->isUsed()) {
            auto* sources = _mem->alloc<ColumnNodeIDs>();
            edgeWriteInfo._sourceNodes = sources;
            _transformData->addColumn(sources, sourceDecl);
        }
    }

    // Generate edgeIDs only if the edge is returned by projection
    const VarExpr* edgeVar = edge->getVar();
    if (edgeVar) {
        VarDecl* edgeDecl = edgeVar->getDecl();
        if (edgeDecl->isUsed()) {
            auto* edges = _mem->alloc<ColumnEdgeIDs>();
            edgeWriteInfo._edges = edges;
            _transformData->addColumn(edges, edgeDecl);
        }
    }

    if (!sourceTypeConstr && !targetTypeConstr) {
        _pipeline->add<ScanEdgesStep>(edgeWriteInfo);
    } else if (!sourceTypeConstr) {
        const LabelSet* labelSet = getOrCreateLabelSet(targetTypeConstr);
        _pipeline->add<ScanInEdgesByLabelStep>(edgeWriteInfo, LabelSetHandle {*labelSet});
    } else {
        const LabelSet* labelSet = getOrCreateLabelSet(sourceTypeConstr);
        _pipeline->add<ScanOutEdgesByLabelStep>(edgeWriteInfo, LabelSetHandle {*labelSet});
    }

    _result = targets;
}

const LabelSet* QueryPlanner::getOrCreateLabelSet(const TypeConstraint* typeConstr) {
    LabelNames labelStrings;

    // Collect strings of all label names in typeConstr
    for (const VarExpr* nameExpr : typeConstr->getTypeNames()) {
        labelStrings.push_back(&nameExpr->getName());
    }

    const auto labelSetCacheIt = _labelSetCache.find(labelStrings);
    if (labelSetCacheIt != _labelSetCache.end()) {
        return labelSetCacheIt->second;
    }

    const LabelSet* labelSet = buildLabelSet(labelStrings);
    _labelSetCache[labelStrings] = labelSet;

    return labelSet;
}

const LabelSet* QueryPlanner::buildLabelSet(const LabelNames& labelNames) {
    auto labelSet = std::make_unique<LabelSet>();
    for (const std::string* labelName : labelNames) {
        const LabelID labelID = getLabel(*labelName);

        if (!labelID.isValid()) {
            throw PlannerException("Unsupported node label: " + *labelName);
        }

        labelSet->set(labelID);
    }

    auto* ptr = labelSet.get();
    _labelSets.push_back(std::move(labelSet));

    return ptr;
}

LabelID QueryPlanner::getLabel(const std::string& labelName) {
    auto res = _view.metadata().labels().get(labelName);
    if (!res) {
        return LabelID {};
    }

    return res.value();
}

bool QueryPlanner::planCreateGraph(const CreateGraphCommand* createCmd) {
    _pipeline->add<StopStep>();
    _pipeline->add<CreateGraphStep>(createCmd->getName());
    _pipeline->add<EndStep>();
    return true;
}

bool QueryPlanner::planListGraph(const ListGraphCommand* listCmd) {
    auto* graphNames = _mem->alloc<ColumnVector<std::string_view>>();

    _pipeline->add<StopStep>();
    _pipeline->add<ListGraphStep>(graphNames);
    _output->addColumn(graphNames);
    planOutputLambda();
    _pipeline->add<EndStep>();

    return true;
}

void QueryPlanner::getMatchingLabelSets(std::vector<LabelSetID>& labelSets,
                                        const LabelSet* targetLabelSet) {
    labelSets.clear();

    for (const auto& [id, labelset] : _view.metadata().labelsets()) {
        if (labelset->hasAtLeastLabels(*targetLabelSet)) {
            labelSets.emplace_back(id);
        }
    }
}

void QueryPlanner::planExpandEdgeWithNoConstraint(const EntityPattern* edge,
                                                  const EntityPattern* target) {
    auto* indices = _mem->alloc<ColumnVector<size_t>>();

    const ExprConstraint* targetExprConstr = target->getExprConstraint();
    const VarExpr* targetVar = target->getVar();
    VarDecl* targetDecl = targetVar ? targetVar->getDecl() : nullptr;
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isUsed();

    const ExprConstraint* edgeExprConstr = edge->getExprConstraint();
    const VarExpr* edgeVar = edge->getVar();
    VarDecl* edgeDecl = edgeVar ? edgeVar->getDecl() : nullptr;
    const bool mustWriteEdges = edgeDecl && edgeDecl->isUsed();

    EdgeWriteInfo edgeWriteInfo;
    edgeWriteInfo._indices = indices;

    _transformData->createStep(indices);

    // We always need target node IDs
    auto* targets = _mem->alloc<ColumnNodeIDs>();
    edgeWriteInfo._targetNodes = targets;

    if (mustWriteEdges || edgeExprConstr) {
        auto* edges = _mem->alloc<ColumnEdgeIDs>();
        edgeWriteInfo._edges = edges;
    }

    _pipeline->add<GetOutEdgesStep>(_result, edgeWriteInfo);


    if (edgeExprConstr || targetExprConstr) {
        planExpressionConstraintFilters(edgeExprConstr, targetExprConstr, edgeWriteInfo._edges, edgeWriteInfo._targetNodes,
                                        edgeDecl, targetDecl, mustWriteEdges, mustWriteTargetNodes);
    } else {
        // Generate edgeIDs only if the edge is returned by projection
        if (mustWriteEdges) {
            _transformData->addColumn(edgeWriteInfo._edges, edgeDecl);
        }

        if (mustWriteTargetNodes) {
            _transformData->addColumn(edgeWriteInfo._targetNodes, targetDecl);
        }
        _result = edgeWriteInfo._targetNodes;
    }
}

void QueryPlanner::planExpressionConstraintFilters(const ExprConstraint* edgeExprConstr, const ExprConstraint* targetExprConstr,
                                                   const ColumnEdgeIDs* edges, const ColumnNodeIDs* targetNodes, VarDecl* edgeDecl, VarDecl* targetDecl,
                                                   const bool mustWriteEdges, const bool mustWriteTargetNodes) {
    auto* filteredIndices = _mem->alloc<ColumnVector<size_t>>();
    auto* filteredNodes = _mem->alloc<ColumnNodeIDs>();

    std::vector<std::vector<ColumnMask*>> filterMasks;

    if (edgeExprConstr) {
        const auto& expressions = edgeExprConstr->getExpressions();
        filterMasks.emplace_back(expressions.size());
        for (auto& mask : filterMasks.back()) {
            mask = _mem->alloc<ColumnMask>();
        }
        generateEdgePropertyFilterMasks(filterMasks.back(),
                                        std::span<const BinExpr* const>(expressions),
                                        edges);
    }

    if (targetExprConstr) {
        const auto& expressions = targetExprConstr->getExpressions();
        filterMasks.emplace_back(expressions.size());
        for (auto& mask : filterMasks.back()) {
            mask = _mem->alloc<ColumnMask>();
        }
        generateNodePropertyFilterMasks(filterMasks.back(),
                                        std::span<const BinExpr* const>(expressions),
                                        targetNodes);
    }

    auto& filter = _pipeline->add<FilterStep>(filteredIndices).get<FilterStep>();

    for (size_t i = 0; i < filterMasks.size(); i++) {
        for (size_t j = 0; j < filterMasks[i].size(); j++) {
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_AND,
                ._mask = filterMasks[0][0],
                ._lhs = filterMasks[0][0],
                ._rhs = filterMasks[i][j],
            });
        }
    }

    filter.addOperand(FilterStep::Operand {
        ._mask = filterMasks[0][0],
        ._src = targetNodes,
        ._dest = filteredNodes,
    });

    _transformData->createStep(filteredIndices);

    // Generate edgeIDs only if the edge is returned by projection
    if (mustWriteEdges) {
        auto* filteredEdges = _mem->alloc<ColumnEdgeIDs>();
        filter.addOperand(FilterStep::Operand {
            ._mask = filterMasks[0][0],
            ._src = edges,
            ._dest = filteredEdges,
        });
        _transformData->addColumn(filteredEdges, edgeDecl);
    }
    // Add targets to the writeSet
    if (mustWriteTargetNodes) {
        _transformData->addColumn(filteredNodes, targetDecl);
    }

    _result = filteredNodes;
}

void QueryPlanner::planExpandEdgeWithEdgeConstraint(const EntityPattern* edge,
                                                    const EntityPattern* target) {
    const TypeConstraint* edgeTypeConstr = edge->getTypeConstraint();

    auto* indices = _mem->alloc<ColumnVector<size_t>>();
    auto* targets = _mem->alloc<ColumnNodeIDs>();
    auto* edges = _mem->alloc<ColumnEdgeIDs>();
    auto* edgeTypeIDs = _mem->alloc<ColumnVector<EdgeTypeID>>();

    _transformData->createStep(indices);

    const auto& typeConstrNames = edgeTypeConstr->getTypeNames();

    const std::string& edgeTypeName = typeConstrNames.front()->getName();

    // Search edge type IDs
    const auto& edgeTypeMap = _view.metadata().edgeTypes();
    const auto edgeTypeID = edgeTypeMap.get(edgeTypeName);
    if (!edgeTypeID) {
        _result = targets;
        return;
    }

    // Get out edges step
    EdgeWriteInfo edgeWriteInfo;
    edgeWriteInfo._indices = indices;
    edgeWriteInfo._targetNodes = targets;
    edgeWriteInfo._edges = edges;
    edgeWriteInfo._edgeTypes = edgeTypeIDs;

    _pipeline->add<GetOutEdgesStep>(_result, edgeWriteInfo);

    // Filter out edges that do not match the edge type ID
    auto* filterEdgeTypeID = _mem->alloc<ColumnConst<EdgeTypeID>>();
    filterEdgeTypeID->set(edgeTypeID.value());

    auto* filterIndices = _mem->alloc<ColumnVector<size_t>>();
    auto& filter = _pipeline->add<FilterStep>(filterIndices).get<FilterStep>();

    auto* filterMask = _mem->alloc<ColumnMask>();
    filter.addExpression(FilterStep::Expression {
        ._op = ColumnOperator::OP_EQUAL,
        ._mask = filterMask,
        ._lhs = edgeTypeIDs,
        ._rhs = filterEdgeTypeID,
    });

    _transformData->createStep(filterIndices);

    // Apply filter to target node IDs
    auto* filterOutNodes = _mem->alloc<ColumnNodeIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = targets,
        ._dest = filterOutNodes,
    });

    auto* filterOutEdges = _mem->alloc<ColumnEdgeIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = edgeWriteInfo._edges,
        ._dest = filterOutEdges,
    });

    const VarExpr* targetVar = target->getVar();
    VarDecl* targetDecl = targetVar ? targetVar->getDecl() : nullptr;
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isUsed();

    const VarExpr* edgeVar = edge->getVar();
    VarDecl* edgeDecl = edgeVar ? edgeVar->getDecl() : nullptr;
    const bool mustWriteEdges = edgeDecl && edgeDecl->isUsed();

    const ExprConstraint* edgeExprConstr = edge->getExprConstraint();
    const ExprConstraint* targetExprConstr = target->getExprConstraint();

    if (edgeExprConstr || targetExprConstr) {
        planExpressionConstraintFilters(edgeExprConstr, targetExprConstr, filterOutEdges, filterOutNodes,
                                        edgeDecl, targetDecl, mustWriteEdges, mustWriteTargetNodes);
    } else {
        // Generate edgeIDs only if the edge is returned by projection
        if (mustWriteEdges) {
            _transformData->addColumn(filterOutEdges, edgeDecl);
        }

        if (mustWriteTargetNodes) {
            _transformData->addColumn(filterOutNodes, targetDecl);
        }

        _result = filterOutNodes;
    }
}

void QueryPlanner::planExpandEdgeWithEdgeAndTargetConstraint(const EntityPattern* edge,
                                                             const EntityPattern* target) {
    const TypeConstraint* edgeTypeConstr = edge->getTypeConstraint();
    const TypeConstraint* targetTypeConstr = target->getTypeConstraint();

    auto* indices = _mem->alloc<ColumnVector<size_t>>();
    auto* targets = _mem->alloc<ColumnNodeIDs>();
    auto* edges = _mem->alloc<ColumnEdgeIDs>();
    auto* edgeTypeIDs = _mem->alloc<ColumnVector<EdgeTypeID>>();

    _transformData->createStep(indices);

    const auto& typeConstrNames = edgeTypeConstr->getTypeNames();

    const std::string& edgeTypeName = typeConstrNames.front()->getName();

    // Search edge type IDs
    const auto& edgeTypeMap = _view.metadata().edgeTypes();
    const auto edgeTypeID = edgeTypeMap.get(edgeTypeName);

    if (!edgeTypeID) {
        _result = targets;
        return;
    }

    // Search label set IDs
    const LabelSet* targetLabelSet = getOrCreateLabelSet(targetTypeConstr);
    getMatchingLabelSets(_tmpLabelSetIDs, targetLabelSet);
    if (_tmpLabelSetIDs.empty()) {
        _result = targets;
        return;
    }

    // Get out edges step
    EdgeWriteInfo edgeWriteInfo;
    edgeWriteInfo._indices = indices;
    edgeWriteInfo._targetNodes = targets;
    edgeWriteInfo._edges = edges;
    edgeWriteInfo._edgeTypes = edgeTypeIDs;

    _pipeline->add<GetOutEdgesStep>(_result, edgeWriteInfo);

    // Get label set IDs of target nodes
    auto* nodesLabelSetIDs = _mem->alloc<ColumnVector<LabelSetID>>();
    _pipeline->add<GetLabelSetIDStep>(targets, nodesLabelSetIDs);

    // Filter out edges that do not match the edge type ID
    auto* filterEdgeTypeID = _mem->alloc<ColumnConst<EdgeTypeID>>();
    filterEdgeTypeID->set(edgeTypeID.value());

    auto* filterIndices = _mem->alloc<ColumnVector<size_t>>();
    auto& filter = _pipeline->add<FilterStep>(filterIndices).get<FilterStep>();

    ColumnMask* filterMaskEdges = _mem->alloc<ColumnMask>();
    filter.addExpression(FilterStep::Expression {
        ._op = ColumnOperator::OP_EQUAL,
        ._mask = filterMaskEdges,
        ._lhs = edgeTypeIDs,
        ._rhs = filterEdgeTypeID,
    });

    ColumnMask* filterMaskNodes = nullptr;
    for (LabelSetID labelSetID : _tmpLabelSetIDs) {
        auto* targetLabelSetID = _mem->alloc<ColumnConst<LabelSetID>>();
        targetLabelSetID->set(labelSetID);

        auto* newMask = _mem->alloc<ColumnMask>();

        filter.addExpression(FilterStep::Expression {
            ._op = ColumnOperator::OP_EQUAL,
            ._mask = newMask,
            ._lhs = nodesLabelSetIDs,
            ._rhs = targetLabelSetID,
        });

        if (filterMaskNodes) {
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_OR,
                ._mask = newMask,
                ._lhs = filterMaskNodes,
                ._rhs = newMask,
            });
        }

        filterMaskNodes = newMask;
    }

    // Combine filter masks
    auto* filterMask = _mem->alloc<ColumnMask>();
    filter.addExpression(FilterStep::Expression {
        ._op = ColumnOperator::OP_AND,
        ._mask = filterMask,
        ._lhs = filterMaskEdges,
        ._rhs = filterMaskNodes,
    });

    _transformData->createStep(filterIndices);

    // Apply filter to target node IDs
    auto* filterOutNodes = _mem->alloc<ColumnNodeIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = targets,
        ._dest = filterOutNodes,
    });

    auto* filterOutEdges = _mem->alloc<ColumnEdgeIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = edgeWriteInfo._edges,
        ._dest = filterOutEdges,
    });

    const VarExpr* targetVar = target->getVar();
    VarDecl* targetDecl = targetVar ? targetVar->getDecl() : nullptr;
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isUsed();

    const VarExpr* edgeVar = edge->getVar();
    VarDecl* edgeDecl = edgeVar ? edgeVar->getDecl() : nullptr;
    const bool mustWriteEdges = edgeDecl && edgeDecl->isUsed();

    const ExprConstraint* edgeExprConstr = edge->getExprConstraint();
    const ExprConstraint* targetExprConstr = target->getExprConstraint();

    if (edgeExprConstr || targetExprConstr) {
        planExpressionConstraintFilters(edgeExprConstr, targetExprConstr, filterOutEdges, filterOutNodes,
                                        edgeDecl, targetDecl, mustWriteEdges, mustWriteTargetNodes);
    } else {
        // Generate edgeIDs only if the edge is returned by projection
        if (mustWriteEdges) {
            _transformData->addColumn(filterOutEdges, edgeDecl);
        }

        // Generate nodeIDs only if the node is returned by projection
        if (mustWriteTargetNodes) {
            _transformData->addColumn(filterOutNodes, targetDecl);
        }

        _result = filterOutNodes;
    }
}

void QueryPlanner::planExpandEdgeWithTargetConstraint(const EntityPattern* edge,
                                                      const EntityPattern* target) {
    const TypeConstraint* targetTypeConstr = target->getTypeConstraint();
    auto* indices = _mem->alloc<ColumnVector<size_t>>();
    auto* targets = _mem->alloc<ColumnNodeIDs>();

    _transformData->createStep(indices);

    const LabelSet* targetLabelSet = getOrCreateLabelSet(targetTypeConstr);

    getMatchingLabelSets(_tmpLabelSetIDs, targetLabelSet);

    // If no matching LabelSet, empty result
    if (_tmpLabelSetIDs.empty()) {
        _result = targets;
        return;
    }

    // GetOutEdgesStep
    EdgeWriteInfo edgeWriteInfo;
    edgeWriteInfo._indices = indices;
    edgeWriteInfo._targetNodes = targets;

    const ExprConstraint* edgeExprConstr = edge->getExprConstraint();
    const VarExpr* edgeVar = edge->getVar();
    VarDecl* edgeDecl = edgeVar ? edgeVar->getDecl() : nullptr;
    const bool mustWriteEdges = edgeDecl && edgeDecl->isUsed();

    if (mustWriteEdges || edgeExprConstr) {
        auto* edges = _mem->alloc<ColumnEdgeIDs>();
        edgeWriteInfo._edges = edges;
    }

    _pipeline->add<GetOutEdgesStep>(_result, edgeWriteInfo);

    // Get labels of target nodes
    auto* nodesLabelSetIDs = _mem->alloc<ColumnVector<LabelSetID>>();
    _pipeline->add<GetLabelSetIDStep>(targets, nodesLabelSetIDs);

    // Create filter to filter target nodes based on target label set
    auto* filterIndices = _mem->alloc<ColumnVector<size_t>>();
    auto& filter = _pipeline->add<FilterStep>(filterIndices).get<FilterStep>();

    // Build filter expression to compute filter for each LabelSetID
    ColumnMask* filterMask {nullptr};
    for (LabelSetID labelSetID : _tmpLabelSetIDs) {
        auto* targetLabelSetID = _mem->alloc<ColumnConst<LabelSetID>>();
        targetLabelSetID->set(labelSetID);

        auto* newMask = _mem->alloc<ColumnMask>();

        filter.addExpression(FilterStep::Expression {
            ._op = ColumnOperator::OP_EQUAL,
            ._mask = newMask,
            ._lhs = nodesLabelSetIDs,
            ._rhs = targetLabelSetID,
        });

        if (filterMask) {
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_OR,
                ._mask = newMask,
                ._lhs = filterMask,
                ._rhs = newMask,
            });
        }

        filterMask = newMask;
    }

    _transformData->createStep(filterIndices);

    // Apply filter to target node IDs
    auto* filterOutNodes = _mem->alloc<ColumnNodeIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = targets,
        ._dest = filterOutNodes,
    });

    // Apply filter to edge IDs if necessary
    ColumnEdgeIDs* outputEdges {nullptr};
    if (mustWriteEdges || edgeExprConstr) {
        outputEdges = _mem->alloc<ColumnEdgeIDs>();
        filter.addOperand(FilterStep::Operand {
            ._mask = filterMask,
            ._src = edgeWriteInfo._edges,
            ._dest = outputEdges,
        });
    }

    const ExprConstraint* targetExprConstr = target->getExprConstraint();
    const VarExpr* targetVar = target->getVar();
    VarDecl* targetDecl = targetVar ? targetVar->getDecl() : nullptr;
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isUsed();

    if (edgeExprConstr || targetExprConstr) {
        planExpressionConstraintFilters(edgeExprConstr, targetExprConstr, outputEdges, filterOutNodes,
                                        edgeDecl, targetDecl, mustWriteEdges, mustWriteTargetNodes);
    } else {
        // Generate edgeIDs only if the edge is returned by projection
        if (mustWriteEdges) {
            _transformData->addColumn(outputEdges, edgeDecl);
        }

        if (mustWriteTargetNodes) {
            _transformData->addColumn(filterOutNodes, targetDecl);
        }

        _result = filterOutNodes;
    }
}

bool QueryPlanner::planLoadGraph(const LoadGraphCommand* loadCmd) {
    _pipeline->add<StopStep>();
    _pipeline->add<LoadGraphStep>(loadCmd->getGraphName());
    _pipeline->add<EndStep>();
    return true;
}

bool QueryPlanner::planImportGraph(const ImportGraphCommand* importCmd) {
    _pipeline->add<StopStep>();

    if (importCmd->getGraphName().empty() || importCmd->getFilePath().empty()) {
        throw PlannerException("Invalid graph name or file path");
    }

    _pipeline->add<ImportGraphStep>(importCmd->getFilePath(), importCmd->getGraphName());

    _pipeline->add<EndStep>();
    return true;
}

void QueryPlanner::planProjection(const MatchCommand* matchCmd) {
    const auto& projection = matchCmd->getProjection();
    const auto& matchTargets = matchCmd->getMatchTargets()->targets();

    for (const ReturnField* field : projection->returnFields()) {
        if (field->isAll()) {
            for (const MatchTarget* target : matchTargets) {
                const PathPattern* pattern = target->getPattern();
                for (EntityPattern* entityPattern : pattern->elements()) {
                    if (const VarExpr* var = entityPattern->getVar()) {
                        if (VarDecl* decl = var->getDecl()) {
                            if (decl->getKind() == DeclKind::NODE_DECL) {
                                auto* columnIDs = decl->getColumn()->cast<ColumnNodeIDs>();
                                if (!columnIDs) {
                                    columnIDs = _mem->alloc<ColumnNodeIDs>();
                                }
                                _output->addColumn(columnIDs);
                            } else if (decl->getKind() == DeclKind::EDGE_DECL) {
                                auto* columnIDs = decl->getColumn()->cast<ColumnEdgeIDs>();
                                if (!columnIDs) {
                                    columnIDs = _mem->alloc<ColumnEdgeIDs>();
                                }
                                _output->addColumn(columnIDs);
                            }
                        }
                    }
                }
            }
            continue;
        }

        // Skip variables for which no columnIDs have been generated
        const VarDecl* decl = field->getDecl();
        const auto kind = decl->getKind();
        if (kind == DeclKind::NODE_DECL) {
            auto* column = decl->getColumn()->cast<ColumnNodeIDs>();
            if (!column) {
                // For the case where a plan step breaks out before
                // setting the Decl Column - e.g If an invalid Label
                // is Filtered On.
                column = _mem->alloc<ColumnNodeIDs>();
            }

            const auto& memberName = field->getMemberName();
            if (memberName.empty()) {
                _output->addColumn(column);
            } else {
                planPropertyProjection(column, decl, field);
            }

        } else if (kind == DeclKind::EDGE_DECL) {
            auto* column = decl->getColumn()->cast<ColumnEdgeIDs>();
            if (!column) {
                column = _mem->alloc<ColumnEdgeIDs>();
            }

            const auto& memberName = field->getMemberName();
            if (memberName.empty()) {
                _output->addColumn(column);
            } else {
                planPropertyProjection(column, decl, field);
            }
        }
    }
}

void QueryPlanner::planPropertyProjection(ColumnNodeIDs* columnIDs,
                                          const VarDecl* parentDecl,
                                          const ReturnField* field) {
    const auto& propType = field->getMemberType();

    const auto process = [&]<SupportedType T> {
        auto propValues = _mem->alloc<ColumnOptVector<typename T::Primitive>>();
        _pipeline->add<GetNodePropertyStep<T>>(columnIDs,
                                               propType,
                                               propValues);
        _output->addColumn(propValues);
    };

    PropertyTypeDispatcher {propType._valueType}.execute(process);
}

void QueryPlanner::planPropertyProjection(ColumnEdgeIDs* columnIDs,
                                          const VarDecl* parentDecl,
                                          const ReturnField* field) {
    const auto& propType = field->getMemberType();

    const auto process = [&]<SupportedType T> {
        auto propValues = _mem->alloc<ColumnOptVector<typename T::Primitive>>();
        _pipeline->add<GetEdgePropertyStep<T>>(columnIDs,
                                               propType,
                                               propValues);
        _output->addColumn(propValues);
    };

    PropertyTypeDispatcher {propType._valueType}.execute(process);
}

void QueryPlanner::planTransformStep() {
    _pipeline->add<TransformStep>(_transformData.get());
}

void QueryPlanner::planOutputLambda() {
    _pipeline->add<LambdaStep>([&](LambdaStep* step, LambdaStep::Operation op) {
        if (op == LambdaStep::Operation::EXECUTE) {
            _queryCallback(*_output);
        }
    });
}

bool QueryPlanner::planExplain(const ExplainCommand* explain) {
    // Plan query
    plan(explain->getQuery());

    auto* pipeDescr = _mem->alloc<ColumnVector<std::string>>();

    std::string stepDescr;
    for (const auto& step : _pipeline->steps()) {
        step.describe(stepDescr);
        pipeDescr->emplace_back(stepDescr);
    }

    // Clear pipeline and add explain result
    _pipeline->clear();
    _output->clear();
    _output->addColumn(pipeDescr);

    _pipeline->add<StopStep>();
    planOutputLambda();
    _pipeline->add<EndStep>();

    return true;
}

bool QueryPlanner::planHistory(const HistoryCommand* history) {
    auto* historyLog = _mem->alloc<ColumnVector<std::string>>();

    _pipeline->add<StopStep>();
    _pipeline->add<HistoryStep>(historyLog);
    planOutputLambda();
    _output->addColumn(historyLog);
    _pipeline->add<EndStep>();

    return true;
}

bool QueryPlanner::planChange(const ChangeCommand* cmd) {
    _pipeline->add<StopStep>();

    switch (cmd->getChangeOpType()) {
        case ChangeOpType::LIST:
        case ChangeOpType::NEW: {
            auto* output = _mem->alloc<ColumnVector<const Change*>>();
            _pipeline->add<ChangeStep>(cmd->getChangeOpType(), output);
            _output->addColumn(output);
            planOutputLambda();
            break;
        }

        case ChangeOpType::SUBMIT:
        case ChangeOpType::DELETE: {
            _pipeline->add<ChangeStep>(cmd->getChangeOpType(), nullptr);
            break;
        }

        case ChangeOpType::_SIZE:
            throw PlannerException("Unsupported change operation");
    }

    _pipeline->add<EndStep>();

    return true;
}

bool QueryPlanner::planCommit(const CommitCommand* commit) {
    _pipeline->add<StopStep>();
    _pipeline->add<CommitStep>();
    _pipeline->add<EndStep>();

    return true;
}

bool QueryPlanner::planCall(const CallCommand* call) {

    _pipeline->add<StopStep>();
    switch (call->getType()) {
        case CallCommand::Type::LABELS: {
            auto* ids = _mem->alloc<ColumnVector<LabelID>>();
            auto* name = _mem->alloc<ColumnVector<std::string_view>>();

            _pipeline->add<CallLabelStep>(ids, name);

            _output->addColumn(ids);
            _output->addColumn(name);

            break;
        }
        case CallCommand::Type::EDGETYPES: {
            auto* ids = _mem->alloc<ColumnVector<EdgeTypeID>>();
            auto* name = _mem->alloc<ColumnVector<std::string_view>>();

            _pipeline->add<CallEdgeTypeStep>(ids, name);

            _output->addColumn(ids);
            _output->addColumn(name);

            break;
        }
        case CallCommand::Type::LABELSETS: {
            auto* ids = _mem->alloc<ColumnVector<LabelSetID>>();
            auto* name = _mem->alloc<ColumnVector<std::string_view>>();

            _pipeline->add<CallLabelSetStep>(ids, name);

            _output->addColumn(ids);
            _output->addColumn(name);
            break;
        }
        case CallCommand::Type::PROPERTIES: {
            auto* ids = _mem->alloc<ColumnVector<PropertyTypeID>>();
            auto* name = _mem->alloc<ColumnVector<std::string_view>>();
            auto* type = _mem->alloc<ColumnVector<std::string_view>>();

            _pipeline->add<CallPropertyStep>(ids, name, type);

            _output->addColumn(ids);
            _output->addColumn(name);
            _output->addColumn(type);

            break;
        }
        default:
            spdlog::error("Could not find CALL() type");
            return false;
    }

    planOutputLambda();
    _pipeline->add<EndStep>();

    return true;
}

bool QueryPlanner::planS3Connect(const S3ConnectCommand* s3Connect) {
    _pipeline->add<StopStep>();
    _pipeline->add<S3ConnectStep>(s3Connect->getAccessId(), s3Connect->getSecretKey(), s3Connect->getRegion());
    _pipeline->add<EndStep>();
    return true;
}

bool QueryPlanner::planS3Transfer(const S3TransferCommand* s3Transfer) {
    _pipeline->add<StopStep>();
    if (s3Transfer->getTransferDirection() == S3TransferCommand::Direction::PUSH) {
        _pipeline->add<S3PushStep>(s3Transfer->getBucket(),
                                   s3Transfer->getPrefix(),
                                   s3Transfer->getFile(),
                                   s3Transfer->getLocalDir(),
                                   s3Transfer->getTransferDirectory());
    } else {
        _pipeline->add<S3PullStep>(s3Transfer->getBucket(),
                                   s3Transfer->getPrefix(),
                                   s3Transfer->getFile(),
                                   s3Transfer->getLocalDir(),
                                   s3Transfer->getTransferDirectory());
    }
    _pipeline->add<EndStep>();
    return true;
}
