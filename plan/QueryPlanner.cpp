#include "QueryPlanner.h"

#include <spdlog/spdlog.h>
#include <range/v3/view/drop.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/all.hpp>

#include "Expr.h"
#include "HistoryStep.h"
#include "MatchTarget.h"
#include "LocalMemory.h"
#include "PathPattern.h"
#include "Pipeline.h"
#include "QueryCommand.h"
#include "ReturnField.h"
#include "ReturnProjection.h"
#include "TypeConstraint.h"
#include "VarDecl.h"

#include "columns/Block.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnMask.h"

#include "reader/GraphReader.h"
#include "PlannerException.h"

using namespace db;

namespace rv = ranges::views;

QueryPlanner::QueryPlanner(const GraphView& view, LocalMemory* mem, QueryCallback callback)
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
    const auto kind = query->getKind();

    switch (kind) {
        case QueryCommand::Kind::MATCH_COMMAND:
            return planMatch(static_cast<const MatchCommand*>(query));

        case QueryCommand::Kind::CREATE_GRAPH_COMMAND:
            return planCreateGraph(static_cast<const CreateGraphCommand*>(query));

        case QueryCommand::Kind::LIST_GRAPH_COMMAND:
            return planListGraph(static_cast<const ListGraphCommand*>(query));

        case QueryCommand::Kind::LOAD_GRAPH_COMMAND:
            return planLoadGraph(static_cast<const LoadGraphCommand*>(query));

        case QueryCommand::Kind::EXPLAIN_COMMAND:
            return planExplain(static_cast<const ExplainCommand*>(query));

        case QueryCommand::Kind::HISTORY_COMMAND:
            return planHistory(static_cast<const HistoryCommand*>(query));

        default:
            spdlog::error("Unsupported query of kind {}", (unsigned)kind);
            return false;
    }
}

bool QueryPlanner::planMatch(const MatchCommand* matchCmd) {
    const auto& matchTargets = matchCmd->matchTargets();
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

    planPath(pathElements);
    planTransformStep();
    planProjection(matchCmd);
    planOutputLambda();

    // Add END step
    _pipeline->add<EndStep>();

    return true;
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
    auto* nodes = _mem->alloc<ColumnIDs>();

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
        if (nodeDecl->isReturned()) {
            _transformData->addColumn(nodes, nodeDecl);
        }
    }

    _result = nodes;
}

#define CASE_SCAN_NODES_PROPERTY_VALUE_TYPE(Type)                                                    \
    case ValueType::Type: {                                                                          \
        using StepType = ScanNodesByProperty##Type##Step;                                            \
        using valType = types::Type::Primitive;                                                      \
                                                                                                     \
        auto propValues = _mem->alloc<ColumnVector<valType>>();                                      \
        const valType constVal = static_cast<Type##ExprConst*>(rightExpr)->getVal();                 \
        auto* filterConstVal = _mem->alloc<ColumnConst<valType>>();                                  \
        filterConstVal->set(constVal);                                                               \
                                                                                                     \
        _pipeline->add<StepType>(scannedNodes,                                                       \
                                 propType,                                                           \
                                 propValues);                                                        \
        /* Checking whether we are in the multi-expression constraint case so we can appropriately   \
           assign the outputNodes column to the correct step */                                      \
        if (expressions.size() > 1) {                                                                \
            auto* scannedMatchingNodes = _mem->alloc<ColumnIDs>();                                   \
            std::vector<ColumnMask*> masks(expressions.size() - 1);                                  \
            for (auto& mask : masks) {                                                               \
                mask = _mem->alloc<ColumnMask>();                                                    \
            }                                                                                        \
                                                                                                     \
            auto& filterScannedNodes = _pipeline->add<FilterStep>().get<FilterStep>();               \
            filterScannedNodes.addExpression(FilterStep::Expression {                                \
                ._op = ColumnOperator::OP_EQUAL,                                                     \
                ._mask = filterMask,                                                                 \
                ._lhs = propValues,                                                                  \
                ._rhs = filterConstVal                                                               \
            });                                                                                      \
            filterScannedNodes.addOperand(FilterStep::Operand {                                      \
                ._mask = filterMask,                                                                 \
                ._src = scannedNodes,                                                                \
                ._dest = scannedMatchingNodes                                                        \
            });                                                                                      \
                                                                                                     \
            generateNodePropertyFilterMasks(masks,                                                   \
                                            std::span<const BinExpr* const>(expressions.data() + 1,  \
                                                                            expressions.size() - 1), \
                                            scannedMatchingNodes);                                   \
                                                                                                     \
            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();                           \
                                                                                                     \
            /* Loop over the mask columns, combinig all the masks onto the first mask column*/       \
            for (auto mask : masks | rv::drop(1)) {                                                  \
                filter.addExpression(FilterStep::Expression {                                        \
                    ._op = ColumnOperator::OP_AND,                                                   \
                    ._mask = masks[0],                                                               \
                    ._lhs = masks[0],                                                                \
                    ._rhs = mask                                                                     \
                });                                                                                  \
            }                                                                                        \
            filter.addOperand(FilterStep::Operand {                                                  \
                ._mask = masks[0],                                                                   \
                ._src = scannedMatchingNodes,                                                        \
                ._dest = outputNodes                                                                 \
            });                                                                                      \
        } else {                                                                                     \
            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();                           \
            filter.addExpression(FilterStep::Expression {                                            \
                ._op = ColumnOperator::OP_EQUAL,                                                     \
                ._mask = filterMask,                                                                 \
                ._lhs = propValues,                                                                  \
                ._rhs = filterConstVal                                                               \
            });                                                                                      \
            filter.addOperand(FilterStep::Operand {                                                  \
                ._mask = filterMask,                                                                 \
                ._src = scannedNodes,                                                                \
                ._dest = outputNodes                                                                 \
            });                                                                                      \
        }                                                                                            \
                                                                                                     \
        break;                                                                                       \
    }

#define CASE_SCAN_NODES_PROPERTY_AND_LABEL_VALUE_TYPE(Type)                                          \
    case ValueType::Type: {                                                                          \
        using StepType = ScanNodesByPropertyAndLabel##Type##Step;                                    \
        using valType = types::Type::Primitive;                                                      \
                                                                                                     \
        auto propValues = _mem->alloc<ColumnVector<valType>>();                                      \
        valType constVal = static_cast<Type##ExprConst*>(rightExpr)->getVal();                       \
        const auto filterConstVal = _mem->alloc<ColumnConst<valType>>();                             \
        filterConstVal->set(constVal);                                                               \
                                                                                                     \
        _pipeline->add<StepType>(scannedNodes,                                                       \
                                 propType,                                                           \
                                 LabelSetHandle {*labelSet},                                         \
                                 propValues);                                                        \
                                                                                                     \
        if (expressions.size() > 1) {                                                                \
            auto* scannedMatchingNodes = _mem->alloc<ColumnIDs>();                              \
            std::vector<ColumnMask*> masks(expressions.size() - 1);                                  \
            for (auto& mask : masks) {                                                               \
                mask = _mem->alloc<ColumnMask>();                                                    \
            }                                                                                        \
                                                                                                     \
            auto& filterScannedNodes = _pipeline->add<FilterStep>().get<FilterStep>();               \
            filterScannedNodes.addExpression(FilterStep::Expression {                                \
                ._op = ColumnOperator::OP_EQUAL,                                                     \
                ._mask = filterMask,                                                                 \
                ._lhs = propValues,                                                                  \
                ._rhs = filterConstVal                                                               \
            });                                                                                      \
            filterScannedNodes.addOperand(FilterStep::Operand {                                      \
                ._mask = filterMask,                                                                 \
                ._src = scannedNodes,                                                                \
                ._dest = scannedMatchingNodes                                                        \
            });                                                                                      \
                                                                                                     \
            generateNodePropertyFilterMasks(masks,                                                   \
                                            std::span<const BinExpr* const>(expressions.data() + 1,  \
                                                                            expressions.size() - 1), \
                                            scannedMatchingNodes);                                   \
                                                                                                     \
            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();                           \
            for (auto mask : masks) {                                                                \
                filter.addExpression(FilterStep::Expression {                                        \
                    ._op = ColumnOperator::OP_AND,                                                   \
                    ._mask = masks[0],                                                               \
                    ._lhs = masks[0],                                                                \
                    ._rhs = mask                                                                     \
                });                                                                                  \
            }                                                                                        \
            filter.addOperand(FilterStep::Operand {                                                  \
                ._mask = masks[0],                                                                   \
                ._src = scannedMatchingNodes,                                                        \
                ._dest = outputNodes                                                                 \
            });                                                                                      \
        } else {                                                                                     \
            auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();                           \
            filter.addExpression(FilterStep::Expression {                                            \
                ._op = ColumnOperator::OP_EQUAL,                                                     \
                ._mask = filterMask,                                                                 \
                ._lhs = propValues,                                                                  \
                ._rhs = filterConstVal                                                               \
            });                                                                                      \
            filter.addOperand(FilterStep::Operand {                                                  \
                ._mask = filterMask,                                                                 \
                ._src = scannedNodes,                                                                \
                ._dest = outputNodes                                                                 \
            });                                                                                      \
        }                                                                                            \
                                                                                                     \
        break;                                                                                       \
    }

void QueryPlanner::planScanNodesWithPropertyConstraints(ColumnIDs* const& outputNodes, const ExprConstraint* exprConstraint) {
    const auto reader = _view.read();
    auto* scannedNodes = _mem->alloc<ColumnIDs>();

    const auto& expressions = exprConstraint->getExpressions();

    const VarExpr* leftExpr = static_cast<VarExpr*>(expressions[0]->getLeftExpr());
    ExprConst* rightExpr = static_cast<ExprConst*>(expressions[0]->getRightExpr());
    const std::string& varExprName = leftExpr->getName();

    const auto propTypeRes = reader.getMetadata().propTypes().get(varExprName);
    if (!propTypeRes) {
        throw PlannerException("Property type does not exist");
    }

    const PropertyType propType = propTypeRes.value();

    auto* filterMask = _mem->alloc<ColumnMask>();

    switch (propType._valueType) {
        CASE_SCAN_NODES_PROPERTY_VALUE_TYPE(Int64)
        CASE_SCAN_NODES_PROPERTY_VALUE_TYPE(UInt64)
        CASE_SCAN_NODES_PROPERTY_VALUE_TYPE(Double)
        CASE_SCAN_NODES_PROPERTY_VALUE_TYPE(String)
        CASE_SCAN_NODES_PROPERTY_VALUE_TYPE(Bool)
        default: {
            throw PlannerException("Unsupported property type for property member");
        }
    }
}

void QueryPlanner::planScanNodesWithPropertyAndLabelConstraints(ColumnIDs* const& outputNodes, const LabelSet* labelSet, const ExprConstraint* exprConstraint) {
    const auto reader = _view.read();
    auto* scannedNodes = _mem->alloc<ColumnIDs>();

    const auto& expressions = exprConstraint->getExpressions();

    const VarExpr* leftExpr = static_cast<VarExpr*>(expressions[0]->getLeftExpr());
    ExprConst* rightExpr = static_cast<ExprConst*>(expressions[0]->getRightExpr());
    const std::string& varExprName = leftExpr->getName();

    const auto propTypeRes = reader.getMetadata().propTypes().get(varExprName);
    if (!propTypeRes) {
        throw PlannerException("Property type does not exist");
    }

    const PropertyType propType = propTypeRes.value();
    auto* filterMask = _mem->alloc<ColumnMask>();

    switch (propType._valueType) {
        CASE_SCAN_NODES_PROPERTY_AND_LABEL_VALUE_TYPE(Int64)
        CASE_SCAN_NODES_PROPERTY_AND_LABEL_VALUE_TYPE(UInt64)
        CASE_SCAN_NODES_PROPERTY_AND_LABEL_VALUE_TYPE(Double)
        CASE_SCAN_NODES_PROPERTY_AND_LABEL_VALUE_TYPE(String)
        CASE_SCAN_NODES_PROPERTY_AND_LABEL_VALUE_TYPE(Bool)
        default: {
            throw PlannerException("Unsupported property type for property member");
        }
    }
}


#define CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(NodeOrEdge, Type)                \
    case ValueType::Type: {                                                    \
        using StepType = GetFiltered##NodeOrEdge##Property##Type##Step;        \
        using valType = types::Type::Primitive;                                \
                                                                               \
        const auto propValues = _mem->alloc<ColumnVector<valType>>();          \
        const auto propValFilterMask = _mem->alloc<ColumnMask>();              \
        const auto filterConstVal = _mem->alloc<ColumnConst<valType>>();       \
                                                                               \
        valType constVal = static_cast<Type##ExprConst*>(rightExpr)->getVal(); \
        filterConstVal->set(constVal);                                         \
                                                                               \
        _pipeline->add<StepType>(entities,                                     \
                                 propType,                                     \
                                 propValues,                                   \
                                 indices,                                      \
                                 filterMasks[i]);                              \
                                                                               \
        auto& filter = _pipeline->add<FilterStep>().get<FilterStep>();         \
        filter.addExpression(FilterStep::Expression {                          \
            ._op = ColumnOperator::OP_EQUAL,                                   \
            ._mask = propValFilterMask,                                        \
            ._lhs = propValues,                                                \
            ._rhs = filterConstVal                                             \
        });                                                                    \
        filter.addExpression(FilterStep::Expression {                          \
            ._op = ColumnOperator::OP_PROJECT,                                 \
            ._mask = filterMasks[i],                                           \
            ._lhs = indices,                                                   \
            ._rhs = propValFilterMask                                          \
        });                                                                    \
                                                                               \
        break;                                                                 \
    }

void QueryPlanner::generateNodePropertyFilterMasks(std::vector<ColumnMask*> filterMasks,
                                                   const std::span<const BinExpr* const> expressions,
                                                   const ColumnIDs* entities) {
    const auto reader = _view.read();

    for (size_t i = 0; i < filterMasks.size(); i++) {
        auto* indices = _mem->alloc<ColumnVector<size_t>>();

        const VarExpr* leftExpr = static_cast<VarExpr*>(expressions[i]->getLeftExpr());
        ExprConst* rightExpr = static_cast<ExprConst*>(expressions[i]->getRightExpr());

        const std::string& varExprName = leftExpr->getName();
        const auto propTypeRes = reader.getMetadata().propTypes().get(varExprName);
        if (!propTypeRes) {
            throw PlannerException("Property type does not exist");
        }

        const PropertyType propType = propTypeRes.value();

        switch (propType._valueType) {
            CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(Node, Int64)
            CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(Node, UInt64)
            CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(Node, Double)
            CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(Node, Bool)
            CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(Node, String)
            default: {
                throw PlannerException("Unsupported property type");
            }
        }
    }
}

void QueryPlanner::generateEdgePropertyFilterMasks(std::vector<ColumnMask*> filterMasks,
                                                   const std::span<const BinExpr* const> expressions,
                                                   const ColumnIDs* entities) {
    const auto reader = _view.read();

    for (size_t i = 0; i < filterMasks.size(); i++) {
        auto* indices = _mem->alloc<ColumnVector<size_t>>();

        const VarExpr* leftExpr = static_cast<VarExpr*>(expressions[i]->getLeftExpr());
        ExprConst* rightExpr = static_cast<ExprConst*>(expressions[i]->getRightExpr());

        const std::string& varExprName = leftExpr->getName();
        const auto propTypeRes = reader.getMetadata().propTypes().get(varExprName);
        if (!propTypeRes) {
            throw PlannerException("Property type does not exist");
        }

        const PropertyType propType = propTypeRes.value();

        switch (propType._valueType) {
            CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(Edge, Int64)
            CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(Edge, UInt64)
            CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(Edge, Double)
            CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(Edge, Bool)
            CASE_GET_FILTERED_PROPERTY_VALUE_TYPE(Edge, String)
            default: {
                throw PlannerException("Unsupported property type");
            }
        }
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
    auto* targets = _mem->alloc<ColumnIDs>();
    edgeWriteInfo._targetNodes = targets;

    const VarExpr* targetVar = target->getVar();
    if (targetVar) {
        VarDecl* targetDecl = targetVar->getDecl();
        if (targetDecl->isReturned()) {
            _transformData->addColumn(targets, targetDecl);
        }
    }

    // Generate sourceIDs only if the source is returned by projection
    const VarExpr* sourceVar = source->getVar();
    if (sourceVar) {
        VarDecl* sourceDecl = sourceVar->getDecl();
        if (sourceDecl->isReturned()) {
            auto* sources = _mem->alloc<ColumnIDs>();
            edgeWriteInfo._sourceNodes = sources;
            _transformData->addColumn(sources, sourceDecl);
        }
    }

    // Generate edgeIDs only if the edge is returned by projection
    const VarExpr* edgeVar = edge->getVar();
    if (edgeVar) {
        VarDecl* edgeDecl = edgeVar->getDecl();
        if (edgeDecl->isReturned()) {
            auto* edges = _mem->alloc<ColumnIDs>();
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
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isReturned();

    const ExprConstraint* edgeExprConstr = edge->getExprConstraint();
    const VarExpr* edgeVar = edge->getVar();
    VarDecl* edgeDecl = edgeVar ? edgeVar->getDecl() : nullptr;
    const bool mustWriteEdges = edgeDecl && edgeDecl->isReturned();

    EdgeWriteInfo edgeWriteInfo;
    edgeWriteInfo._indices = indices;

    _transformData->createStep(indices);

    // We always need target node IDs
    auto* targets = _mem->alloc<ColumnIDs>();
    edgeWriteInfo._targetNodes = targets;

    if (mustWriteEdges || edgeExprConstr) {
        auto* edges = _mem->alloc<ColumnIDs>();
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
                                                   const ColumnIDs* edges, const ColumnIDs* targetNodes, VarDecl* edgeDecl, VarDecl* targetDecl,
                                                   const bool mustWriteEdges, const bool mustWriteTargetNodes) {
    auto* filteredIndices = _mem->alloc<ColumnVector<size_t>>();
    auto* filteredNodes = _mem->alloc<ColumnIDs>();

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
        auto* filteredEdges = _mem->alloc<ColumnIDs>();
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
    auto* targets = _mem->alloc<ColumnIDs>();
    auto* edges = _mem->alloc<ColumnIDs>();
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
    auto* filterOutNodes = _mem->alloc<ColumnIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = targets,
        ._dest = filterOutNodes,
    });

    auto* filterOutEdges = _mem->alloc<ColumnIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = edgeWriteInfo._edges,
        ._dest = filterOutEdges,
    });

    const VarExpr* targetVar = target->getVar();
    VarDecl* targetDecl = targetVar ? targetVar->getDecl() : nullptr;
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isReturned();

    const VarExpr* edgeVar = edge->getVar();
    VarDecl* edgeDecl = edgeVar ? edgeVar->getDecl() : nullptr;
    const bool mustWriteEdges = edgeDecl && edgeDecl->isReturned();

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
    auto* targets = _mem->alloc<ColumnIDs>();
    auto* edges = _mem->alloc<ColumnIDs>();
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
    auto* filterOutNodes = _mem->alloc<ColumnIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = targets,
        ._dest = filterOutNodes,
    });

    auto* filterOutEdges = _mem->alloc<ColumnIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = edgeWriteInfo._edges,
        ._dest = filterOutEdges,
    });

    const VarExpr* targetVar = target->getVar();
    VarDecl* targetDecl = targetVar ? targetVar->getDecl() : nullptr;
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isReturned();

    const VarExpr* edgeVar = edge->getVar();
    VarDecl* edgeDecl = edgeVar ? edgeVar->getDecl() : nullptr;
    const bool mustWriteEdges = edgeDecl && edgeDecl->isReturned();

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
    auto* targets = _mem->alloc<ColumnIDs>();

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
    const bool mustWriteEdges = edgeDecl && edgeDecl->isReturned();

    if (mustWriteEdges || edgeExprConstr) {
        auto* edges = _mem->alloc<ColumnIDs>();
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
    auto* filterOutNodes = _mem->alloc<ColumnIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = targets,
        ._dest = filterOutNodes,
    });

    // Apply filter to edge IDs if necessary
    ColumnIDs* outputEdges {nullptr};
    if (mustWriteEdges || edgeExprConstr) {
        outputEdges = _mem->alloc<ColumnIDs>();
        filter.addOperand(FilterStep::Operand {
            ._mask = filterMask,
            ._src = edgeWriteInfo._edges,
            ._dest = outputEdges,
        });
    }

    const ExprConstraint* targetExprConstr = target->getExprConstraint();
    const VarExpr* targetVar = target->getVar();
    VarDecl* targetDecl = targetVar ? targetVar->getDecl() : nullptr;
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isReturned();

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
    _pipeline->add<LoadGraphStep>(loadCmd->getName());
    _pipeline->add<EndStep>();
    return true;
}

void QueryPlanner::planProjection(const MatchCommand* matchCmd) {
    const auto& projection = matchCmd->getProjection();

    for (const ReturnField* field : projection->returnFields()) {
        if (field->isAll()) {
            for (const MatchTarget* target : matchCmd->matchTargets()) {
                const PathPattern* pattern = target->getPattern();
                for (EntityPattern* entityPattern : pattern->elements()) {
                    if (const VarExpr* var = entityPattern->getVar()) {
                        if (VarDecl* decl = var->getDecl()) {
                            ColumnIDs* columnIDs = decl->getColumn()->cast<ColumnIDs>();
                            if (!columnIDs) {
                                columnIDs = _mem->alloc<ColumnIDs>();
                            }
                            _output->addColumn(columnIDs);
                        }
                    }
                }
            }
            continue;
        }

        // Skip variables for which no columnIDs have been generated
        const VarDecl* decl = field->getDecl();
        ColumnIDs* columnIDs = decl->getColumn()->cast<ColumnIDs>();
        if (!columnIDs) {
            columnIDs = _mem->alloc<ColumnIDs>();
        }

        const auto& memberName = field->getMemberName();
        if (memberName.empty()) {
            _output->addColumn(columnIDs);
        } else {
            planPropertyProjection(columnIDs, decl, memberName);
        }
    }
}

#define CASE_PLAN_VALUE_TYPE(Type)                                                \
    case ValueType::Type: {                                                       \
        auto propValues = _mem->alloc<ColumnOptVector<types::Type::Primitive>>(); \
        if (declKind == DeclKind::NODE_DECL) {                                    \
            using StepType = GetNodeProperty##Type##Step;                         \
            _pipeline->add<StepType>(columnIDs,                                   \
                                     propType,                                    \
                                     propValues);                                 \
        } else if (declKind == DeclKind::EDGE_DECL) {                             \
            using StepType = GetEdgeProperty##Type##Step;                         \
            _pipeline->add<StepType>(columnIDs,                                   \
                                     propType,                                    \
                                     propValues);                                 \
        } else {                                                                  \
            throw PlannerException("Unsupported declaration kind for variable \"" \
                                   + parentDecl->getName() + "\"");               \
        }                                                                         \
        _output->addColumn(propValues);                                           \
    } break;


void QueryPlanner::planPropertyProjection(ColumnIDs* columnIDs, const VarDecl* parentDecl, const std::string& memberName) {
    // Get property type information
    const auto propTypeRes = _view.metadata().propTypes().get(memberName);
    if (!propTypeRes) {
        throw PlannerException("Property type not found for property member \""
                               + memberName + "\"");
    }

    const auto propType = propTypeRes.value();

    const DeclKind declKind = parentDecl->getKind();

    switch (propTypeRes.value()._valueType) {
        CASE_PLAN_VALUE_TYPE(Int64)
        CASE_PLAN_VALUE_TYPE(UInt64)
        CASE_PLAN_VALUE_TYPE(Double)
        CASE_PLAN_VALUE_TYPE(String)
        CASE_PLAN_VALUE_TYPE(Bool)
        default: {
            throw PlannerException("Unsupported property type for property member \""
                                   + memberName + "\"");
        }
    }
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
