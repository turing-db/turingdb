#include "QueryPlanner.h"

#include <spdlog/spdlog.h>
#include <range/v3/view/drop.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/all.hpp>

#include "Expr.h"
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

#include "Graph.h"
#include "GraphMetadata.h"
#include "reader/GraphReader.h"
#include "PlannerException.h"

using namespace db;

namespace rv = ranges::views;

QueryPlanner::QueryPlanner(const GraphView& view, LocalMemory* mem, QueryCallback callback)
    : _view(view),
    _mem(mem),
    _queryCallback(callback),
    _pipeline(std::make_unique<Pipeline>()),
    _output(std::make_unique<Block>()),
    _transformData(std::make_unique<TransformData>(mem))
{
}

QueryPlanner::~QueryPlanner() {
    for (LabelSet* labelSet : _labelSets) {
        delete labelSet;
    }
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

        if (!edgeTypeConstr && (!sourceTypeConstr || !targetTypeConstr)) {
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
    const auto nodes = _mem->alloc<ColumnIDs>();

    if (const TypeConstraint* typeConstr = entity->getTypeConstraint()) {
        const LabelSet* labelSet = getLabelSet(typeConstr);
        _pipeline->add<ScanNodesByLabelStep>(nodes, labelSet);
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
    auto targets = _mem->alloc<ColumnIDs>();
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
            const auto sources = _mem->alloc<ColumnIDs>();
            edgeWriteInfo._sourceNodes = sources;
            _transformData->addColumn(sources, sourceDecl);
        }
    }

    // Generate edgeIDs only if the edge is returned by projection
    const VarExpr* edgeVar = edge->getVar();
    if (edgeVar) {
        VarDecl* edgeDecl = edgeVar->getDecl();
        if (edgeDecl->isReturned()) {
            const auto edges = _mem->alloc<ColumnIDs>();
            edgeWriteInfo._edges = edges;
            _transformData->addColumn(edges, edgeDecl);
        }
    }

    if (!sourceTypeConstr && !targetTypeConstr) {
        _pipeline->add<ScanEdgesStep>(edgeWriteInfo);
    } else if (!sourceTypeConstr) {
        const LabelSet* labelSet = getLabelSet(targetTypeConstr);
        _pipeline->add<ScanInEdgesByLabelStep>(edgeWriteInfo, labelSet);
    } else {
        const LabelSet* labelSet = getLabelSet(sourceTypeConstr);
        _pipeline->add<ScanOutEdgesByLabelStep>(edgeWriteInfo, labelSet);
    }

    _result = targets;
}

LabelSet* QueryPlanner::getLabelSet(const TypeConstraint* typeConstr) {
    LabelNames labelStrings;

    // Collect strings of all label names in typeConstr
    for (const VarExpr* nameExpr : typeConstr->getTypeNames()) {
        labelStrings.push_back(&nameExpr->getName());
    }

    const auto labelSetCacheIt = _labelSetCache.find(labelStrings);
    if (labelSetCacheIt != _labelSetCache.end()) {
        return labelSetCacheIt->second;
    }
    
    LabelSet* labelSet = buildLabelSet(labelStrings);
    _labelSetCache[labelStrings] = labelSet;

    return labelSet;
}

LabelSet* QueryPlanner::buildLabelSet(const LabelNames& labelNames) {
    LabelSet* labelSet = new LabelSet();
    for (const std::string* labelName : labelNames) {
        const LabelID labelID = getLabel(*labelName);
        labelSet->set(labelID);
    }

    _labelSets.push_back(labelSet);

    return labelSet;
}

LabelID QueryPlanner::getLabel(const std::string& labelName) {
    GraphMetadata& metadata = _view.metadata();
    return metadata.labels().getOrCreate(labelName);
}

bool QueryPlanner::planCreateGraph(const CreateGraphCommand* createCmd) {
    _pipeline->add<StopStep>();
    _pipeline->add<CreateGraphStep>(createCmd->getName());
    _pipeline->add<EndStep>();
    return true;
}

bool QueryPlanner::planListGraph(const ListGraphCommand* listCmd) {
    const auto graphNames = _mem->alloc<ColumnVector<std::string_view>>();

    _pipeline->add<StopStep>();
    _pipeline->add<ListGraphStep>(graphNames);
    _output->addColumn(graphNames);
    _pipeline->add<EndStep>();

    return true;
}

void QueryPlanner::getMatchingLabelSets(std::vector<LabelSetID>& labelSets,
                                        const LabelSet* targetLabelSet) {
    labelSets.clear();

    const auto reader = _view.read();

    auto it = reader.matchLabelSetIDs(targetLabelSet);
    for (; it.isValid(); ++it) {
        labelSets.emplace_back(*it);
    }
}

void QueryPlanner::planExpandEdgeWithNoConstraint(const EntityPattern* edge,
                                                  const EntityPattern* target) {
    const auto indices = _mem->alloc<ColumnVector<size_t>>();

    EdgeWriteInfo edgeWriteInfo;
    edgeWriteInfo._indices = indices;

    _transformData->createStep(indices);

    // Generate edgeIDs only if the edge is returned by projection
    const VarExpr* edgeVar = edge->getVar();
    if (edgeVar) {
        VarDecl* edgeDecl = edgeVar->getDecl();
        if (edgeDecl->isReturned()) {
            const auto edges = _mem->alloc<ColumnIDs>();
            edgeWriteInfo._edges = edges;
            _transformData->addColumn(edges, edgeDecl);
        }
    }

    // We always need target node IDs
    const auto targets = _mem->alloc<ColumnIDs>();
    edgeWriteInfo._targetNodes = targets;

    // Add targets to the writeSet
    const VarExpr* targetVar = target->getVar();
    if (targetVar) {
        VarDecl* targetDecl = targetVar->getDecl();
        if (targetDecl->isReturned()) {
            _transformData->addColumn(targets, targetDecl);
        }
    }

    _pipeline->add<GetOutEdgesStep>(_result, edgeWriteInfo);
    _result = targets;
}

void QueryPlanner::planExpandEdgeWithEdgeConstraint(const EntityPattern* edge,
                                                    const EntityPattern* target) {
    const TypeConstraint* edgeTypeConstr = edge->getTypeConstraint();

    const auto indices = _mem->alloc<ColumnVector<size_t>>();
    const auto targets = _mem->alloc<ColumnIDs>();
    const auto edges = _mem->alloc<ColumnIDs>();
    const auto edgeTypeIDs = _mem->alloc<ColumnVector<EdgeTypeID>>();

    const auto& typeConstrNames = edgeTypeConstr->getTypeNames();

    const std::string& edgeTypeName = typeConstrNames.front()->getName();
    
    // Search edge type IDs
    const auto& edgeTypeMap = _view.metadata().edgeTypes();
    const auto edgeTypeID = edgeTypeMap.get(edgeTypeName);
    if (!edgeTypeID.isValid()) {
        _transformData->createStep(indices);
        _transformData->addColumn(targets, nullptr);
        _result = targets;
        return;
    }

    // Get out edges step
    EdgeWriteInfo edgeWriteInfo;
    edgeWriteInfo._indices = indices;

    _transformData->createStep(indices);

    edgeWriteInfo._targetNodes = targets;
    edgeWriteInfo._edges = edges;
    edgeWriteInfo._edgeTypes = edgeTypeIDs;

    const VarExpr* edgeVar = edge->getVar();
    VarDecl* edgeDecl = edgeVar ? edgeVar->getDecl() : nullptr;
    const bool mustWriteEdges = edgeDecl && edgeDecl->isReturned();
    if (mustWriteEdges) {
        _transformData->addColumn(edges, edgeDecl);
    }

    _pipeline->add<GetOutEdgesStep>(_result, edgeWriteInfo);

    // Filter out edges that do not match the edge type ID
    const auto filterEdgeTypeID = _mem->alloc<ColumnConst<EdgeTypeID>>();
    filterEdgeTypeID->set(edgeTypeID);

    const auto filterIndices = _mem->alloc<ColumnVector<size_t>>();
    auto& filter = _pipeline->add<FilterStep>(filterIndices).get<FilterStep>();

    const auto filterMask = _mem->alloc<ColumnMask>();
    filter.addExpression(FilterStep::Expression {
        ._op = ColumnOperator::OP_EQUAL,
        ._mask = filterMask,
        ._lhs = edgeTypeIDs,
        ._rhs = filterEdgeTypeID
    });

    _transformData->createStep(filterIndices);

    // Apply filter to target node IDs
    const auto filterOutNodes = _mem->alloc<ColumnIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = targets,
        ._dest = filterOutNodes
    });

    if (mustWriteEdges) {
        const auto filterOutEdges = _mem->alloc<ColumnIDs>();
        filter.addOperand(FilterStep::Operand {
            ._mask = filterMask,
            ._src = edgeWriteInfo._edges,
            ._dest = filterOutEdges
        });
        _transformData->addColumn(filterOutEdges, edgeDecl);
    }

    // Add targets to the writeSet
    const VarExpr* targetVar = target->getVar();
    VarDecl* targetDecl = targetVar ? targetVar->getDecl() : nullptr;
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isReturned();
    if (mustWriteTargetNodes) {
        _transformData->addColumn(filterOutNodes, targetDecl);
    }

    _result = filterOutNodes;
}

void QueryPlanner::planExpandEdgeWithEdgeAndTargetConstraint(const EntityPattern* edge,
                                                             const EntityPattern* target) {
    const TypeConstraint* edgeTypeConstr = edge->getTypeConstraint();
    const TypeConstraint* targetTypeConstr = target->getTypeConstraint();

    const auto indices = _mem->alloc<ColumnVector<size_t>>();
    const auto targets = _mem->alloc<ColumnIDs>();
    const auto edges = _mem->alloc<ColumnIDs>();
    const auto edgeTypeIDs= _mem->alloc<ColumnVector<EdgeTypeID>>();

    const auto& typeConstrNames = edgeTypeConstr->getTypeNames();

    const std::string& edgeTypeName = typeConstrNames.front()->getName();
    
    // Search edge type IDs
    const auto& edgeTypeMap = _view.metadata().edgeTypes();
    const auto edgeTypeID = edgeTypeMap.get(edgeTypeName);
    if (!edgeTypeID.isValid()) {
        _transformData->createStep(indices);
        _transformData->addColumn(targets, nullptr);
        _result = targets;
        return;
    }

    // Search label set IDs
    const LabelSet* targetLabelSet = getLabelSet(targetTypeConstr);
    getMatchingLabelSets(_tmpLabelSetIDs, targetLabelSet);
    if (_tmpLabelSetIDs.empty()) {
        _transformData->createStep(indices);
        _transformData->addColumn(targets, nullptr);
        _result = targets;
        return;
    }

    // Get out edges step
    EdgeWriteInfo edgeWriteInfo;
    edgeWriteInfo._indices = indices;

    _transformData->createStep(indices);

    edgeWriteInfo._targetNodes = targets;
    edgeWriteInfo._edges = edges;
    edgeWriteInfo._edgeTypes = edgeTypeIDs;

    const VarExpr* edgeVar = edge->getVar();
    VarDecl* edgeDecl = edgeVar ? edgeVar->getDecl() : nullptr;
    const bool mustWriteEdges = edgeDecl && edgeDecl->isReturned();
    if (mustWriteEdges) {
        _transformData->addColumn(edges, edgeDecl);
    }

    _pipeline->add<GetOutEdgesStep>(_result, edgeWriteInfo);

    // Get label set IDs of target nodes
    const auto nodesLabelSetIDs = _mem->alloc<ColumnVector<LabelSetID>>();
    _pipeline->add<GetLabelSetIDStep>(targets, nodesLabelSetIDs);


    // Filter out edges that do not match the edge type ID
    const auto filterEdgeTypeID = _mem->alloc<ColumnConst<EdgeTypeID>>();
    filterEdgeTypeID->set(edgeTypeID);

    const auto filterIndices = _mem->alloc<ColumnVector<size_t>>();
    auto& filter = _pipeline->add<FilterStep>(filterIndices).get<FilterStep>();

    ColumnMask* filterMaskEdges = _mem->alloc<ColumnMask>();
    filter.addExpression(FilterStep::Expression {
        ._op = ColumnOperator::OP_EQUAL,
        ._mask = filterMaskEdges,
        ._lhs = edgeTypeIDs,
        ._rhs = filterEdgeTypeID
    });

    ColumnMask* filterMaskNodes = nullptr;
    for (LabelSetID labelSetID : _tmpLabelSetIDs) {
        const auto targetLabelSetID = _mem->alloc<ColumnConst<LabelSetID>>();
        targetLabelSetID->set(labelSetID);

        const auto newMask = _mem->alloc<ColumnMask>();

        filter.addExpression(FilterStep::Expression {
            ._op = ColumnOperator::OP_EQUAL,
            ._mask = newMask,
            ._lhs = nodesLabelSetIDs,
            ._rhs = targetLabelSetID
        });

        if (filterMaskNodes) {
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_OR,
                ._mask = newMask,
                ._lhs = filterMaskNodes,
                ._rhs = newMask
            });
        }

        filterMaskNodes = newMask;
    }

    // Combine filter masks
    const auto filterMask = _mem->alloc<ColumnMask>();
    filter.addExpression(FilterStep::Expression {
        ._op = ColumnOperator::OP_AND,
        ._mask = filterMask,
        ._lhs = filterMaskEdges,
        ._rhs = filterMaskNodes
    });

    _transformData->createStep(filterIndices);

    // Apply filter to target node IDs
    const auto filterOutNodes = _mem->alloc<ColumnIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = targets,
        ._dest = filterOutNodes
    });

    if (mustWriteEdges) {
        const auto filterOutEdges = _mem->alloc<ColumnIDs>();
        filter.addOperand(FilterStep::Operand {
            ._mask = filterMask,
            ._src = edgeWriteInfo._edges,
            ._dest = filterOutEdges
        });
        _transformData->addColumn(filterOutEdges, edgeDecl);
    }

    // Add targets to the writeSet
    const VarExpr* targetVar = target->getVar();
    VarDecl* targetDecl = targetVar ? targetVar->getDecl() : nullptr;
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isReturned();
    if (mustWriteTargetNodes) {
        _transformData->addColumn(filterOutNodes, targetDecl);
    }

    _result = filterOutNodes;
}

void QueryPlanner::planExpandEdgeWithTargetConstraint(const EntityPattern* edge,
                                                      const EntityPattern* target) {
    const TypeConstraint* targetTypeConstr = target->getTypeConstraint();
    const auto indices = _mem->alloc<ColumnVector<size_t>>();
    const auto targets = _mem->alloc<ColumnIDs>();

    const LabelSet* targetLabelSet = getLabelSet(targetTypeConstr);

    getMatchingLabelSets(_tmpLabelSetIDs, targetLabelSet);

    // If no matching LabelSet, empty result
    if (_tmpLabelSetIDs.empty()) {
        _transformData->createStep(indices);
        _transformData->addColumn(targets, nullptr);
        _result = targets;
        return;
    }

    // GetOutEdgesStep
    EdgeWriteInfo edgeWriteInfo;

    edgeWriteInfo._indices = indices;
    _transformData->createStep(indices);

    edgeWriteInfo._targetNodes = targets;

    const VarExpr* edgeVar = edge->getVar();
    VarDecl* edgeDecl = edgeVar ? edgeVar->getDecl() : nullptr;
    const bool mustWriteEdges = edgeDecl && edgeDecl->isReturned();
    if (mustWriteEdges) {
        const auto edges = _mem->alloc<ColumnIDs>();
        edgeWriteInfo._edges = edges;
        _transformData->addColumn(edges, edgeDecl);
    }

    _pipeline->add<GetOutEdgesStep>(_result, edgeWriteInfo);

    // Get labels of target nodes
    const auto nodesLabelSetIDs = _mem->alloc<ColumnVector<LabelSetID>>();
    _pipeline->add<GetLabelSetIDStep>(targets, nodesLabelSetIDs);

    // Create filter to filter target nodes based on target label set
    const auto filterIndices = _mem->alloc<ColumnVector<size_t>>();
    auto& filter = _pipeline->add<FilterStep>(filterIndices).get<FilterStep>();

    // Build filter expression to compute filter for each LabelSetID
    ColumnMask* filterMask = nullptr;
    for (LabelSetID labelSetID : _tmpLabelSetIDs) {
        const auto targetLabelSetID = _mem->alloc<ColumnConst<LabelSetID>>();
        targetLabelSetID->set(labelSetID);

        const auto newMask = _mem->alloc<ColumnMask>();

        filter.addExpression(FilterStep::Expression {
            ._op = ColumnOperator::OP_EQUAL,
            ._mask = newMask,
            ._lhs = nodesLabelSetIDs,
            ._rhs = targetLabelSetID
        });

        if (filterMask) {
            filter.addExpression(FilterStep::Expression {
                ._op = ColumnOperator::OP_OR,
                ._mask = newMask,
                ._lhs = filterMask,
                ._rhs = newMask
            });
        }

        filterMask = newMask;
    }

    _transformData->createStep(filterIndices);

    // Apply filter to target node IDs
    const auto filterOutNodes = _mem->alloc<ColumnIDs>();
    filter.addOperand(FilterStep::Operand {
        ._mask = filterMask,
        ._src = targets,
        ._dest = filterOutNodes
    });

    // Apply filter to edge IDs if necessary
    if (mustWriteEdges) {
        const auto filterOutEdges = _mem->alloc<ColumnIDs>();
        filter.addOperand(FilterStep::Operand {
            ._mask = filterMask,
            ._src = edgeWriteInfo._edges,
            ._dest = filterOutEdges
        });

        _transformData->addColumn(filterOutEdges, edgeDecl);
    }

    // Add targets to the writeSet
    const VarExpr* targetVar = target->getVar();
    VarDecl* targetDecl = targetVar ? targetVar->getDecl() : nullptr;
    const bool mustWriteTargetNodes = targetDecl && targetDecl->isReturned();
    if (mustWriteTargetNodes) {
        _transformData->addColumn(filterOutNodes, targetDecl);
    }

    _result = filterOutNodes;
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
                    if (VarExpr* var = entityPattern->getVar()) {
                        if (VarDecl* decl = var->getDecl()) {
                            ColumnIDs* columnIDs = decl->getColumn()->cast<ColumnIDs>();
                            if (!columnIDs) {
                                continue;
                            }
                            _output->addColumn(columnIDs);
                        }
                    }
                }
            }
            continue;
        }

        // Skip variables for which no columnIDs have been generated
        ColumnIDs* columnIDs = field->getDecl()->getColumn()->cast<ColumnIDs>();
        if (!columnIDs) {
            continue;
        }

        const auto& memberName = field->getMemberName();
        if (memberName.empty()) {
            _output->addColumn(columnIDs);
        } else {
            planPropertyProjection(columnIDs, memberName);
        }
    }
}

#define CASE_PLAN_VALUE_TYPE(Type)                                                \
    case ValueType::Type: {                                                       \
        auto propValues = _mem->alloc<ColumnOptVector<types::Type::Primitive>>(); \
        _pipeline->add<GetProperty##Type##Step>(columnIDs, propType, propValues); \
        _output->addColumn(propValues);                                           \
    } break;


void QueryPlanner::planPropertyProjection(ColumnIDs* columnIDs, const std::string& memberName) {
    const auto reader = _view.read();

    // Get property type information
    const PropertyType propType = reader.getPropertyType(memberName);
    if (!propType.isValid()) {
        throw PlannerException("Property type not found for property member \""
                               + memberName + "\"");
    }

    switch (propType._valueType) {
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

    auto pipeDescr = _mem->alloc<ColumnVector<std::string>>();

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
