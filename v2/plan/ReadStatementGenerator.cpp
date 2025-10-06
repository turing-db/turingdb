#include "ReadStatementGenerator.h"

#include <queue>
#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "PlanGraph.h"
#include "WhereClause.h"
#include "PlanGraphVariables.h"
#include "PlanGraphTopology.h"

#include "decl/VarDecl.h"
#include "nodes/FilterNode.h"
#include "nodes/GetEdgeTargetNode.h"
#include "nodes/GetEdgesNode.h"
#include "nodes/GetInEdgesNode.h"
#include "nodes/GetOutEdgesNode.h"
#include "nodes/JoinNode.h"
#include "nodes/MaterializeNode.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/VarNode.h"

#include "stmt/Stmt.h"
#include "stmt/MatchStmt.h"

#include "decl/PatternData.h"
#include "metadata/LabelSet.h"

using namespace db::v2;

ReadStatementGenerator::ReadStatementGenerator(const CypherAST* ast,
                                               PlanGraph* tree,
                                               PlanGraphVariables* variables)
    : _ast(ast),
      _tree(tree),
      _variables(variables) {
}

ReadStatementGenerator::~ReadStatementGenerator() {
}

void ReadStatementGenerator::generateStmt(const Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::MATCH:
            generateMatchStmt(static_cast<const MatchStmt*>(stmt));
            break;

        default:
            throwError(fmt::format("Unsupported read statement type: {}", (uint64_t)stmt->getKind()), stmt);
            break;
    }
}

void ReadStatementGenerator::generateMatchStmt(const MatchStmt* stmt) {
    const Pattern* pattern = stmt->getPattern();

    // Each PatternElement is a target of the match
    // and contains a chain of EntityPatterns
    for (const PatternElement* element : pattern->elements()) {
        generatePatternElement(element);
    }

    const WhereClause* where = pattern->getWhere();
    if (where) {
        generateWhereClause(where);
    }
}

void ReadStatementGenerator::generateWhereClause(const WhereClause* where) {
    const Expr* expr = where->getExpr();

    unwrapWhereExpr(expr);
}

void ReadStatementGenerator::generatePatternElement(const PatternElement* element) {
    if (element->size() == 0) {
        throwError("Empty match pattern element", element);
    }

    const NodePattern* origin = dynamic_cast<const NodePattern*>(element->getRootEntity());
    if (!origin) {
        throwError("Pattern element origin must be a node pattern", element);
    }

    // Reset the decl order of variables (each branch starts at 0)
    _variables->resetDeclOrder();

    VarNode* currentNode = generatePatternElementOrigin(origin);

    const auto& chain = element->getElementChain();
    for (const auto& [edge, node] : chain) {
        const EdgePattern* e = dynamic_cast<const EdgePattern*>(edge);
        if (!edge) {
            throwError("Pattern element edge must be an edge pattern", element);
        }

        currentNode = generatePatternElementEdge(currentNode, e);

        const NodePattern* n = dynamic_cast<const NodePattern*>(node);
        if (!node) {
            throwError("Pattern element node must be a node pattern", element);
        }

        currentNode = generatePatternElementTarget(currentNode, n);
    }
}

VarNode* ReadStatementGenerator::generatePatternElementOrigin(const NodePattern* origin) {
    const NodePatternData* data = origin->getData();
    const LabelSet& labelset = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = origin->getDecl();

    auto [var, filter] = _variables->getVarNodeAndFilter(decl);

    if (!var) {
        // Scan nodes
        ScanNodesNode* scan = _tree->create<ScanNodesNode>();
        std::tie(var, filter) = _variables->createVarNodeAndFilter(decl);

        scan->connectOut(filter);
    } else {
        _variables->setNextDeclOrder(var->getDeclOrder() + 1);
    }

    NodeFilterNode* nodeFilter = filter->asNodeFilter();

    // Type constraints
    nodeFilter->addLabelConstraints(labelset);

    // Property constraints
    for (const auto& [propType, expr] : exprConstraints) {
        _propConstraints.push_back(std::make_unique<PropertyConstraint>(var, propType._id, expr));
    }

    return var;
}

VarNode* ReadStatementGenerator::generatePatternElementEdge(VarNode* prevNode,
                                                            const EdgePattern* edge) {
    // Expand edge based on direction

    PlanGraphNode* currentNode = nullptr;
    switch (edge->getDirection()) {
        case EdgePattern::Direction::Undirected: {
            currentNode = _tree->newOut<GetEdgesNode>(prevNode);
        } break;
        case EdgePattern::Direction::Backward: {
            currentNode = _tree->newOut<GetInEdgesNode>(prevNode);
        } break;
        case EdgePattern::Direction::Forward: {
            currentNode = _tree->newOut<GetOutEdgesNode>(prevNode);
        } break;
    }

    // Edge constraints
    const EdgePatternData* data = edge->getData();
    const auto& edgeTypes = data->edgeTypeConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = edge->getDecl();

    if (edgeTypes.size() > 1) {
        throwError("Only one edge type constraint is supported for now", edge);
    }

    auto [var, filter] = _variables->getVarNodeAndFilter(decl);
    if (!var) {
        std::tie(var, filter) = _variables->createVarNodeAndFilter(decl);
    } else {
        throwError("Re-using the same edge variable, this is not supported yet", edge);
    }

    currentNode->connectOut(filter);
    EdgeFilterNode* edgeFilter = filter->asEdgeFilter();

    // Type constraints
    for (const EdgeTypeID edgeTypeID : edgeTypes) {
        edgeFilter->addEdgeTypeConstraint(edgeTypeID);
    }

    // Property constraints
    for (const auto& [propType, expr] : exprConstraints) {
        _propConstraints.push_back(std::make_unique<PropertyConstraint>(var, propType._id, expr));
    }

    return var;
}

VarNode* ReadStatementGenerator::generatePatternElementTarget(VarNode* prevNode,
                                                              const NodePattern* target) {
    // Target nodes
    const NodePatternData* data = target->getData();
    const auto& labelset = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = target->getDecl();

    PlanGraphNode* currentNode = _tree->newOut<GetEdgeTargetNode>(prevNode);

    auto [var, filter] = _variables->getVarNodeAndFilter(decl);
    if (!var) {
        std::tie(var, filter) = _variables->createVarNodeAndFilter(decl);
        currentNode->connectOut(filter);
    } else {
        incrementDeclOrders(prevNode->getDeclOrder(), filter);
        currentNode->connectOut(filter);

        // Detect loops
        if (PlanGraphTopology::detectLoops(filter)) {
            throwError("Loop detected. This is not supported yet", target);
        }
    }

    auto* typedFilter = static_cast<NodeFilterNode*>(filter);

    // Type constraintsj
    typedFilter->addLabelConstraints(labelset);

    // Property constraints
    for (const auto& [propType, expr] : exprConstraints) {
        _propConstraints.push_back(std::make_unique<PropertyConstraint>(var, propType._id, expr));
    }

    return var;
}

void ReadStatementGenerator::unwrapWhereExpr(const Expr* expr) {
    if (expr->getKind() == Expr::Kind::NODE_LABEL) {
        // Node label expression can be pushed down to the var node
        //
        // WARNING
        // NODE_LABEL is missleading
        // It can also refer to an edge type such as: e:KNOWS

        const NodeLabelExpr* entityTypeExpr = static_cast<const NodeLabelExpr*>(expr);
        const VarDecl* decl = entityTypeExpr->getDecl();
        const VarNode* varNode = _variables->getVarNode(decl);
        FilterNode* filter = _variables->getNodeFilter(varNode);

        if (decl->getType() == EvaluatedType::NodePattern) {
            NodeFilterNode* nodeFilter = static_cast<NodeFilterNode*>(filter);
            nodeFilter->addLabelConstraints(entityTypeExpr->labelSet());

        } else if (decl->getType() == EvaluatedType::EdgePattern) {
            EdgeFilterNode* edgeFilter = static_cast<EdgeFilterNode*>(filter);

            if (entityTypeExpr->labels().size() != 1) {
                throwError("Only one edge type constraint is supported for now", expr);
            }

            // TODO: Fix, this is ugly, we make the NodeLabel expression Node/Edge agnostic
            // Right now we have to interprete "Label ids" as edge types
            std::vector<LabelID> edgeTypes;
            entityTypeExpr->labelSet().decompose(edgeTypes);
            for (const auto& edgeType : edgeTypes) {
                edgeFilter->addEdgeTypeConstraint(edgeType);
            }
        }

        return;
    }

    if (expr->getKind() == Expr::Kind::BINARY) {
        const BinaryExpr* binaryExpr = static_cast<const BinaryExpr*>(expr);

        if (binaryExpr->getOperator() == BinaryOperator::And) {
            // If AND operator, we can unwrap to push down predicates to var nodes
            unwrapWhereExpr(binaryExpr->getLHS());
            unwrapWhereExpr(binaryExpr->getRHS());
            return;
        }

        // --> Fallthrough
        //
        // If any other binary operator, we treat it as a whole predicate on
        // which we need to generate dependencies
    }

    // Unwraped the first list of AND expressions,
    // Treating other cases as a whole Where predicate
    WherePredicate* predicate = _tree->createWherePredicate(expr);
    predicate->generate(*_variables);

    const auto& dependencies = predicate->getDependencies();
    if (dependencies.empty()) {
        throwError("Where clauses without dependencies are not supported yet", expr);
    }

    const VarNode* firstVar = dependencies.begin()->_var;
    uint32_t _lastDeclOrder = firstVar->getDeclOrder();
    const VarNode* _lastDependency = firstVar;

    for (const auto& dep : dependencies) {
        if (_lastDeclOrder < dep._var->getDeclOrder()) {
            _lastDeclOrder = dep._var->getDeclOrder();
            _lastDependency = dep._var;
        }
    }

    FilterNode* filterNode = _variables->getNodeFilter(_lastDependency);
    filterNode->addWherePredicate(predicate);
    predicate->setFilterNode(filterNode);
}

void ReadStatementGenerator::incrementDeclOrders(uint32_t declOrder, PlanGraphNode* origin) {
    std::queue<PlanGraphNode*> q;
    q.push(origin);

    while (!q.empty()) {
        auto* node = q.front();
        q.pop();

        if (node->getOpcode() == PlanGraphOpcode::VAR) {
            auto* varNode = static_cast<VarNode*>(node);
            varNode->setDeclOrder(varNode->getDeclOrder() + declOrder + 1);
        }

        for (auto* out : node->outputs()) {
            q.push(out);
        }
    }
}

void ReadStatementGenerator::placeJoinsOnVars() {
    for (auto [var, filter] : _variables->getNodeFiltersMap()) {
        if (filter->inputs().size() > 1) {
            _tree->insertBefore<JoinNode>(filter);
        }
    }
}

void ReadStatementGenerator::placePropertyExprJoins() {
    for (auto& prop : _propConstraints) {
        auto& deps = prop->dependencies;
        const auto& depContainer = deps.getDependencies();

        // Generate missing dependencies
        deps.genExprDependencies(*_variables, prop->expr);

        // Step 1: find the latest dependency
        auto it = depContainer.begin();
        const VarNode* var = prop->var;
        uint32_t order = var->getDeclOrder();

        for (; it != depContainer.end(); ++it) {
            if (order < it->_var->getDeclOrder()) {
                order = it->_var->getDeclOrder();
                var = it->_var;
            }
        }

        // Step 2: Place the constraint
        auto* filter = _variables->getNodeFilter(var);
        filter->addPropertyConstraint(prop.get());

        // Step 3: place joins
        insertDataFlowNode(var, prop->var);

        for (const auto& dep : deps.getDependencies()) {
            insertDataFlowNode(var, dep._var);
        }
    }
}

void ReadStatementGenerator::placePredicateJoins() {
    for (const auto& pred : _tree->wherePredicates()) {
        FilterNode* filterNode = pred->getFilterNode();
        const VarNode* var = filterNode->getVarNode();

        for (const auto& dep : pred->getDependencies()) {
            if (filterNode->getVarNode() == dep._var) {
                continue;
            }

            insertDataFlowNode(var, dep._var);
        }
    }
}

void ReadStatementGenerator::insertDataFlowNode(const VarNode* node, VarNode* dependency) {
    FilterNode* filter = _variables->getNodeFilter(node);
    const auto path = PlanGraphTopology::getShortestPath(node, dependency);

    switch (path) {
        case PlanGraphTopology::PathToDependency::SameVar: {
            // Nothing to be done
            return;
        }

        case PlanGraphTopology::PathToDependency::BackwardPath: {
            // If only walked backward
            // Materialize
            _tree->insertBefore<MaterializeNode>(filter);
            return;
        }

        case PlanGraphTopology::PathToDependency::UndirectedPath: {
            // If had to walk both backward and forward
            // Join
            JoinNode* join = _tree->insertBefore<JoinNode>(filter);
            auto* depBranchTip = PlanGraphTopology::getBranchTip(dependency);
            depBranchTip->connectOut(join);
            return;
        }

        case PlanGraphTopology::PathToDependency::NoPath: {
            // If nodes are on two different islands
            // ValueHashJoin
            JoinNode* join = _tree->insertBefore<JoinNode>(filter);
            auto* depBranchTip = PlanGraphTopology::getBranchTip(dependency);
            depBranchTip->connectOut(join);
            return;
        }
    }
}

void ReadStatementGenerator::throwError(std::string_view msg, const void* obj) const {
    throw PlannerException(_ast->createErrorString(msg, obj));
}
