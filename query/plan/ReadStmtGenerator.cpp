#include "ReadStmtGenerator.h"

#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "PlanGraph.h"
#include "Symbol.h"
#include "SymbolChain.h"
#include "WhereClause.h"
#include "PlanGraphVariables.h"
#include "PlanGraphTopology.h"
#include "Predicate.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "decl/VarDecl.h"
#include "decl/PatternData.h"
#include "metadata/LabelSet.h"

#include "nodes/CartesianProductNode.h"
#include "nodes/FilterNode.h"
#include "nodes/GetEdgeTargetNode.h"
#include "nodes/GetEdgesNode.h"
#include "nodes/GetInEdgesNode.h"
#include "nodes/GetOutEdgesNode.h"
#include "nodes/GetPropertyNode.h"
#include "nodes/GetEntityTypeNode.h"
#include "nodes/GetPropertyWithNullNode.h"
#include "nodes/JoinNode.h"
#include "nodes/ProcedureEvalNode.h"
#include "nodes/ProduceResultsNode.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/VarNode.h"

#include "stmt/Stmt.h"
#include "stmt/MatchStmt.h"
#include "stmt/CallStmt.h"

#include "BioAssert.h"

using namespace db;

ReadStmtGenerator::ReadStmtGenerator(const CypherAST* ast,
                                     GraphView graphView,
                                     PlanGraph* tree,
                                     PlanGraphVariables* variables)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(graphView.metadata()),
    _tree(tree),
    _variables(variables),
    _topology(std::make_unique<PlanGraphTopology>())
{
}

ReadStmtGenerator::~ReadStmtGenerator() {
}

void ReadStmtGenerator::generateStmt(const Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::MATCH:
            generateMatchStmt(static_cast<const MatchStmt*>(stmt));
            break;

        case Stmt::Kind::CALL:
            generateCallStmt(static_cast<const CallStmt*>(stmt));
            break;

        default:
            throwError(fmt::format("Unsupported read statement type: {}", (uint64_t)stmt->getKind()), stmt);
            break;
    }
}

void ReadStmtGenerator::generateMatchStmt(const MatchStmt* stmt) {
    const Pattern* pattern = stmt->getPattern();

    if (stmt->hasOrderBy()) {
        throwError("MATCH ... ORDER BY ... is not supported yet. "
                   "Please use RETURN ... ORDER BY ... instead",
                   stmt);
    }

    if (stmt->hasSkip()) {
        throwError("MATCH ... SKIP ... is not supported yet. "
                   "Please use RETURN ... SKIP ... instead",
                   stmt);
    }

    if (stmt->hasLimit()) {
        throwError("MATCH ... LIMIT ... is not supported yet. "
                   "Please use RETURN ... LIMIT ... instead",
                   stmt);
    }

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

void ReadStmtGenerator::generateCallStmt(const CallStmt* callStmt) {
    if (callStmt->isOptional()) {
        throwError("OPTIONAL CALL not supported", callStmt);
    }

    bioassert(callStmt->getFunc(), "Function invocation expression is null");

    const FunctionInvocationExpr* funcExpr = callStmt->getFunc();
    YieldClause* yield = callStmt->getYield();

    ProcedureEvalNode* procNode = _tree->create<ProcedureEvalNode>(funcExpr, yield);

    if (callStmt->isStandaloneCall()) {
        _tree->newOut<ProduceResultsNode>(procNode);
        return;
    } else {
        bioassert(yield, "Procedure call without YIELD must be a standalone CALL");
    }
}

void ReadStmtGenerator::generateWhereClause(const WhereClause* where) {
    Expr* expr = where->getExpr();

    unwrapWhereExpr(expr);
}

void ReadStmtGenerator::generatePatternElement(const PatternElement* element) {
    if (element->size() == 0) {
        throwError("Empty match pattern element", element);
    }

    const NodePattern* origin = dynamic_cast<const NodePattern*>(element->getRootEntity());
    if (!origin) {
        throwError("Pattern element origin must be a node pattern", element);
    }

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

VarNode* ReadStmtGenerator::generatePatternElementOrigin(const NodePattern* origin) {
    const NodePatternData* data = origin->getData();
    const std::span labels = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = origin->getDecl();
    const LabelMap& labelMap = _graphMetadata.labels();
    const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

    auto [var, filter] = _variables->getVarNodeAndFilter(decl);

    if (!var) {
        // Scan nodes
        ScanNodesNode* scan = _tree->create<ScanNodesNode>();
        std::tie(var, filter) = _variables->createVarNodeAndFilter(decl);

        scan->connectOut(filter);
    }

    NodeFilterNode* nodeFilter = filter->asNodeFilter();

    // Type constraints
    LabelSet labelset;

    for (const std::string_view label : labels) {
        const std::optional<LabelID> labelID = labelMap.get(label);
        if (!labelID) {
            throwError(fmt::format("Unknown label: {}", label), origin);
        }
        labelset.set(labelID.value());
    }

    nodeFilter->addLabelConstraints(labelset);

    // Property constraints
    for (const EntityPropertyConstraint& constraint : exprConstraints) {
        const std::optional propType = propTypeMap.get(constraint._propTypeName);
        if (!propType) {
            throwError(fmt::format("Unknown property type: {}", constraint._propTypeName), constraint._expr);
        }

        Predicate* predicate = _tree->createPredicate(constraint._expr);
        predicate->generate(*_variables);
    }

    return var;
}

VarNode* ReadStmtGenerator::generatePatternElementEdge(VarNode* prevNode,
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
    const std::span edgeTypes = data->edgeTypeConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const EdgeTypeMap& edgeTypeMap = _graphMetadata.edgeTypes();
    const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();
    const VarDecl* decl = edge->getDecl();

    if (edgeTypes.size() > 1) {
        throwError("Only one edge type constraint is supported for now", edge);
    }

    auto [var, filter] = _variables->getVarNodeAndFilter(decl);
    if (!var) {
        std::tie(var, filter) = _variables->createVarNodeAndFilter(decl);
    } else {
        throwError("Re-using the same edge variable, this is not supported", edge);
    }

    currentNode->connectOut(filter);
    EdgeFilterNode* edgeFilter = filter->asEdgeFilter();

    // Type constraints
    for (std::string_view edgeTypeName : edgeTypes) {
        const std::optional edgeType = edgeTypeMap.get(edgeTypeName);
        if (!edgeType) {
            throwError(fmt::format("Unknown edge type: {}", edgeTypeName), edge);
        }

        edgeFilter->addEdgeTypeConstraint(edgeType.value());
    }

    // Property constraints
    for (const EntityPropertyConstraint& constraint : exprConstraints) {
        const std::optional propType = propTypeMap.get(constraint._propTypeName);
        if (!propType) {
            throwError(fmt::format("Unknown property type: {}", constraint._propTypeName), constraint._expr);
        }

        Predicate* predicate = _tree->createPredicate(constraint._expr);
        predicate->generate(*_variables);
    }

    return var;
}

VarNode* ReadStmtGenerator::generatePatternElementTarget(VarNode* prevNode,
                                                         const NodePattern* target) {
    // Target nodes
    const NodePatternData* data = target->getData();
    const std::span labels = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = target->getDecl();
    const LabelMap& labelMap = _graphMetadata.labels();
    const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

    PlanGraphNode* currentNode = _tree->newOut<GetEdgeTargetNode>(prevNode);

    auto [var, filter] = _variables->getVarNodeAndFilter(decl);
    if (!var) {
        std::tie(var, filter) = _variables->createVarNodeAndFilter(decl);
        currentNode->connectOut(filter);
    } else {
        currentNode->connectOut(filter);

        // Detect loops
        if (_topology->detectLoopsFrom(filter)) {
            throwError("Loop detected. This is not supported yet", target);
        }
    }

    auto* nodeFilter = static_cast<NodeFilterNode*>(filter);

    // Type constraints
    LabelSet labelset;

    for (const std::string_view label : labels) {
        const std::optional<LabelID> labelID = labelMap.get(label);
        if (!labelID) {
            throwError(fmt::format("Unknown label: {}", label), target);
        }
        labelset.set(labelID.value());
    }

    nodeFilter->addLabelConstraints(labelset);

    // Property constraints
    for (const EntityPropertyConstraint& constraint : exprConstraints) {
        const std::optional propType = propTypeMap.get(constraint._propTypeName);
        if (!propType) {
            throwError(fmt::format("Unknown property type: {}", constraint._propTypeName), constraint._expr);
        }

        Predicate* predicate = _tree->createPredicate(constraint._expr);
        predicate->generate(*_variables);
    }

    return var;
}

void ReadStmtGenerator::unwrapWhereExpr(Expr* expr) {
    if (expr->getKind() == Expr::Kind::ENTITY_TYPES) {
        // Entity type expressions can be pushed down to the var (node or edge)

        const EntityTypeExpr* entityTypeExpr = static_cast<const EntityTypeExpr*>(expr);
        const VarDecl* decl = entityTypeExpr->getEntityVarDecl();
        const VarNode* varNode = _variables->getVarNode(decl);
        FilterNode* filter = _variables->getNodeFilter(varNode);

        const LabelMap& labelMap = _graphMetadata.labels();
        const EdgeTypeMap& edgeTypeMap = _graphMetadata.edgeTypes();

        if (decl->getType() == EvaluatedType::NodePattern) {
            NodeFilterNode* nodeFilter = static_cast<NodeFilterNode*>(filter);

            const SymbolChain* labels = entityTypeExpr->getTypes();

            if (labels) {
                LabelSet labelset;
                for (const Symbol* symbol : *labels) {
                    const std::string_view label = symbol->getName();
                    const std::optional labelID = labelMap.get(label);

                    if (!labelID) {
                        throwError(fmt::format("Unknown label: {}", label), entityTypeExpr);
                    }

                    labelset.set(labelID.value());
                }

                nodeFilter->addLabelConstraints(labelset);
            }

        } else if (decl->getType() == EvaluatedType::EdgePattern) {
            EdgeFilterNode* edgeFilter = static_cast<EdgeFilterNode*>(filter);

            const SymbolChain* edgeTypes = entityTypeExpr->getTypes();

            if (edgeTypes) {
                if (edgeTypes->size() != 1) {
                    throwError("Only one edge type constraint is supported for now", expr);
                }

                const std::string_view edgeTypeName = edgeTypes->front()->getName();
                const std::optional edgeType = edgeTypeMap.get(edgeTypeName);

                if (!edgeType) {
                    throwError(fmt::format("Unknown edge type: {}", edgeTypeName), entityTypeExpr);
                }

                edgeFilter->addEdgeTypeConstraint(edgeType.value());
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
    Predicate* predicate = _tree->createPredicate(expr);
    predicate->generate(*_variables);
}

void ReadStmtGenerator::placeJoinsOnVars() {
    const auto createJoin = [this](VarNode* node,
                                   PlanGraphNode* lhs,
                                   PlanGraphNode* rhs) -> PlanGraphNode* {
        const auto [path, _] = _topology->getShortestPath(lhs, rhs);

        switch (path) {
            case PlanGraphTopology::PathToDependency::SameVar: {
                throwError("Unknown error. Cannot join on the same var");
            }
            case PlanGraphTopology::PathToDependency::BackwardPath: {
                // Should not happen
                throwError("Unknown error. Cannot join if the lhs and rhs are on the same islands");
            }
            case PlanGraphTopology::PathToDependency::NoPath: {
                return _tree->create<JoinNode>(node->getVarDecl(),
                                               node->getVarDecl(),
                                               JoinType::COMMON_SUCCESSOR);
            }
            case PlanGraphTopology::PathToDependency::UndirectedPath:
                return _tree->create<JoinNode>(node->getVarDecl(),
                                               node->getVarDecl(),
                                               JoinType::DIAMOND);
            }

        throwError("Unexpected erorr. Cannot place join on variables");
    };

    for (VarNode* var : _variables->getVarNodes()) {
        FilterNode* filter = _variables->getNodeFilter(var);

        if (filter->inputs().size() <= 1) {
            continue;
        }

        // Make a copy of the inputs, because the inputs vector will be modified
        std::vector inputs = filter->inputs();

        PlanGraphNode* rhsNode = inputs[0];

        for (size_t i = 1; i < inputs.size(); i++) {
            PlanGraphNode* lhsNode = inputs[i];
            lhsNode->clearOutputs();
            rhsNode->clearOutputs();

            PlanGraphNode* join = createJoin(var, lhsNode, rhsNode);
            lhsNode->connectOut(join);
            rhsNode->connectOut(join);
            join->connectOut(filter);

            rhsNode = join;
        }
    }
}

void ReadStmtGenerator::placePredicateJoins() {
    for (auto& pred : _tree->getPredicates()) {
        ExprDependencies& deps = pred->getDependencies();

        if (deps.getVarDeps().empty()) {
            throwError("Predicates without dependencies are not supported yet", pred->getExpr());
        }

        // Step 1: find the earliest point on the graph where to place the join
        VarNode* var = deps.findCommonSuccessor(_topology.get(), nullptr);

        if (!var) {
            throwError("Unknown error. Could not place predicate");
        }

        GetPropertyCache& getPropertyCache = _tree->getGetPropertyCache();
        GetEntityTypeCache& getEntityTypeCache = _tree->getGetEntityTypeCache();

        // Step 2: Place joins
        for (ExprDependencies::VarDependency& dep : deps.getVarDeps()) {
            FilterNode* filter = _variables->getNodeFilter(dep._var);

            if (auto* expr = dynamic_cast<PropertyExpr*>(dep._expr)) {
                const VarDecl* entityDecl = expr->getEntityVarDecl();
                const VarDecl* exprDecl = expr->getExprVarDecl();

                const auto* cached = getPropertyCache.cacheOrRetrieve(entityDecl, exprDecl, expr->getPropName());

                if (cached) {
                    // GetProperty is already present in the cache. Map the existing expr to the current one
                    if (!cached->_exprDecl) [[unlikely]] {
                        throwError("GetProperty expression does not have an expression variable declaration", expr);
                    }

                    expr->setExprVarDecl(cached->_exprDecl);
                } else {
                    GetPropertyWithNullNode* n = _tree->insertBefore<GetPropertyWithNullNode>(filter, expr->getPropName());
                    n->setEntityVarDecl(entityDecl);
                    n->setExpr(expr);
                }

            } else if (auto* expr = dynamic_cast<EntityTypeExpr*>(dep._expr)) {
                const VarDecl* entityDecl = expr->getEntityVarDecl();
                const VarDecl* exprDecl = expr->getExprVarDecl();

                const auto* cached = getEntityTypeCache.cacheOrRetrieve(entityDecl, exprDecl);

                if (cached) {
                    // GetEntityType is already present in the cache. Map the existing expr to the current one
                    if (!cached->_exprDecl) [[unlikely]] {
                        throwError("GetEntityType expression does not have an expression variable declaration", expr);
                    }

                    expr->setExprVarDecl(cached->_exprDecl);
                } else {
                    GetEntityTypeNode* n = _tree->insertBefore<GetEntityTypeNode>(filter);
                    n->setExpr(expr);
                    n->setEntityVarDecl(entityDecl);
                }

            } else if (dynamic_cast<const SymbolExpr*>(dep._expr)) {
                // Symbol value should already be in a column in a block, no need to change anything
            } else {
                throwError("Expression dependency could not be handled in the predicate evaluation");
            }

            insertDataFlowNode(var, dep._var);
        }

        // Step 3: Place the constraint
        FilterNode* filterNode = _variables->getNodeFilter(var);
        filterNode->addPredicate(pred.get());
        pred->setFilterNode(filterNode);
    }
}

PlanGraphNode* ReadStmtGenerator::generateEndpoint() {
    // Step 1: Find all end points
    std::vector<PlanGraphNode*> ends;
    for (const auto& node : _tree->nodes()) {
        if (node->outputs().empty()) {
            ends.push_back(node.get());
        }
    }

    if (ends.empty()) {
        /* Right now (a)-->(b)-->(c)-->(a) is a loop, which means that we
         * cannot define an endpoint.
         *
         * This needs to be explictely handled,
         * probably using "loop unrolling". When we detect a loop, we actually
         * define a new variable (a') in this example, and add a constraint,
         * WHERE a == a'.
         *
         * To implement this, we need to:
         *
         * - Allow comparing entities (e.g. a == b) and test the query:
         *   `MATCH (a)-->(b) WHERE a == b RETURN *`
         * - Then, add the unrolling logic to the query planner. This may
         *   be as simple as: in planOrigin and planTarget, if the
         *   node already exists, detect if we can come back to the same
         *   position by going backwards. If so, create a new unnamed variable
         *   and add the constraint.
         * */

        throwError("No endpoints found, loops are not supported yet");
    }

    if (ends.size() == 1) {
        // No joins needed, this endpoint can be connected to the next stage of
        // the query pipeline
        return ends[0];
    }

    // Step 2: Generate all joins
    // Algorithm:
    // - Pick the first endpoint (= rhs)
    // - For each other endpoint (= lhs):
    //     - Find the shortest path between the lhs and rhs
    //     - If the path is undirected, JOIN the two endpoints
    //     - If no path is found, CARTESIAN_PRODUCT the two endpoints
    //     - rhs becomes the join node
    /*       A              A              A
     *      / \            / \            / \
     *     B   C          B   C          B   C
     *    /     \    ->  /     \    ->  /     \
     *   D       F      D       F      D       F
     *    \     / \      \     / \      \     / \
     *     H   I   J      H   I   J      H   I   J
     *                         \ /        \   \ /
     *                         [u]         \  [u]
     *                                      \ /
     *                                      [u]   */

    PlanGraphNode* rhsNode = ends[0];

    for (size_t i = 1; i < ends.size(); i++) {
        PlanGraphNode* lhsNode = ends[i];

        const auto [path, ancestorNode] = _topology->getShortestPath(rhsNode, lhsNode);

        switch (path) {
            case PlanGraphTopology::PathToDependency::SameVar:
            case PlanGraphTopology::PathToDependency::BackwardPath: {
                // Should not happen
                throwError("Unknown error. A branch cannot have two endpoints.");
            } break;

            case PlanGraphTopology::PathToDependency::UndirectedPath: {
                // Join
                const auto* varDecl = static_cast<VarNode*>(ancestorNode)->getVarDecl();
                JoinNode* join = _tree->create<JoinNode>(varDecl,
                                                         varDecl,
                                                         JoinType::COMMON_ANCESTOR);
                rhsNode->connectOut(join);
                lhsNode->connectOut(join);

                rhsNode = join;
            } break;
            case PlanGraphTopology::PathToDependency::NoPath: {
                // Cartesian product
                CartesianProductNode* join = _tree->create<CartesianProductNode>();
                rhsNode->connectOut(join);
                lhsNode->connectOut(join);

                rhsNode = join;
            } break;
        }
    }

    // From here, there is only one endpoint remaining which can be connected
    // to the next stage of the query pipeline
    return rhsNode;
}

void ReadStmtGenerator::insertDataFlowNode(VarNode* node, VarNode* dependency) {
    FilterNode* filter = _variables->getNodeFilter(node);
    const auto [path, ancestorNode] = _topology->getShortestPath(node, dependency);

    switch (path) {
        case PlanGraphTopology::PathToDependency::SameVar:
        case PlanGraphTopology::PathToDependency::BackwardPath: {
            return;
        }

        case PlanGraphTopology::PathToDependency::UndirectedPath: {
            const auto* varDecl = static_cast<VarNode*>(ancestorNode)->getVarDecl();
            JoinNode* join = _tree->create<JoinNode>(varDecl,
                                                     varDecl,
                                                     JoinType::COMMON_ANCESTOR);
            PlanGraphNode* depBranchTip = _topology->getBranchTip(dependency);
            depBranchTip->connectOut(join);
            return;
        }

        case PlanGraphTopology::PathToDependency::NoPath: {
            // If nodes are on two different islands
            // Cartesian product. This can be optimized in the future into a ValueHashJoin
            CartesianProductNode* join = _tree->insertBefore<CartesianProductNode>(filter);
            PlanGraphNode* depBranchTip = _topology->getBranchTip(dependency);
            depBranchTip->connectOut(join);
            return;
        }
    }
}

void ReadStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
