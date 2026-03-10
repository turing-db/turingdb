#include "ReadStmtGenerator.h"

#include <spdlog/fmt/bundled/format.h>
#include <spdlog/spdlog.h>

#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "FunctionInvocation.h"
#include "Literal.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "PlanGraph.h"
#include "PlanGraphTopology.h"
#include "PlanGraphVariables.h"
#include "Predicate.h"
#include "Symbol.h"
#include "SymbolChain.h"
#include "WhereClause.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"
#include "expr/BinaryExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/ListExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"
#include "metadata/LabelSet.h"
#include "nodes/CartesianProductNode.h"
#include "nodes/FilterNode.h"
#include "nodes/GetEdgeTargetNode.h"
#include "nodes/GetEdgesNode.h"
#include "nodes/GetEntityTypeNode.h"
#include "nodes/GetInEdgesNode.h"
#include "nodes/GetOutEdgesNode.h"
#include "nodes/GetPropertyNode.h"
#include "nodes/GetPropertyWithNullNode.h"
#include "nodes/JoinNode.h"
#include "nodes/ProcedureEvalNode.h"
#include "nodes/ProduceResultsNode.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/VarNode.h"
#include "nodes/ShortestPathNode.h"
#include "nodes/VectorSearchNode.h"
#include "nodes/PathExplorerNode.h"

#include "QuantifiedPath.h"
#include "CardinalityEstimation.h"
#include "PlanGenConfig.h"

#include "stmt/Stmt.h"
#include "stmt/MatchStmt.h"
#include "stmt/CallStmt.h"
#include "stmt/LoadCSVStmt.h"
#include "nodes/LoadCSVNode.h"
#include "stmt/VectorSearchStmt.h"

#include "PlannerException.h"

#include "BioAssert.h"

using namespace db;

ReadStmtGenerator::ReadStmtGenerator(const CypherAST* ast,
                                     GraphView graphView,
                                     const PlanGenConfig* config,
                                     PlanGraph* tree,
                                     PlanGraphVariables* variables)
    : _ast(ast),
    _graphView(graphView),
    _config(config),
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

        case Stmt::Kind::LOAD_CSV:
            generateLoadCSVStmt(static_cast<const LoadCSVStmt*>(stmt));
            break;

        case Stmt::Kind::VECTOR_SEARCH:
            generateVectorSearchStmt(static_cast<const VectorSearchStmt*>(stmt));
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

    if (yield && yield->getItems()) {
        YieldItems* yieldItems = yield->getItems();
        for (SymbolExpr* item : yieldItems->getItems()) {
            _variables->setProducer(item->getDecl(), procNode);
        }
    }

    if (callStmt->isStandaloneCall()) {
        _tree->newOut<ProduceResultsNode>(procNode);
        return;
    } else {
        bioassert(yield, "Procedure call without YIELD must be a standalone CALL");
    }
}

void ReadStmtGenerator::generateLoadCSVStmt(const LoadCSVStmt* stmt) {
    const VarDecl* aliasDecl = stmt->getAliasDecl();
    bioassert(aliasDecl, "LoadCSVStmt alias does not have a VarDecl");

    LoadCSVNode* loadCSVNode = _tree->create<LoadCSVNode>(stmt->getFilePath(),
                                                          stmt->hasHeaders(),
                                                          stmt->skipOnError(),
                                                          aliasDecl);

    _variables->setProducer(aliasDecl, loadCSVNode);
}

void ReadStmtGenerator::generateVectorSearchStmt(const VectorSearchStmt* stmt) {
    // Extract float values from ListExpr
    std::vector<float> queryVector;
    const ListExpr* listExpr = stmt->getQueryVector();

    for (Expr* elem : listExpr->getElements()) {
        if (elem->getKind() != Expr::Kind::LITERAL) {
            throwError("VECTOR SEARCH query vector elements must be literals", stmt);
        }

        const LiteralExpr* litExpr = static_cast<const LiteralExpr*>(elem);
        const Literal* lit = litExpr->getLiteral();

        if (lit->getKind() == Literal::Kind::DOUBLE) {
            const DoubleLiteral* doubleLit = static_cast<const DoubleLiteral*>(lit);
            queryVector.push_back(static_cast<float>(doubleLit->getValue()));
        } else if (lit->getKind() == Literal::Kind::INTEGER) {
            const IntegerLiteral* intLit = static_cast<const IntegerLiteral*>(lit);
            queryVector.push_back(static_cast<float>(intLit->getValue()));
        } else {
            throwError("VECTOR SEARCH query vector elements must be numeric", stmt);
        }
    }

    VectorSearchNode* node = _tree->create<VectorSearchNode>(
        stmt->getIndexName(),
        stmt->getK(),
        std::move(queryVector));

    // Register the 'ids' variable for downstream use
    const YieldClause* yield = stmt->getYield();
    if (yield) {
        YieldItems* yieldItems = yield->getItems();
        for (SymbolExpr* yieldItemExpr : *yieldItems) {
            const VarDecl* decl = yieldItemExpr->getExprVarDecl();
            node->setIDsVarDecl(decl);
            _variables->setProducer(yieldItemExpr->getDecl(), node);
        }
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

    _edgesInPattern.clear();

    const auto& chain = element->getElementChain();
    for (const auto& [edge, node] : chain) {
        const EdgePattern* e = dynamic_cast<const EdgePattern*>(edge);
        if (!e) {
            throwError("Pattern element edge must be an edge pattern", element);
        }

        if (e->getDecl() && !_edgesInPattern.insert(e->getDecl()).second) {
            throwError("Re-using the same edge variable in a single pattern is not supported", edge);
        }

        const NodePattern* n = dynamic_cast<const NodePattern*>(node);
        if (!n) {
            throwError("Pattern element node must be a node pattern", element);
        }

        if (e->getQuantifiedPath()) {
            PlanGraphNode* bfsNode = generatePatternElementVariableLengthPath(currentNode, e, n);
            currentNode = generatePatternElementTarget(bfsNode, n, false);
        } else {
            currentNode = generatePatternElementEdge(currentNode, e);
            currentNode = generatePatternElementTarget(currentNode, n);
        }
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
        _variables->setProducer(decl, var);

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

VarNode* ReadStmtGenerator::generatePatternElementEdge(PlanGraphNode* prevNode,
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
        _variables->setProducer(decl, var);
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

VarNode* ReadStmtGenerator::generatePatternElementTarget(PlanGraphNode* prevNode,
                                                         const NodePattern* target,
                                                         bool generateGetTarget) {
    // Target nodes
    const NodePatternData* data = target->getData();
    const std::span labels = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = target->getDecl();
    const LabelMap& labelMap = _graphMetadata.labels();
    const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

    PlanGraphNode* currentNode = generateGetTarget
        ? _tree->newOut<GetEdgeTargetNode>(prevNode)
        : prevNode;

    auto [var, filter] = _variables->getVarNodeAndFilter(decl);
    if (!var) {
        std::tie(var, filter) = _variables->createVarNodeAndFilter(decl);
        _variables->setProducer(decl, var);

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

PlanGraphNode* ReadStmtGenerator::generatePatternElementVariableLengthPath(PlanGraphNode* prevNode,
                                                                           const EdgePattern* edge,
                                                                           const NodePattern* target) {
    const QuantifiedPath* qp = edge->getQuantifiedPath();
    const int64_t minHops = qp->getLhs();
    const int64_t maxHops = qp->getRhs();

    // Checking if the edge variable is already used
    const VarDecl* edgeDecl = edge->getDecl();
    auto [edgeVar, edgeFilter] = _variables->getVarNodeAndFilter(edgeDecl);

    if (edgeVar) {
        throwError("Re-using the same edge variable, this is not supported", edge);
    }

    const VarDecl* nodeDecl = target->getDecl();

    if (minHops < 0) {
        throwError("Variable-length path minimum hops must be greater than or equal to 0", edge);
    }

    if (maxHops < 1) {
        throwError("Variable-length path maximum hops must be greater than or equal to 1", edge);
    }

    PathExplorerNode* expandNode = _tree->newOut<PathExplorerNode>(prevNode,
                                                                   edgeDecl,
                                                                   nodeDecl,
                                                                   minHops,
                                                                   maxHops);

    switch (edge->getDirection()) {
        case EdgePattern::Direction::Undirected: {
            expandNode->setDir(PathExplorationDir::BOTH);
        } break;
        case EdgePattern::Direction::Backward: {
            expandNode->setDir(PathExplorationDir::BACKWARD);
        } break;
        case EdgePattern::Direction::Forward: {
            expandNode->setDir(PathExplorationDir::FORWARD);
        } break;
    }

    _variables->setProducer(edgeDecl, expandNode);
    return expandNode;
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
        const std::vector inputs = filter->inputs();

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
    const bool useValueHashJoin = _config->getUseValueHashJoin();
    std::vector<FilterNode*> vhjFilters;

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

        // Try to place a value hash join instead of cartesian product + filter
        if (useValueHashJoin) {
            if (tryPlaceValueHashJoin(pred.get(), deps, var)) {
                vhjFilters.push_back(_variables->getNodeFilter(var));
                continue;
            }
        }

        // Step 2: Place joins
        for (ExprDependencies::VarDependency& dep : deps.getVarDeps()) {
            generateDependency(dep._producerNode, dep._expr);
            insertDataFlowNode(var, dep._producerNode);
        }

        // Step 3: Place the constraint
        FilterNode* filterNode = _variables->getNodeFilter(var);
        filterNode->addPredicate(pred.get());
        pred->setFilterNode(filterNode);
    }

    // Bypass filters left empty by value hash joins.
    // We do this here rather than in a general optimizer pass to avoid
    // masking planner bugs that accidentally produce empty filters.
    for (FilterNode* filterNode : vhjFilters) {
        if (!filterNode->isEmpty()) {
            continue;
        }

        const auto inputs = filterNode->inputs();
        const auto outputs = filterNode->outputs();

        filterNode->clearInputs();
        filterNode->clearOutputs();

        for (PlanGraphNode* input : inputs) {
            for (PlanGraphNode* output : outputs) {
                input->connectOut(output);
            }
        }

        _tree->removeNode(filterNode);
    }
}

bool ReadStmtGenerator::tryPlaceValueHashJoin(Predicate* pred,
                                               ExprDependencies& deps,
                                               VarNode* var) {
    // Must have exactly 2 variable dependencies and no function dependencies
    auto& varDeps = deps.getVarDeps();
    if (varDeps.size() != 2 || !deps.getFuncDeps().empty()) {
        return false;
    }

    // Must be a BinaryExpr with Equal operator
    const Expr* expr = pred->getExpr();
    if (expr->getKind() != Expr::Kind::BINARY) {
        return false;
    }
    const auto* binExpr = static_cast<const BinaryExpr*>(expr);
    if (binExpr->getOperator() != BinaryOperator::Equal) {
        return false;
    }

    // Both dep expressions must resolve to a single VarDecl (simple property/symbol)
    auto& dep0 = varDeps[0];
    auto& dep1 = varDeps[1];

    if (!dep0._expr->getExprVarDecl() || !dep1._expr->getExprVarDecl()) {
        return false;
    }

    // Both producers must be VarNodes (graph pattern traversals)
    if (dep0._producerNode->getOpcode() != PlanGraphOpcode::VAR ||
        dep1._producerNode->getOpcode() != PlanGraphOpcode::VAR) {
        return false;
    }

    // Check which dep is on a different island from var (NoPath = remote)
    const auto [path0, ancestor0] = _topology->getShortestPath(var, dep0._producerNode);
    const auto [path1, ancestor1] = _topology->getShortestPath(var, dep1._producerNode);

    ExprDependencies::VarDependency* localDep = nullptr;
    ExprDependencies::VarDependency* remoteDep = nullptr;

    if (path0 == PlanGraphTopology::PathToDependency::NoPath &&
        path1 != PlanGraphTopology::PathToDependency::NoPath) {
        remoteDep = &dep0;
        localDep = &dep1;
    } else if (path1 == PlanGraphTopology::PathToDependency::NoPath &&
               path0 != PlanGraphTopology::PathToDependency::NoPath) {
        remoteDep = &dep1;
        localDep = &dep0;
    } else {
        // Both on same island or both remote - fall back to cartesian product
        return false;
    }

    // Skip VHJ when the cartesian product is small enough to be cheap
    if (!_config->getForceValueHashJoin()) {
        auto* localVar = dynamic_cast<VarNode*>(localDep->_producerNode);
        auto* remoteVar = dynamic_cast<VarNode*>(remoteDep->_producerNode);
        if (localVar && remoteVar) {
            LabelSet leftLabels;
            LabelSet rightLabels;

            FilterNode* localFilter = _variables->getNodeFilter(localVar);
            if (localFilter) {
                if (auto* nf = localFilter->asNodeFilter()) {
                    leftLabels = nf->getLabelConstraints();
                }
            }

            FilterNode* remoteFilter = _variables->getNodeFilter(remoteVar);
            if (remoteFilter) {
                if (auto* nf = remoteFilter->asNodeFilter()) {
                    rightLabels = nf->getLabelConstraints();
                }
            }

            CardinalityEstimation estimation(_graphView);
            if (estimation.isSmallCartesianProduct(leftLabels, rightLabels)) {
                return false;
            }
        }
    }

    // Generate property dependencies (place GetPropertyNodes)
    generateDependency(localDep->_producerNode, localDep->_expr);
    generateDependency(remoteDep->_producerNode, remoteDep->_expr);

    // Handle local dep's data flow (may need a join within its island)
    insertDataFlowNode(var, localDep->_producerNode);

    // Create PREDICATE join instead of CartesianProduct for the remote dep
    const VarDecl* leftDecl = localDep->_expr->getExprVarDecl();
    const VarDecl* rightDecl = remoteDep->_expr->getExprVarDecl();

    FilterNode* filterNode = _variables->getNodeFilter(var);
    JoinNode* joinNode = _tree->insertBefore<JoinNode>(filterNode,
                                                        leftDecl,
                                                        rightDecl,
                                                        JoinType::PREDICATE);
    PlanGraphNode* remoteBranchTip = _topology->getBranchTip(remoteDep->_producerNode);
    remoteBranchTip->connectOut(joinNode);

    return true;
}

void ReadStmtGenerator::placeJoinsOnProcedures() {
    for (const auto& node : _tree->nodes()) {
        if (node->getOpcode() == PlanGraphOpcode::PROCEDURE_EVAL) {
            auto* n = static_cast<ProcedureEvalNode*>(node.get());
            const ExprChain* args = n->getFuncExpr()->getFunctionInvocation()->getArguments();

            for (Expr* arg : *args) {
                ExprDependencies deps;
                deps.genExprDependencies(*_variables, arg);

                for (ExprDependencies::VarDependency& dep : deps.getVarDeps()) {
                    generateDependency(dep._producerNode, dep._expr);
                    const auto [path, ancestorNode] = _topology->getShortestPath(n, dep._producerNode);

                    switch (path) {
                        case PlanGraphTopology::PathToDependency::SameVar: {
                            throwError("Unknown error. Cannot place procedure call on the same var", args);
                        }

                        case PlanGraphTopology::PathToDependency::BackwardPath: {
                            return;
                        }

                        case PlanGraphTopology::PathToDependency::UndirectedPath: {
                            // Join
                            const auto* varDecl = static_cast<VarNode*>(ancestorNode)->getVarDecl();
                            JoinNode* join = _tree->create<JoinNode>(varDecl,
                                                                     varDecl,
                                                                     JoinType::COMMON_ANCESTOR);
                            PlanGraphNode* depBranchTip = _topology->getBranchTip(dep._producerNode);
                            depBranchTip->connectOut(join);
                            return;
                        }

                        case PlanGraphTopology::PathToDependency::NoPath: {
                            PlanGraphNode* depBranchTip = _topology->getBranchTip(dep._producerNode);
                            depBranchTip->connectOut(n);
                            return;
                        }
                    }
                }
            }
        }
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
                if (lhsNode->getOpcode() == PlanGraphOpcode::SHORTEST_PATH ||
                    rhsNode->getOpcode() == PlanGraphOpcode::SHORTEST_PATH) {
                    throwError("Common Ancestor Joins With Shortest Path Unsupported");
                }

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

void ReadStmtGenerator::insertShortestPathNode(VarNode* source,
                                               VarNode* target,
                                               const PropertyType& edgeType,
                                               const VarDecl* distDecl,
                                               const VarDecl* pathDecl) {
    auto* sourceTip = _topology->getBranchTip(source);
    auto* targetTip = _topology->getBranchTip(target);

    ShortestPathNode* node = _tree->create<ShortestPathNode>(source->getVarDecl(),
                                                             target->getVarDecl(),
                                                             distDecl,
                                                             pathDecl,
                                                             edgeType);
    sourceTip->connectOut(node);
    targetTip->connectOut(node);
    _variables->setProducer(distDecl, node);
    _variables->setProducer(pathDecl, node);
}

void ReadStmtGenerator::insertDataFlowNode(VarNode* node, PlanGraphNode* dependency) {
    FilterNode* filter = _variables->getNodeFilter(node);
    const auto* dependencyVarDecl = static_cast<VarDeclProviderNode*>(dependency)->getVarDecl();
    const auto [path, ancestorNode] = _topology->getShortestPath(node, dependency);

    switch (path) {
        case PlanGraphTopology::PathToDependency::SameVar:
        case PlanGraphTopology::PathToDependency::BackwardPath: {
            return;
        }

        case PlanGraphTopology::PathToDependency::UndirectedPath: {
            const auto* varDecl = static_cast<VarNode*>(ancestorNode)->getVarDecl();
            JoinNode* join = _tree->insertBefore<JoinNode>(filter,
                                                           varDecl,
                                                           varDecl,
                                                           dependencyVarDecl,
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

void ReadStmtGenerator::generateDependency(PlanGraphNode* producer, Expr* rawExpr) {
    auto& getPropertyCache = _tree->getGetPropertyCache();
    auto& getEntityTypeCache = _tree->getGetEntityTypeCache();

    if (auto* expr = dynamic_cast<PropertyExpr*>(rawExpr)) {
        auto* varNode = dynamic_cast<VarNode*>(producer);
        FilterNode* filter = _variables->getNodeFilter(varNode);

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

    } else if (auto* expr = dynamic_cast<EntityTypeExpr*>(rawExpr)) {
        auto* varNode = dynamic_cast<VarNode*>(producer);
        FilterNode* filter = _variables->getNodeFilter(varNode);
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

    } else if (dynamic_cast<const SymbolExpr*>(rawExpr)) {
        // Symbol value should already be in a column in a block, no need to change anything
    } else {
        throwError("Expression dependency could not be handled in the predicate evaluation");
    }
}

void ReadStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
