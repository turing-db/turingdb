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

        WhereClause* where = yieldItems->getWhereClause();
        if (where) {
            generateWhereClause(where);
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

        auto insertRes = _edgesInPattern.insert(e->getDecl());
        bool existsInSet = !insertRes.second;
        if (e->getDecl() && existsInSet) {
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

    NodeFilterNode* nodeFilter = static_cast<NodeFilterNode*>(filter);

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

void ReadStmtGenerator::placePredicates() {
    for (auto& pred : _tree->getPredicates()) {
        ExprDependencies& deps = pred->getDependencies();

        if (deps.getVarDeps().empty()) {
            throwError("Predicates without dependencies are not supported yet", pred->getExpr());
        }

        // Check if all dependencies are from procedure producers (no VarNodes)
        const bool allProcedureDeps = std::ranges::all_of(deps.getVarDeps(), 
            [](const auto& dep) {
                return dep._producerNode->getOpcode() == PlanGraphOpcode::PROCEDURE_EVAL;
            }
        );

        if (allProcedureDeps) {
            placeProcedurePredicate(pred.get());
            continue;
        }

        // Step 1: find the earliest point on the graph where to place the join
        VarNode* var = deps.findCommonSuccessor(_topology.get(), nullptr);

        if (!var) {
            throwError("Unknown error. Could not place predicate");
        }

        // Step 2: Place joins. insertDataFlowNode also adds the predicate to the filter
        // for any dep requiring a join (UndirectedPath/NoPath).
        bool vhjPlaced = false;
        for (ExprDependencies::VarDependency& dep : deps.getVarDeps()) {
            generateDependency(dep._producerNode, dep._expr);
            vhjPlaced |= insertDataFlowNode(var, dep._producerNode, pred.get());
        }

        // Step 3: For single-var and common sucessor predicates
        // all deps return early (SameVar/BackwardPath) and never add the predicate,
        // so add it directly here if not already present.
        FilterNode* filterNode = _variables->getNodeFilter(var);
        const auto& filterPreds = filterNode->getPredicates();
        const bool beenPlaced = std::ranges::find(filterPreds.begin(), filterPreds.end(), pred.get()) != filterPreds.end();
        if (!vhjPlaced && !beenPlaced) {
            filterNode->addPredicate(pred.get());
            pred->setFilterNode(filterNode);
        }
    }
}

bool ReadStmtGenerator::shouldPlaceValueHashJoin(VarNode* localVar, PlanGraphNode* remoteNode) {
    if (_config->getForceValueHashJoin()) {
        return true;
    }

    if (!_config->getUseValueHashJoin()) {
        return false;
    }

    LabelSet leftLabels;
    LabelSet rightLabels;

    FilterNode* localFilter = _variables->getNodeFilter(localVar);
    if (localFilter) {
        if (NodeFilterNode* nf = localFilter->asNodeFilter()) {
            leftLabels = nf->getLabelConstraints();
        }
    }

    CardinalityEstimation estimation(_graphView);

    const VarNode* rightVar = dynamic_cast<VarNode*>(remoteNode);
    if (rightVar) {
        FilterNode* remoteFilter = _variables->getNodeFilter(rightVar);
        if (remoteFilter) {
            if (NodeFilterNode* nf = dynamic_cast<NodeFilterNode*>(remoteFilter)) {
                rightLabels = nf->getLabelConstraints();
            }
        }
        return !estimation.shouldPreferCartesian(leftLabels, rightLabels, _queryLimit);
    } else {
        // In the case where we have a Call procedure yield in the expression (for now
        // this would only work if the yield item is on the rhs), we default the
        // right side cardinality to 10
        return !estimation.shouldPreferCartesian(leftLabels, 10, _queryLimit);
    }
}

void ReadStmtGenerator::placeJoinsOnProcedures() {
    for (const auto& node : _tree->nodes()) {
        if (node->getOpcode() == PlanGraphOpcode::PROCEDURE_EVAL) {
            ProcedureEvalNode* n = static_cast<ProcedureEvalNode*>(node.get());
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

// returns true if a Value Hash Join has been place - indicating to the caller that the
// predicate does not need to be added to the filter
bool ReadStmtGenerator::insertDataFlowNode(VarNode* node, PlanGraphNode* dependency, Predicate* pred) {
    FilterNode* filter = _variables->getNodeFilter(node);

    // dependencyVarDecl may be null when the dependency is a ProcedureEvalNode
    // (which does not inherit from VarDeclProviderNode). This is valid in the
    // mixed MATCH + CALL case: the procedure dependency takes the NoPath branch
    // below, which does not use dependencyVarDecl.
    auto* depProvider = dynamic_cast<VarDeclProviderNode*>(dependency);
    const VarDecl* dependencyVarDecl = depProvider ? depProvider->getVarDecl() : nullptr;
    const auto [path, ancestorNode] = _topology->getShortestPath(node, dependency);

    switch (path) {
        case PlanGraphTopology::PathToDependency::SameVar:
        case PlanGraphTopology::PathToDependency::BackwardPath: {
            return false;
        }

        case PlanGraphTopology::PathToDependency::UndirectedPath: {
            bioassert(dependencyVarDecl, "UndirectedPath requires a VarDeclProviderNode dependency");
            const auto* varDecl = static_cast<VarNode*>(ancestorNode)->getVarDecl();
            JoinNode* join = _tree->insertBefore<JoinNode>(filter,
                                                           varDecl,
                                                           varDecl,
                                                           dependencyVarDecl,
                                                           JoinType::COMMON_ANCESTOR);

            PlanGraphNode* depBranchTip = _topology->getBranchTip(dependency);
            depBranchTip->connectOut(join);
            return false;
        }

        case PlanGraphTopology::PathToDependency::NoPath: {
            if (tryPlaceValueHashJoin(filter, node, dependency, pred)) {
                return true;
            }
            CartesianProductNode* join = _tree->insertBefore<CartesianProductNode>(filter);
            PlanGraphNode* depBranchTip = _topology->getBranchTip(dependency);
            depBranchTip->connectOut(join);
            return false;
        }
        default:
            throwError("No Path Found Between Dataflow Dependency");
    }
}

void ReadStmtGenerator::generateDependency(PlanGraphNode* producer, Expr* rawExpr) {
    auto& getPropertyCache = _tree->getGetPropertyCache();
    auto& getEntityTypeCache = _tree->getGetEntityTypeCache();

    if (PropertyExpr* expr = dynamic_cast<PropertyExpr*>(rawExpr)) {
        VarNode* varNode = dynamic_cast<VarNode*>(producer);
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

    } else if (EntityTypeExpr* expr = dynamic_cast<EntityTypeExpr*>(rawExpr)) {
        VarNode* varNode = dynamic_cast<VarNode*>(producer);
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

bool ReadStmtGenerator::tryPlaceValueHashJoin(FilterNode* filter, VarNode* node,
                                              PlanGraphNode* dependency, Predicate* pred) {
    const BinaryExpr* binExpr = dynamic_cast<const BinaryExpr*>(pred->getExpr());
    if (!binExpr) {
        return false;
    }

    const Expr* lhs = binExpr->getLHS();
    const Expr* rhs = binExpr->getRHS();

    const bool isValidLhs = lhs->getKind() == Expr::Kind::SYMBOL || lhs->getKind() == Expr::Kind::PROPERTY;
    const bool isValidRhs = rhs->getKind() == Expr::Kind::SYMBOL || rhs->getKind() == Expr::Kind::PROPERTY;
    const bool isEqualityPred = binExpr->getOperator() == BinaryOperator::Equal;
    const bool noFunctionDependencies = pred->getDependencies().getFuncDeps().empty();

    const bool canPlaceValueHashJoin = isEqualityPred && isValidLhs && isValidRhs && noFunctionDependencies;

    if (!canPlaceValueHashJoin || !shouldPlaceValueHashJoin(node, dependency)) {
        return false;
    }

    const VarDecl* firstJoinKeyDecl = lhs->getExprVarDecl();
    const VarDecl* secondJoinKeyDecl = rhs->getExprVarDecl();
    bioassert(firstJoinKeyDecl && secondJoinKeyDecl, "Both lhs and rhs of join need to have VarDecls");

    // By convention the right hand side of the expression is the dependency var decl
    const VarDecl* dependencyVarDecl = rhs->getExprVarDecl();

    JoinNode* join = _tree->insertBefore<JoinNode>(filter,
                                                   firstJoinKeyDecl,
                                                   secondJoinKeyDecl,
                                                   dependencyVarDecl,
                                                   JoinType::PREDICATE);
    PlanGraphNode* depBranchTip = _topology->getBranchTip(dependency);
    depBranchTip->connectOut(join);
    return true;
}

void ReadStmtGenerator::placeProcedurePredicate(Predicate* pred) {
    ExprDependencies& deps = pred->getDependencies();
    const auto& varDeps = deps.getVarDeps();

    // Try VHJ for binary equality predicates
    const BinaryExpr* binExpr = dynamic_cast<const BinaryExpr*>(pred->getExpr());
    if (binExpr) {
        const bool isEqual = (binExpr->getOperator() == BinaryOperator::Equal);
        const bool hasTwoDependencies = (varDeps.size() == 2);
        const bool noFuncDeps = deps.getFuncDeps().empty();
        if (isEqual && hasTwoDependencies && noFuncDeps) {
            const Expr* lhs = binExpr->getLHS();
            const Expr* rhs = binExpr->getRHS();
            const Expr::Kind lhsKind = lhs->getKind();
            const Expr::Kind rhsKind = rhs->getKind();
            const bool validLhs = (lhsKind == Expr::Kind::SYMBOL || lhsKind == Expr::Kind::PROPERTY);
            const bool validRhs = (rhsKind == Expr::Kind::SYMBOL || rhsKind == Expr::Kind::PROPERTY);

            if (validLhs && validRhs) {
                const VarDecl* firstKey = lhs->getExprVarDecl();
                const VarDecl* secondKey = rhs->getExprVarDecl();

                // Insert a ValueHashJoin between the two procedure branches.
                // Both branch tips are connected as inputs to the JoinNode,
                // which will be translated to a HashJoinProcessor in the pipeline.
                JoinNode* join = _tree->create<JoinNode>(firstKey,
                                                         secondKey,
                                                         secondKey,
                                                         JoinType::PREDICATE);
                for (const auto& dep : varDeps) {
                    generateDependency(dep._producerNode, dep._expr);
                    PlanGraphNode* tip = _topology->getBranchTip(dep._producerNode);
                    tip->connectOut(join);
                }

                return;
            }
        }
    }

    // Fallback: CartesianProduct + DataframeFilterNode for non-equality predicates
    DataframeFilterNode* filter = _tree->create<DataframeFilterNode>();
    filter->addPredicate(pred);
    pred->setFilterNode(filter);

    CartesianProductNode* cart = _tree->create<CartesianProductNode>();
    cart->connectOut(filter);

    for (const auto& dep : varDeps) {
        generateDependency(dep._producerNode, dep._expr);
        PlanGraphNode* tip = _topology->getBranchTip(dep._producerNode);
        tip->connectOut(cart);
    }
}

void ReadStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
