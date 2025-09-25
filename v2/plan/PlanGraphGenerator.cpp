#include "PlanGraphGenerator.h"

#include "CypherAST.h"
#include "CypherError.h"
#include "PlanGraphTopology.h"
#include "WherePredicate.h"
#include "QualifiedName.h"
#include "WhereClause.h"
#include "decl/VarDecl.h"
#include "nodes/MaterializeNode.h"
#include "views/GraphView.h"

#include "nodes/JoinNode.h"
#include "nodes/CartesianProductNode.h"
#include "nodes/FilterNode.h"
#include "nodes/GetEdgeTargetNode.h"
#include "nodes/GetEdgesNode.h"
#include "nodes/GetInEdgesNode.h"
#include "nodes/GetOutEdgesNode.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/VarNode.h"
#include "nodes/ProduceResultsNode.h"

#include "expr/PropertyExpr.h"
#include "expr/BinaryExpr.h"
#include "expr/UnaryExpr.h"
#include "expr/StringExpr.h"
#include "expr/NodeLabelExpr.h"
#include "expr/PathExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/LiteralExpr.h"

#include "PlanGraph.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "stmt/StmtContainer.h"
#include "stmt/Stmt.h"
#include "stmt/MatchStmt.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "NodePattern.h"
#include "decl/PatternData.h"

#include "Symbol.h"

#include "PlannerException.h"

using namespace db::v2;
using namespace db;

PlanGraphGenerator::PlanGraphGenerator(const CypherAST& ast,
                                       const GraphView& view,
                                       const QueryCallback& callback)
    : _ast(&ast),
      _view(view),
      _variables(_tree) {
}

PlanGraphGenerator::~PlanGraphGenerator() {
}

void PlanGraphGenerator::generate(const QueryCommand* query) {
    switch (query->getKind()) {
        case QueryCommand::Kind::SINGLE_PART_QUERY:
            generateSinglePartQuery(static_cast<const SinglePartQuery*>(query));
            break;

        default:
            throwError(fmt::format("Unsupported query command of type {}", (uint64_t)query->getKind()), query);
            break;
    }
}

void PlanGraphGenerator::generateSinglePartQuery(const SinglePartQuery* query) {
    const StmtContainer* readStmts = query->getReadStmts();
    const StmtContainer* updateStmts = query->getUpdateStmts();
    const ReturnStmt* returnStmt = query->getReturnStmt();

    // Generate read statements (optional)
    if (readStmts) {
        for (const Stmt* stmt : readStmts->stmts()) {
            generateStmt(stmt);
        }
    }

    // Generate update statements (optional)
    if (updateStmts) {
        throwError("Update statements are not supported yet", query);
    } else {
        if (!returnStmt) {
            // Return statement is mandatory if there are no update statements
            throwError("Return statement is missing", query);
        }
    }

    // Place joins on vars that have more than one input
    placeJoinsOnVars();

    // Place joins based on property expressions
    placePropertyExprJoins();

    // Place joins based on where predicates
    placePredicateJoins();

    // Generate return statement
    if (returnStmt) {
        generateReturnStmt(returnStmt);
    }
}

void PlanGraphGenerator::generateStmt(const Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::MATCH:
            generateMatchStmt(static_cast<const MatchStmt*>(stmt));
            break;

        default:
            throwError(fmt::format("Unsupported statement type: {}", (uint64_t)stmt->getKind()), stmt);
            break;
    }
}

void PlanGraphGenerator::generateMatchStmt(const MatchStmt* stmt) {
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

void PlanGraphGenerator::generateReturnStmt(const ReturnStmt* stmt) {
    std::vector<PlanGraphNode*> ends;
    for (const auto& node : _tree.nodes()) {
        if (node->outputs().empty()) {
            ends.push_back(node.get());
        }
    }

    if (ends.empty()) {
        throwError("No end points found", stmt);
    }

    PlanGraphNode* a = (*ends.begin());

    // If there is only one end point, the loop is skipped

    for (auto itB = ++ends.begin(); itB != ends.end(); itB++) {
        PlanGraphNode* b = *itB;

        const auto path = PlanGraphTopology::getShortestPath(a, b);

        switch (path) {
            case PlanGraphTopology::PathToDependency::SameVar:
            case PlanGraphTopology::PathToDependency::BackwardPath: {
                // Should not happen
                throwError("Unknown error", a);
                continue;
            } break;

            case PlanGraphTopology::PathToDependency::UndirectedPath: {
                // Join
                JoinNode* join = _tree.create<JoinNode>();
                a->connectOut(join);
                b->connectOut(join);

                a = join;
            } break;
            case PlanGraphTopology::PathToDependency::NoPath: {
                // Cartesian product
                CartesianProductNode* join = _tree.create<CartesianProductNode>();
                a->connectOut(join);
                b->connectOut(join);

                a = join;
            } break;
        }
    }

    // Connect the last node to the output
    auto* results = _tree.create<ProduceResultsNode>();
    a->connectOut(results);
}

void PlanGraphGenerator::generateWhereClause(const WhereClause* where) {
    const Expr* expr = where->getExpr();

    unwrapWhereExpr(expr);
}

void PlanGraphGenerator::generatePatternElement(const PatternElement* element) {
    if (element->size() == 0) {
        throwError("Empty match pattern element", element);
    }

    const NodePattern* origin = dynamic_cast<const NodePattern*>(element->getRootEntity());
    if (!origin) {
        throwError("Pattern element origin must be a node pattern", element);
    }

    // Reset the decl order of variables (each branch starts at 0)
    _variables.resetDeclOrder();

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

VarNode* PlanGraphGenerator::generatePatternElementOrigin(const NodePattern* origin) {
    const NodePatternData* data = origin->getData();
    const LabelSet& labelset = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = origin->getDecl();

    auto [var, filter] = _variables.getVarNodeAndFilter(decl);

    if (!var) {
        // Scan nodes
        PlanGraphNode* scan = _tree.create<ScanNodesNode>();
        std::tie(var, filter) = _variables.createVarNodeAndFilter(decl);

        scan->connectOut(filter);
    }

    auto* typedFilter = static_cast<FilterNodeNode*>(filter);
    typedFilter->addLabelConstraints(labelset);

    for (const auto& [propType, expr] : exprConstraints) {
        _propConstraints.push_back(std::make_unique<PropertyConstraint>(var, propType._id, expr));
    }

    return var;
}

VarNode* PlanGraphGenerator::generatePatternElementEdge(VarNode* prevNode,
                                                        const EdgePattern* edge) {
    // Expand edge based on direction

    PlanGraphNode* currentNode = nullptr;
    switch (edge->getDirection()) {
        case EdgePattern::Direction::Undirected: {
            currentNode = _tree.newOut<GetEdgesNode>(prevNode);
        } break;
        case EdgePattern::Direction::Backward: {
            currentNode = _tree.newOut<GetInEdgesNode>(prevNode);
        } break;
        case EdgePattern::Direction::Forward: {
            currentNode = _tree.newOut<GetOutEdgesNode>(prevNode);
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

    auto [var, filter] = _variables.getVarNodeAndFilter(decl);
    if (!var) {
        std::tie(var, filter) = _variables.createVarNodeAndFilter(decl);
    } else {
        incrementDeclOrders(prevNode->getDeclOdrer(), filter);
    }

    currentNode->connectOut(filter);
    auto* typedFilter = static_cast<FilterEdgeNode*>(filter);

    for (const auto& [propType, expr] : exprConstraints) {
        _propConstraints.push_back(std::make_unique<PropertyConstraint>(var, propType._id, expr));
    }

    for (const EdgeTypeID edgeTypeID : edgeTypes) {
        typedFilter->addEdgeTypeConstraint(edgeTypeID);
    }

    return var;
}

VarNode* PlanGraphGenerator::generatePatternElementTarget(VarNode* prevNode,
                                                          const NodePattern* target) {
    // Target nodes
    const NodePatternData* data = target->getData();
    const auto& labelset = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = target->getDecl();

    PlanGraphNode* currentNode = _tree.newOut<GetEdgeTargetNode>(prevNode);

    auto [var, filter] = _variables.getVarNodeAndFilter(decl);
    if (!var) {
        std::tie(var, filter) = _variables.createVarNodeAndFilter(decl);
    } else {
        incrementDeclOrders(prevNode->getDeclOdrer(), filter);
    }

    currentNode->connectOut(filter);
    auto* typedFilter = static_cast<FilterNodeNode*>(filter);
    typedFilter->addLabelConstraints(labelset);

    for (const auto& [propType, expr] : exprConstraints) {
        _propConstraints.push_back(std::make_unique<PropertyConstraint>(var, propType._id, expr));
    }

    return var;
}

void PlanGraphGenerator::unwrapWhereExpr(const Expr* expr) {
    if (expr->getKind() == Expr::Kind::NODE_LABEL) {
        // Node label expression can be pushed down to the var node
        //
        // WARNING
        // NODE_LABEL is missleading
        // It can also refer to an edge type such as: e:KNOWS

        const NodeLabelExpr* entityTypeExpr = static_cast<const NodeLabelExpr*>(expr);
        const VarDecl* decl = entityTypeExpr->getDecl();
        const VarNode* varNode = _variables.getVarNode(decl);
        FilterNode* filter = _variables.getNodeFilter(varNode);

        if (decl->getType() == EvaluatedType::NodePattern) {
            FilterNodeNode* nodeFilter = static_cast<FilterNodeNode*>(filter);
            nodeFilter->addLabelConstraints(entityTypeExpr->labelSet());

        } else if (decl->getType() == EvaluatedType::EdgePattern) {
            FilterEdgeNode* edgeFilter = static_cast<FilterEdgeNode*>(filter);

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
    WherePredicate* predicate = _tree.createWherePredicate(expr);
    predicate->generate(_variables);

    const auto& dependencies = predicate->getDependencies();
    if (dependencies.empty()) {
        throwError("Where clauses without dependencies are not supported yet", expr);
    }

    const VarNode* firstVar = dependencies.begin()->_var;
    uint32_t _lastDeclOrder = firstVar->getDeclOdrer();
    const VarNode* _lastDependency = firstVar;

    for (const auto& dep : dependencies) {
        if (_lastDeclOrder < dep._var->getDeclOdrer()) {
            _lastDeclOrder = dep._var->getDeclOdrer();
            _lastDependency = dep._var;
        }
    }

    FilterNode* filterNode = _variables.getNodeFilter(_lastDependency);
    filterNode->addWherePredicate(predicate);
    predicate->setFilterNode(filterNode);
}

void PlanGraphGenerator::throwError(std::string_view msg, const void* obj) const {
    const SourceLocation* location = _ast->getLocation(obj);
    std::string errorMsg;

    CypherError err {_ast->getQueryString()};
    err.setTitle("Query plan error");
    err.setErrorMsg(msg);

    if (location) {
        err.setLocation(*location);
    }

    err.generate(errorMsg);

    throw PlannerException(std::move(errorMsg));
}

void PlanGraphGenerator::incrementDeclOrders(uint32_t declOrder, PlanGraphNode* origin) {
    std::queue<PlanGraphNode*> q;
    q.push(origin);

    while (!q.empty()) {
        auto* node = q.front();
        q.pop();

        if (node->getOpcode() == PlanGraphOpcode::VAR) {
            auto* varNode = static_cast<VarNode*>(node);
            varNode->setDeclOdrer(varNode->getDeclOdrer() + declOrder + 1);
        }

        for (auto* out : node->outputs()) {
            q.push(out);
        }
    }
}

void PlanGraphGenerator::placeJoinsOnVars() {
    for (auto [var, filter] : _variables.getNodeFiltersMap()) {
        if (filter->inputs().size() > 1) {
            _tree.insertBefore<JoinNode>(filter);
        }
    }
}

void PlanGraphGenerator::placePropertyExprJoins() {
    for (auto& prop : _propConstraints) {
        auto& deps = prop->dependencies;
        const auto& depContainer = deps.getDependencies();

        // Generate missing dependencies
        deps.genExprDependencies(_variables, prop->expr);

        // Step 1: find the latest dependency
        auto it = depContainer.begin();
        const VarNode* var = prop->var;
        uint32_t order = var->getDeclOdrer();

        for (; it != depContainer.end(); ++it) {
            if (order < it->_var->getDeclOdrer()) {
                order = it->_var->getDeclOdrer();
                var = it->_var;
            }
        }

        // Step 2: Place the constraint
        auto* filter = _variables.getNodeFilter(var);
        filter->addPropertyConstraint(prop.get());

        // Step 3: place joins
        insertDataFlowNode(var, prop->var);

        for (const auto& dep : deps.getDependencies()) {
            insertDataFlowNode(var, dep._var);
        }
    }
}

void PlanGraphGenerator::placePredicateJoins() {
    for (const auto& pred : _tree.wherePredicates()) {
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

void PlanGraphGenerator::insertDataFlowNode(const VarNode* node, VarNode* dependency) {
    FilterNode* filter = _variables.getNodeFilter(node);
    const auto path = PlanGraphTopology::getShortestPath(node, dependency);

    switch (path) {
        case PlanGraphTopology::PathToDependency::SameVar: {
            // Nothing to be done
            return;
        }

        case PlanGraphTopology::PathToDependency::BackwardPath: {
            // If only walked backward
            // Materialize
            _tree.insertBefore<MaterializeNode>(filter);
            return;
        }

        case PlanGraphTopology::PathToDependency::UndirectedPath: {
            // If had to walk both backward and forward
            // Join
            JoinNode* join = _tree.insertBefore<JoinNode>(filter);
            auto* depBranchTip = PlanGraphTopology::getBranchTip(dependency);
            depBranchTip->connectOut(join);
            return;
        }

        case PlanGraphTopology::PathToDependency::NoPath: {
            // If nodes are on two different islands
            // ValueHashJoin
            JoinNode* join = _tree.insertBefore<JoinNode>(filter);
            auto* depBranchTip = PlanGraphTopology::getBranchTip(dependency);
            depBranchTip->connectOut(join);
            return;
        }
    }
}
