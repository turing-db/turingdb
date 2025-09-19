#include "PlanGraphGenerator.h"

#include <spdlog/fmt/bundled/core.h>

#include "CypherAST.h"
#include "CypherError.h"
#include "QualifiedName.h"
#include "views/GraphView.h"

#include "nodes/FilterNode.h"
#include "nodes/GetEdgeTargetNode.h"
#include "nodes/GetEdgesNode.h"
#include "nodes/GetInEdgesNode.h"
#include "nodes/GetOutEdgesNode.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/VarNode.h"

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
    : _view(view),
    _ast(&ast),
    _variables(_tree)
{
}

PlanGraphGenerator::~PlanGraphGenerator() {
}

const LabelSet* PlanGraphGenerator::getOrCreateLabelSet(const Symbols& symbols) {
    LabelNames labelNames;
    for (const Symbol* symbol : symbols) {
        labelNames.push_back(symbol->getName());
    }

    const auto labelSetCacheIt = _labelSetCache.find(labelNames);
    if (labelSetCacheIt != _labelSetCache.end()) {
        return labelSetCacheIt->second;
    }

    const LabelSet* labelSet = buildLabelSet(symbols);
    _labelSetCache[labelNames] = labelSet;

    return labelSet;
}

const LabelSet* PlanGraphGenerator::buildLabelSet(const Symbols& symbols) {
    auto labelSet = std::make_unique<LabelSet>();
    for (const Symbol* symbol : symbols) {
        const LabelID labelID = getLabel(symbol);

        if (!labelID.isValid()) {
            throwError(fmt::format("Unsupported node label: {}", symbol->getName()), symbol);
        }

        labelSet->set(labelID);
    }

    LabelSet* ptr = labelSet.get();
    _labelSets.push_back(std::move(labelSet));

    return ptr;
}

LabelID PlanGraphGenerator::getLabel(const Symbol* symbol) {
    auto res = _view.metadata().labels().get(symbol->getName());
    if (!res) {
        return LabelID {};
    }

    return res.value();
}

void PlanGraphGenerator::generate(const QueryCommand* query) {
    switch (query->getKind()) {
        case QueryCommand::Kind::SINGLE_PART_QUERY:
            generateSinglePartQuery(static_cast<const SinglePartQuery*>(query));
            break;

        default:
            throwError("Unsupported query command of type "
                           + std::to_string((unsigned)query->getKind()),
                       query);
            break;
    }
}

void PlanGraphGenerator::generateSinglePartQuery(const SinglePartQuery* query) {
    const StmtContainer* readStmts = query->getReadStmts();
    for (const Stmt* stmt : readStmts->stmts()) {
        generateStmt(stmt);
    }

    // TODO: Return statement
}

void PlanGraphGenerator::generateStmt(const Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::MATCH:
            generateMatchStmt(static_cast<const MatchStmt*>(stmt));
            break;

        default:
            throwError(fmt::format("Unsupported statement type: {}", (unsigned)stmt->getKind()), stmt);
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
        throwError("WHERE clause not supported yet", where);
    }
}

void PlanGraphGenerator::generatePatternElement(const PatternElement* element) {
    if (element->size() == 0) {
        throwError("Empty match pattern element", element);
    }

    const NodePattern* origin = dynamic_cast<const NodePattern*>(element->getRootEntity());
    if (!origin) {
        throwError("Pattern element origin must be a node pattern", element);
    }

    PlanGraphNode* currentNode = generatePatternElementOrigin(origin);

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

PlanGraphNode* PlanGraphGenerator::generatePatternElementOrigin(const NodePattern* origin) {
    const NodePatternData* data = origin->getData();
    const LabelSet& labelset = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = origin->getDecl();

    // Scan nodes
    PlanGraphNode* currentNode = _tree.create<ScanNodesNode>();

    const auto [var, filter] = _variables.getOrCreateVarNodeAndFilter(decl);
    currentNode->connectOut(filter);
    currentNode = var;

    auto* typedFilter = static_cast<FilterNodeNode*>(filter);

    typedFilter->addLabelConstraints(labelset);

    for (const auto& [propType, expr] : exprConstraints) {
        typedFilter->addPropertyConstraint(propType._id, expr, BinaryOperator::Equal);
    }

    return currentNode;
}

PlanGraphNode* PlanGraphGenerator::generatePatternElementEdge(PlanGraphNode* currentNode,
                                                              const EdgePattern* edge) {
    // Expand edge based on direction

    switch (edge->getDirection()) {
        case EdgePattern::Direction::Undirected: {
            auto* expandEdges = _tree.create<GetEdgesNode>();
            currentNode->connectOut(expandEdges);
            currentNode = expandEdges;
            break;
        }
        case EdgePattern::Direction::Backward: {
            auto* expandEdges = _tree.create<GetInEdgesNode>();
            currentNode->connectOut(expandEdges);
            currentNode = expandEdges;
            break;
        } break;
        case EdgePattern::Direction::Forward: {
            auto* expandEdges = _tree.create<GetOutEdgesNode>();
            currentNode->connectOut(expandEdges);
            currentNode = expandEdges;
            break;
        }
    }

    // Edge constraints
    const EdgePatternData* data = edge->getData();
    const auto& edgeTypes = data->edgeTypeConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = edge->getDecl();

    if (edgeTypes.size() > 1) {
        throwError("Only one edge type constraint is supported for now", edge);
    }

    auto [var, filter] = _variables.getOrCreateVarNodeAndFilter(decl);
    currentNode->connectOut(filter);
    currentNode = var;

    auto* typedFilter = static_cast<FilterEdgeNode*>(filter);

    for (const auto& [propType, expr] : exprConstraints) {
        typedFilter->addPropertyConstraint(propType._id, expr, BinaryOperator::Equal);
    }

    for (const EdgeTypeID edgeTypeID : edgeTypes) {
        typedFilter->addEdgeTypeConstraint(edgeTypeID);
    }

    return currentNode;
}

PlanGraphNode* PlanGraphGenerator::generatePatternElementTarget(PlanGraphNode* currentNode,
                                                                const NodePattern* target) {
    // Target nodes
    const NodePatternData* data = target->getData();
    const auto& labelset = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = target->getDecl();

    auto* getTargetNode = _tree.create<GetEdgeTargetNode>();
    currentNode->connectOut(getTargetNode);
    currentNode = getTargetNode;

    auto [var, filter] = _variables.getOrCreateVarNodeAndFilter(decl);
    currentNode->connectOut(filter);
    currentNode = var;

    auto* typedFilter = static_cast<FilterNodeNode*>(filter);

    typedFilter->addLabelConstraints(labelset);

    for (const auto& [propType, expr] : exprConstraints) {
        typedFilter->addPropertyConstraint(propType._id, expr, BinaryOperator::Equal);
    }

    return currentNode;
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
