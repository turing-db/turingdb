#include "ReadStmtAnalyzer.h"

#include "AnalyzeException.h"
#include "ExprAnalyzer.h"
#include "metadata/GraphMetadata.h"

#include "DiagnosticsManager.h"
#include "CypherAST.h"
#include "decl/DeclContext.h"
#include "decl/VarDecl.h"
#include "decl/PatternData.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/Skip.h"
#include "stmt/Limit.h"
#include "stmt/MatchStmt.h"
#include "QualifiedName.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Symbol.h"
#include "WhereClause.h"

#include "expr/Expr.h"
#include "expr/BinaryExpr.h"
#include "expr/Literal.h"
#include "expr/LiteralExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/PathExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/StringExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"

using namespace db::v2;

ReadStmtAnalyzer::ReadStmtAnalyzer(CypherAST* ast, GraphView graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(graphView.metadata())
{
}

ReadStmtAnalyzer::~ReadStmtAnalyzer() {
}

void ReadStmtAnalyzer::analyze(const Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::MATCH:
            analyze(static_cast<const MatchStmt*>(stmt));
            break;

        default:
            throwError("Unsupported read statement type", stmt);
            break;
    }
}

void ReadStmtAnalyzer::analyze(const MatchStmt* matchSt) {
    if (matchSt->isOptional()) {
        throwError("OPTIONAL MATCH not supported", matchSt);
    }

    const Pattern* pattern = matchSt->getPattern();
    if (!pattern) {
        throwError("MATCH statement must have a pattern", matchSt);
    }

    analyze(pattern);

    if (matchSt->hasOrderBy()) {
        analyze(matchSt->getOrderBy());
    }

    if (matchSt->hasSkip()) {
        analyze(matchSt->getSkip());
    }

    if (matchSt->hasLimit()) {
        analyze(matchSt->getLimit());
    }
}

void ReadStmtAnalyzer::analyze(OrderBy* orderBySt) {
    for (OrderByItem* item : orderBySt->getItems()) {
        _exprAnalyzer->analyze(item->getExpr());
    }
}

void ReadStmtAnalyzer::analyze(Skip* skip) {
    Expr* expr = skip->getExpr();
    _exprAnalyzer->analyze(expr);

    if (expr->getType() != EvaluatedType::Integer) {
        throwError("SKIP expression must be an integer", skip);
    }
}

void ReadStmtAnalyzer::analyze(Limit* limit) {
    Expr* expr = limit->getExpr();
    _exprAnalyzer->analyze(expr);

    if (expr->getType() != EvaluatedType::Integer) {
        throwError("LIMIT expression must be an integer", limit);
    }
}

void ReadStmtAnalyzer::analyze(const Pattern* pattern) {
    for (const PatternElement* element : pattern->elements()) {
        analyze(element);
    }

    if (const WhereClause* where = pattern->getWhere()) {
        Expr* whereExpr = where->getExpr();
        _exprAnalyzer->analyze(whereExpr);

        if (whereExpr->getType() != EvaluatedType::Bool) {
            throwError("WHERE expression must be a boolean", pattern);
        }
    }
}

void ReadStmtAnalyzer::analyze(const PatternElement* element) {
    const auto& entities = element->getEntities();

    for (EntityPattern* entity : entities) {
        if (NodePattern* node = dynamic_cast<NodePattern*>(entity)) {
            analyze(node);
        } else if (EdgePattern* edge = dynamic_cast<EdgePattern*>(entity)) {
            analyze(edge);
        } else {
            throwError("Unsupported pattern entity type", entity);
        }
    }
}

void ReadStmtAnalyzer::analyze(NodePattern* nodePattern) {
    if (Symbol* symbol = nodePattern->getSymbol()) {
        VarDecl* decl = _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::NodePattern, symbol->getName());
        nodePattern->setDecl(decl);
    } else {
        VarDecl* decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::NodePattern);
        nodePattern->setDecl(decl);
    }

    NodePatternData* data = NodePatternData::create(_ast);
    nodePattern->setData(data);

    const auto& labels = nodePattern->labels();
    if (!labels.empty()) {
        const LabelMap& labelMap = _graphMetadata.labels();

        for (const Symbol* label : labels) {
            const std::optional<LabelID> labelID = labelMap.get(label->getName());
            if (!labelID) {
                throwError(fmt::format("Unknown label: {}", label->getName()), nodePattern);
            }

            data->addLabelConstraint(label->getName());
        }
    }

    const MapLiteral* properties = nodePattern->getProperties();
    if (properties) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : *properties) {
            _exprAnalyzer->analyze(expr);

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (!propType) {
                throwError(fmt::format("Unknown property: {}", propName->getName()), nodePattern);
            }

            if (!ExprAnalyzer::propTypeCompatible(propType->_valueType, expr->getType())) {
                throwError(fmt::format("Cannot evaluate node property: types '{}' and '{}' are incompatible",
                                       ValueTypeName::value(propType->_valueType),
                                       EvaluatedTypeName::value(expr->getType())),
                           nodePattern);
            }

            data->addExprConstraint(propName->getName(), propType->_valueType, expr);
        }
    }
}

void ReadStmtAnalyzer::analyze(EdgePattern* edgePattern) {
    if (Symbol* symbol = edgePattern->getSymbol()) {
        VarDecl* decl = _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::EdgePattern, symbol->getName());
        edgePattern->setDecl(decl);
    } else {
        VarDecl* decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::EdgePattern);
        edgePattern->setDecl(decl);
    }

    EdgePatternData* data = EdgePatternData::create(_ast);
    edgePattern->setData(data);

    const auto& types = edgePattern->types();
    if (!types.empty()) {
        const EdgeTypeMap& edgeTypeMap = _graphMetadata.edgeTypes();

        for (const Symbol* edgeTypeSymbol : types) {
            const std::optional<EdgeTypeID> etID = edgeTypeMap.get(edgeTypeSymbol->getName());
            if (!etID) {
                throwError(fmt::format("Unknown edge type: {}", edgeTypeSymbol->getName()), edgePattern);
            }

            data->addEdgeTypeConstraint(edgeTypeSymbol->getName());
        }
    }

    const MapLiteral* properties = edgePattern->getProperties();
    if (properties) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : *properties) {
            _exprAnalyzer->analyze(expr);

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (!propType) {
                throwError(fmt::format("Unknown property: {}", propName->getName()), edgePattern);
            }

            if (!ExprAnalyzer::propTypeCompatible(propType->_valueType, expr->getType())) {
                throwError(fmt::format("Cannot evaluate edge property: types '{}' and '{}' are incompatible",
                                       ValueTypeName::value(propType->_valueType),
                                       EvaluatedTypeName::value(expr->getType())),
                           edgePattern);
            }

            data->addExprConstraint(propName->getName(), propType->_valueType, expr);
        }
    }
}

void ReadStmtAnalyzer::throwError(std::string_view msg, const void* obj) const {
    throw AnalyzeException(_ast->getDiagnosticsManager()->createErrorString(msg, obj));
}
