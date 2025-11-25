#include "ReadStmtAnalyzer.h"

#include "AnalyzeException.h"
#include "ExprAnalyzer.h"
#include "FunctionInvocation.h"
#include "SymbolChain.h"
#include "YieldClause.h"
#include "YieldItems.h"
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
#include "stmt/CallStmt.h"
#include "QualifiedName.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Symbol.h"
#include "Literal.h"
#include "WhereClause.h"

#include "expr/Expr.h"
#include "expr/BinaryExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/PathExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/StringExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"
#include "expr/FunctionInvocationExpr.h"

#include "BioAssert.h"

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

        case Stmt::Kind::CALL:
            analyze(static_cast<const CallStmt*>(stmt));
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

void ReadStmtAnalyzer::analyze(const CallStmt* callStmt) {
    // Step 1. Check OPTIONAL keyword
    if (callStmt->isOptional()) {
        throwError("OPTIONAL CALL not supported", callStmt);
    }

    // Step 2. Analyze function invocation
    FunctionInvocationExpr* funcExpr = callStmt->getFunc();
    if (!funcExpr) [[unlikely]] {
        throwError("CALL statement must have a function invocation", callStmt);
    }

    _exprAnalyzer->analyzeFuncInvocExpr(funcExpr);

    const FunctionInvocation* func = funcExpr->getFunctionInvocation();
    const FunctionSignature* signature = func->getSignature();

    if (!signature->_isDatabaseProcedure) {
        throwError(fmt::format("Function '{} is not a database procedure'", signature->_fullName), callStmt);
    }

    // Step 3. Analyze YIELD clause
    const YieldClause* yield = callStmt->getYield();
    analyze(*func, yield);
}

void ReadStmtAnalyzer::analyze(const FunctionInvocation& func, const YieldClause* yield) {
    // Step 1. Check if `YIELD *` (in this case, yield == nullptr)
    if (yield == nullptr) {
        return;
    }

    bioassert(func.getSignature() && "Analyzed a yield function that has no signature");
    bioassert(yield->getItems() && "Analyzed a yield function that has no yield items");

    FunctionSignature* signature = func.getSignature();
    YieldItems* yieldItems = yield->getItems();

    // Step 2. Create the decls for the yield items
    for (SymbolExpr* yieldItemExpr : *yieldItems) {
        Symbol* yieldItem = yieldItemExpr->getSymbol();

        if (_ctxt->hasDecl(yieldItem->getName())) {
            throwError(fmt::format("Variable '{}' already declared", yieldItem->getName()), yieldItemExpr);
        }

        VarDecl* decl = nullptr;

        // Step 3. Find the item in the return values of the function
        for (const FunctionReturnType& returnItem : signature->_returnTypes) {
            if (returnItem._name == yieldItem->getName()) {
                bioassert(!returnItem._name.empty() && "Procedure return item has empty name");
                decl = _ctxt->getOrCreateNamedVariable(_ast, returnItem._type, yieldItem->getName());
                break;
            }
        }

        if (decl == nullptr) {
            throwError(fmt::format("Procedure '{}' does not return item '{}'", signature->_fullName, yieldItem->getName()), yieldItemExpr);
        }

        yieldItemExpr->setExprVarDecl(decl);
    }
}

void ReadStmtAnalyzer::analyze(OrderBy* orderBySt) {
    for (OrderByItem* item : orderBySt->getItems()) {
        Expr* expr = item->getExpr();
        _exprAnalyzer->analyzeRootExpr(expr);

        if (expr->isAggregate()) {
            throwError("Invalid use of aggregate expression in this context", orderBySt);
        }
    }
}

void ReadStmtAnalyzer::analyze(Skip* skip) {
    Expr* expr = skip->getExpr();
    _exprAnalyzer->analyzeRootExpr(expr);

    if (expr->isDynamic()) {
        throwError("SKIP expression must be a value that can be evaluated at compile time", skip);
    }

    if (expr->isAggregate()) {
        throwError("Invalid use of aggregate expression in this context", skip);
    }

    if (expr->getType() != EvaluatedType::Integer) {
        throwError("SKIP expression must be an integer", skip);
    }
}

void ReadStmtAnalyzer::analyze(Limit* limit) {
    Expr* expr = limit->getExpr();
    _exprAnalyzer->analyzeRootExpr(expr);

    if (expr->isDynamic()) {
        throwError("LIMIT expression must be a value that can be evaluated at compile time", limit);
    }

    if (expr->isAggregate()) {
        throwError("Invalid use of aggregate expression in this context", limit);
    }

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
        _exprAnalyzer->analyzeRootExpr(whereExpr);

        if (whereExpr->isAggregate()) {
            throwError("Invalid use of aggregate expression in this context", pattern);
        }

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
    VarDecl* decl = nullptr;

    if (Symbol* symbol = nodePattern->getSymbol()) {
        decl = _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::NodePattern, symbol->getName());
        nodePattern->setDecl(decl);
    } else {
        decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::NodePattern);
        nodePattern->setDecl(decl);
    }

    NodePatternData* data = NodePatternData::create(_ast);
    nodePattern->setData(data);

    const auto& labels = nodePattern->labels();
    if (labels && !labels->empty()) {
        const LabelMap& labelMap = _graphMetadata.labels();

        for (const Symbol* label : *labels) {
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
            _exprAnalyzer->analyzeRootExpr(expr);

            if (expr->isAggregate()) {
                throwError("Invalid use of aggregate expression in this context", nodePattern);
            }

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

            // Create a dummy Expr that represents the predicate evaluation
            Symbol* varSymbol = Symbol::create(_ast, decl->getName());
            QualifiedName* fullName = QualifiedName::create(_ast);

            fullName->addName(varSymbol);
            fullName->addName(propName);

            PropertyExpr* propExpr = PropertyExpr::create(_ast, fullName);
            BinaryExpr* predExpr = BinaryExpr::create(_ast, BinaryOperator::Equal, propExpr, expr);
            _exprAnalyzer->analyzeRootExpr(predExpr);

            data->addExprConstraint(propName->getName(), propType->_valueType, predExpr);
        }
    }
}

void ReadStmtAnalyzer::analyze(EdgePattern* edgePattern) {
    VarDecl* decl = nullptr;

    if (Symbol* symbol = edgePattern->getSymbol()) {
        decl = _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::EdgePattern, symbol->getName());
        edgePattern->setDecl(decl);
    } else {
        decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::EdgePattern);
        edgePattern->setDecl(decl);
    }

    EdgePatternData* data = EdgePatternData::create(_ast);
    edgePattern->setData(data);

    const auto& types = edgePattern->types();
    if (types && !types->empty()) {
        const EdgeTypeMap& edgeTypeMap = _graphMetadata.edgeTypes();

        for (const Symbol* edgeTypeSymbol : *types) {
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
            _exprAnalyzer->analyzeRootExpr(expr);

            if (expr->isAggregate()) {
                throwError("Invalid use of aggregate expression in this context", edgePattern);
            }

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

            // Create a dummy Expr that represents the predicate evaluation
            Symbol* varSymbol = Symbol::create(_ast, decl->getName());
            QualifiedName* fullName = QualifiedName::create(_ast);

            fullName->addName(varSymbol);
            fullName->addName(propName);

            PropertyExpr* propExpr = PropertyExpr::create(_ast, fullName);
            BinaryExpr* predExpr = BinaryExpr::create(_ast, BinaryOperator::Equal, propExpr, expr);
            _exprAnalyzer->analyzeRootExpr(predExpr);

            data->addExprConstraint(propName->getName(), propType->_valueType, predExpr);
        }
    }
}

void ReadStmtAnalyzer::throwError(std::string_view msg, const void* obj) const {
    throw AnalyzeException(_ast->getDiagnosticsManager()->createErrorString(msg, obj));
}
