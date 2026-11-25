#include "CypherAnalyzer.h"

#include <algorithm>
#include <utility>
#include <string_view>
#include <vector>

#include "CypherAST.h"
#include "Symbol.h"
#include "SourceManager.h"
#include "ExprAnalyzer.h"
#include "Projection.h"
#include "WhereClause.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "expr/Expr.h"
#include "decl/VarDecl.h"
#include "expr/SymbolExpr.h"
#include "stmt/ReturnStmt.h"
#include "stmt/WithStmt.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/Skip.h"
#include "stmt/Limit.h"

#include "BioAssert.h"

using namespace db;

void CypherAnalyzer::analyze(const ReturnStmt* returnSt) {
    analyzeProjection(returnSt->getProjection(), returnSt);
}

void CypherAnalyzer::analyze(WithStmt* withSt) {
    Projection* projection = withSt->getProjection();

    analyzeWithAliases(projection);
    analyzeProjection(projection, withSt);

    openWithScope(withSt);

    const WhereClause* where = withSt->getWhere();
    if (!where) {
        return;
    }

    // An aggregate or a dedup leaves no row of the scope before it for the filter to read
    const bool readsDroppedVariables = !projection->isAggregate() && !projection->isDistinct();
    _ctxt->setReadsEnclosingScope(readsDroppedVariables);

    std::vector<const VarDecl*> filterImports;
    _exprAnalyzer->setImportSink(&filterImports);

    Expr* predicate = where->getExpr();
    _exprAnalyzer->analyzeRootExpr(predicate);

    _exprAnalyzer->setImportSink(nullptr);
    _ctxt->setReadsEnclosingScope(false);

    for (const VarDecl* decl : filterImports) {
        withSt->addFilterImport(decl);
    }

    if (predicate->isAggregate()) {
        throwError("Invalid use of aggregate expression in this context", predicate);
    }

    const EvaluatedType predicateType = predicate->getType();
    if (!namesATruthValue(predicateType)) {
        throwError("WHERE expression must be a boolean", predicate);
    }
}

// The names a WITH gives its columns are the whole scope of what follows, so a statement
// after the barrier resolves its items and nothing the projection dropped
void CypherAnalyzer::openWithScope(WithStmt* withSt) {
    DeclContext* scope = DeclContext::create(_ast, _ctxt);

    publishProjection(withSt->getProjection(), scope);

    std::vector<const VarDecl*> redeclared;
    redeclareDroppedImports(scope, redeclared);

    for (const VarDecl* decl : redeclared) {
        withSt->addRedeclaredImport(decl);
    }

    setScope(scope);
}

void CypherAnalyzer::publishProjection(Projection* projection, DeclContext* scope) {
    for (const Projection::ReturnItem& returnItem : projection->items()) {
        if (const auto* declPtr = std::get_if<VarDecl*>(&returnItem)) {
            const VarDecl* decl = *declPtr;
            VarDecl* published = scope->getOrCreateNamedVariable(_ast, decl->getType(), decl->getName());
            published->setExprType(decl->getExprType());
            published->setIsQuantifiedPath(decl->isQuantifiedPath());
            projection->addPublishedDecl(published);

            if (published != decl) {
                _declSources[published] = decl;
            }

            _exprAnalyzer->importCSVSource(published, decl);

            continue;
        }

        const Expr* item = std::get<Expr*>(returnItem);
        const std::optional<std::string_view> name = projection->getName(item);
        bioassert(name.has_value(), "Projected item of a WITH without a name.");

        const VarDecl* itemDecl = item->getExprVarDecl();

        VarDecl* published = scope->getOrCreateNamedVariable(_ast, item->getType(), *name);
        published->setExprType(item->getExprType());
        published->setIsQuantifiedPath(itemDecl && itemDecl->isQuantifiedPath());
        projection->addPublishedDecl(published);

        _exprAnalyzer->importCSVSource(published, itemDecl);
    }
}

// RETURN needs no such rule: it names its columns for the caller and nothing downstream
// reads them back
void CypherAnalyzer::analyzeWithAliases(const Projection* projection) const {
    for (const Projection::ReturnItem& returnItem : projection->items()) {
        const auto* exprPtr = std::get_if<Expr*>(&returnItem);
        if (!exprPtr) {
            continue;
        }

        const Expr* item = *exprPtr;
        const bool named = item->getKind() == Expr::Kind::SYMBOL || !item->getName().empty();

        if (!named) {
            throwError("Expression in WITH must be aliased with AS", item);
        }
    }
}

void CypherAnalyzer::analyzeProjection(Projection* projection, const Stmt* clause) {
    if (projection->hasSkip()) {
        analyze(projection->getSkip());
    }

    if (projection->hasLimit()) {
        analyze(projection->getLimit());
    }

    // Check if the projection contains aggregate expressions and grouping keys
    bool isAggregate = false;
    bool hasGroupingKeys = false;

    const SourceManager* srcMan = _ast->getSourceManager();

    std::vector<Expr*> aliasedItems;

    for (const Projection::ReturnItem& returnItem : projection->items()) {
        const auto* exprPtr = std::get_if<Expr*>(&returnItem);
        if (!exprPtr) {
            continue;
        }

        Expr* item = *exprPtr;
        std::string_view name;

        // The parser records an alias given via the AS keyword as the item's name
        const bool hasExplicitAlias = !item->getName().empty();

        if (hasExplicitAlias) {
            name = item->getName();
        } else if (item->getKind() == Expr::Kind::SYMBOL) {
            const SymbolExpr* symbolExpr = static_cast<const SymbolExpr*>(item);
            const Symbol* symbol = symbolExpr->getSymbol();
            name = symbol->getName();
        } else {
            const std::string_view generatedName =
                srcMan->getStringRepr(std::bit_cast<std::uintptr_t>(item));
            if (generatedName.empty()) [[unlikely]] {
                throwError("Failed to generate name for projection item", item);
            }

            item->setName(generatedName);
            name = generatedName;
        }

        _exprAnalyzer->analyzeRootExpr(item);

        bioassert(!name.empty(), "All declared variable must have a name.");

        if (projection->hasName(name)) {
            throwError(fmt::format("Return items must have unique names; "
                                   "{} was already defined.", name), item);
        }

        if (hasExplicitAlias) {
            aliasedItems.push_back(item);
        }

        projection->setName(item, name);

        isAggregate |= item->isAggregate();
        hasGroupingKeys |= !item->isAggregate();
    }

    for (Expr* item : aliasedItems) {
        declareItemAlias(item, item->getName());
    }

    if (projection->hasOrderBy()) {
        DeclSet consumedVariables;

        const bool groupsRows = isAggregate || projection->isDistinct();
        if (groupsRows && !projection->isReturningAll()) {
            for (const VarDecl* decl : _ctxt->decls()) {
                if (!projection->hasItemDecl(decl)) {
                    consumedVariables.insert(decl);
                }
            }
        }

        const Projection* const outerOrderedProjection = std::exchange(_orderedProjection, projection);
        std::swap(_consumedVariables, consumedVariables);

        analyze(projection->getOrderBy(), projection);

        std::swap(_consumedVariables, consumedVariables);
        _orderedProjection = outerOrderedProjection;

        if (!isAggregate) {
            throwOnOrderByAggregate(projection, clause);
        }
    }

    if (projection->isReturningAll()) {
        // Return all variables defined in the current query

        bioassert(_ctxt, "Query context is invalid");

        // Iterate the decls in reverse declaration order. Since we call `pushFrontDecl()`
        // The decls end up in order. e.g. MATCH (a), (b), (c) RETURN *, a.name
        // - Initial projection items: ['a.name'];
        // - After first `pushFrontDecl()`: ['c', 'a.name'];
        // - After second `pushFrontDecl()`: ['b', 'c', 'a.name'];
        // - After third `pushFrontDecl()`: ['a', 'b', 'c', 'a.name'];
        for (VarDecl* decl : std::views::reverse(_ctxt->decls())) {
            if (decl->isUnnamed()) {
                continue;
            }

            // If the projection already contains this variable's name, it means it was
            // added explicitly above. Skip: do not again again as part of the wildcard
            const std::string_view declName = decl->getName();
            if (projection->hasName(declName)) {
                continue;
            }

            // Push at the front since '*' is only allowed at the beginning of the return statement
            projection->pushFrontDecl(decl);
            projection->setName(decl, decl->getName());

            // A variable is never an aggregate, and the item loop above never saw this
            // one: the wildcard had not been expanded yet
            hasGroupingKeys = true;
        }

        if (projection->items().empty()) {
            throwError("Cannot use '*' when there are no variables in scope.", clause);
        }
    }

    if (clause->getKind() == Stmt::Kind::WITH) {
        hasGroupingKeys |= carrySubqueryImports(projection, isAggregate, hasGroupingKeys);
    }

    if (projection->isDistinct()) {
        analyzeDistinct(projection, isAggregate);
    }

    if (isAggregate) {
        projection->setAggregate();
        projection->setHasGroupingKeys(hasGroupingKeys);

        analyzeAggregateItems(projection);
        analyzeAggregateOrderBy(projection);
    }
}

void CypherAnalyzer::declareItemAlias(Expr* item, std::string_view alias) {
    // A column is resolved through the declaration of the item that produced it, and the
    // column of a bare variable is produced by the traversal that declared it: an alias on
    // it is a second name for that one variable, not a variable of its own
    if (item->getKind() != Expr::Kind::SYMBOL) {
        const EvaluatedType type = item->getType();
        const VarDecl* namedDecl = _ctxt->declareProjectedVariable(_ast, type, alias);
        item->setExprVarDecl(namedDecl);

        return;
    }

    SymbolExpr* symbolExpr = static_cast<SymbolExpr*>(item);
    VarDecl* aliasedDecl = symbolExpr->getDecl();
    const VarDecl* declared = _ctxt->getDecl(alias);

    const bool rebindsAnImport = declared
                                 && declared != aliasedDecl
                                 && std::ranges::find(_subqueryImports, alias, &VarDecl::getName) != _subqueryImports.end();

    if (rebindsAnImport) {
        throwError(fmt::format("Variable '{}' is imported by the subquery: a clause of the subquery "
                               "cannot declare it again",
                               alias),
                   item);
    }

    _ctxt->declareAlias(alias, aliasedDecl);
}

void CypherAnalyzer::analyze(OrderBy* orderBySt, const Projection* projection) {
    for (OrderByItem* item : orderBySt->getItems()) {
        Expr* expr = item->getExpr();
        _exprAnalyzer->analyzeRootExpr(expr);

        // A key naming the alias of an aggregate is a symbol, and a symbol is never itself
        // aggregate: the aggregate is the item the key names, so the key inherits it
        const Expr* namedItem = projection->findItemExpr(expr);
        const bool namesAnAggregateItem = namedItem && namedItem->isAggregate();

        if (namesAnAggregateItem) {
            expr->setAggregate();
        }
    }
}

void CypherAnalyzer::analyze(Skip* skipSt) {
    Expr* expr = skipSt->getExpr();
    _exprAnalyzer->analyzeRootExpr(expr);

    const EvaluatedType type = expr->getType();
    if (type != EvaluatedType::Integer && type != EvaluatedType::Any) {
        throwError("SKIP expression must be an integer", skipSt);
    }

    if (expr->isDynamic() || expr->isAggregate()) {
        throwError("SKIP expression must be a value that can be evaluated at compile time", skipSt);
    }
}

void CypherAnalyzer::analyze(Limit* limitSt) {
    Expr* expr = limitSt->getExpr();
    _exprAnalyzer->analyzeRootExpr(expr);

    const EvaluatedType type = expr->getType();
    if (type != EvaluatedType::Integer && type != EvaluatedType::Any) {
        throwError("LIMIT expression must be an integer", limitSt);
    }

    if (expr->isDynamic() || expr->isAggregate()) {
        throwError("LIMIT expression must be a value that can be evaluated at compile time", limitSt);
    }
}
