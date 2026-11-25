#include "CypherAnalyzer.h"

#include <algorithm>
#include <utility>
#include <string_view>
#include <vector>

#include "EntityPattern.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Symbol.h"
#include "ExprAnalyzer.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "UnionQuery.h"
#include "ConditionalQuery.h"
#include "Projection.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "expr/Expr.h"
#include "decl/VarDecl.h"
#include "expr/SymbolExpr.h"
#include "stmt/StmtContainer.h"
#include "stmt/ReturnStmt.h"
#include "stmt/WithStmt.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/NextSegmentStmt.h"

#include "expr/ExistsExpr.h"
#include "expr/CountSubqueryExpr.h"
#include "expr/CollectSubqueryExpr.h"

#include "BioAssert.h"

using namespace db;

void CypherAnalyzer::analyze(CallSubqueryStmt* subquery) {
    if (subquery->importsEverything()) {
        importEveryVariable(subquery);
    }

    const BranchImports mode = subquery->hasScopeClause() ? BranchImports::SCOPE_CLAUSE : BranchImports::LEADING_WITH;

    std::vector<DeclContext*> returnScopes;
    analyzeSubqueryBody(subquery->getBody(), subquery->getImports(), mode, returnScopes);

    if (subquery->isReturning()) {
        publishSubqueryReturn(subquery);
    }
}

void CypherAnalyzer::analyze(NextSegmentStmt* segment) {
    CallSubqueryStmt* call = segment->getBody();
    RegularQuery* body = call->getBody();

    importEveryVariable(call);

    std::vector<DeclContext*> returnScopes;
    analyzeSubqueryBody(body, call->getImports(), BranchImports::NEXT_SEGMENT, returnScopes);

    DeclContext* scope = DeclContext::create(_ast, _ctxt);

    if (body->isReturning()) {
        publishBranchReturns(body, scope);
    }

    std::vector<const VarDecl*> redeclared;
    redeclareDroppedImports(scope, redeclared);

    for (const VarDecl* decl : redeclared) {
        segment->addRedeclaredImport(decl);
    }

    setScope(scope);
}

void CypherAnalyzer::analyzeSubqueryBody(RegularQuery* body,
                                         std::span<const Symbol* const> imports,
                                         BranchImports mode,
                                         std::vector<DeclContext*>& returnScopes) {
    const QueryCommand::Kind kind = body->getKind();

    if (kind == QueryCommand::Kind::SINGLE_PART_QUERY) {
        SinglePartQuery* query = static_cast<SinglePartQuery*>(body);

        if (mode == BranchImports::LEADING_WITH) {
            CallSubqueryStmt::Imports leadingImports;
            importThroughLeadingWith(query, leadingImports);

            returnScopes.push_back(analyzeSubqueryBranch(query, leadingImports, mode));
        } else {
            returnScopes.push_back(analyzeSubqueryBranch(query, imports, mode));
        }
    } else if (kind == QueryCommand::Kind::UNION_QUERY) {
        const UnionQuery::Branches& branches = static_cast<const UnionQuery*>(body)->branches();

        for (const UnionQuery::Branch& branch : branches) {
            analyzeSubqueryBody(branch._query, imports, mode, returnScopes);
        }

        for (size_t index = 1; index < branches.size(); index++) {
            analyzeUnionColumns(branches.front()._query, branches[index]._query);
        }
    } else {
        ConditionalQuery* conditional = static_cast<ConditionalQuery*>(body);
        const ConditionalQuery::Branches& branches = conditional->branches();

        analyzeConditionalPredicates(conditional, imports, mode);

        for (const ConditionalQuery::Branch& branch : branches) {
            analyzeSubqueryBody(branch._query, imports, mode, returnScopes);
        }

        for (size_t index = 1; index < branches.size(); index++) {
            analyzeConditionalColumns(branches.front()._query, branches[index]._query);
        }
    }
}

DeclContext* CypherAnalyzer::analyzeSubqueryBranch(SinglePartQuery* query,
                                                   std::span<const Symbol* const> imports,
                                                   BranchImports mode) {
    // The body reads the imported variables and nothing else of the scope around it, so
    // its context is seeded with a declaration per import and the body resolves in that
    DeclContext* const outer = _ctxt;
    DeclContext* const inner = query->getDeclContext();

    importIntoQuery(query, imports, outer, inner);

    // What the scope clause names is readable everywhere in the body. A body importing
    // through a leading WITH carries nothing this way: that WITH is an ordinary
    // projection, and an ordinary WITH below it descopes what it does not project. Nor
    // does a segment, whose WITH reads the whole table and groups it on its own keys.
    const bool keepsEnclosingImports = mode == BranchImports::NEXT_SEGMENT;

    std::vector<VarDecl*> outerImports;
    if (mode == BranchImports::SCOPE_CLAUSE) {
        for (const Symbol* import : imports) {
            outerImports.push_back(inner->getDecl(import->getName()));
        }
    }

    if (!keepsEnclosingImports) {
        std::swap(_subqueryImports, outerImports);
    }

    setScope(inner);

    analyze(query);

    if (!keepsEnclosingImports) {
        std::swap(_subqueryImports, outerImports);
    }

    DeclContext* const returnScope = _ctxt;
    setScope(outer);

    return returnScope;
}

// A predicate reads the row the WHEN runs for. Under a leading WITH it reads none of it:
// that WITH runs only once the predicate has picked its branch.
void CypherAnalyzer::analyzeConditionalPredicates(ConditionalQuery* conditional,
                                                  std::span<const Symbol* const> imports,
                                                  BranchImports mode) {
    DeclContext* const outer = _ctxt;
    DeclContext* const inner = conditional->getDeclContext();

    if (mode != BranchImports::LEADING_WITH) {
        importIntoQuery(conditional, imports, outer, inner);
    }

    setScope(inner);
    analyzePredicates(conditional);
    setScope(outer);
}

void CypherAnalyzer::analyzePredicates(const ConditionalQuery* conditional) {
    for (const ConditionalQuery::Branch& branch : conditional->branches()) {
        Expr* predicate = branch._condition;
        if (!predicate) {
            continue;
        }

        _exprAnalyzer->analyzeRootExpr(predicate);

        if (predicate->isAggregate()) {
            throwError("Invalid use of aggregate expression in this context", predicate);
        }

        const EvaluatedType predicateType = predicate->getType();
        if (!namesATruthValue(predicateType)) {
            throwError("WHEN predicate must be a boolean", predicate);
        }
    }
}

void CypherAnalyzer::importIntoQuery(RegularQuery* query,
                                     std::span<const Symbol* const> imports,
                                     DeclContext* outer,
                                     DeclContext* inner) {
    query->clearImportDecls();

    for (const Symbol* import : imports) {
        const std::string_view name = import->getName();

        const VarDecl* decl = outer->getDecl(name);
        if (!decl) {
            throwError(fmt::format("Variable '{}' not found", name), import);
        }

        VarDecl* imported = inner->getOrCreateNamedVariable(_ast, decl->getType(), name);
        imported->setExprType(decl->getExprType());
        imported->setIsQuantifiedPath(decl->isQuantifiedPath());
        _declSources[imported] = decl;
        _exprAnalyzer->importCSVSource(imported, decl);

        query->addImportDecl(imported);
    }
}

void CypherAnalyzer::analyzeExistsBody(ExistsExpr* exists) {
    RegularQuery* body = exists->getBody();

    if (body->writesToTheGraph()) {
        throwError("An EXISTS subquery is read-only: its body cannot write to the graph", exists);
    }

    if (const Pattern* predicatePattern = exists->getPredicatePattern()) {
        const bool hasARelationship = predicatePattern->elements().front()->getEntities().size() > 1;
        if (!hasARelationship) {
            throwError("A pattern in an expression needs at least one relationship", exists);
        }

        throwOnPatternPredicateVariable(predicatePattern, _ctxt);
    }

    // A branch needs no RETURN here, but one that has it names the union's columns
    analyzeCorrelatedBody(body, /*dedupNamesColumns=*/false);
}

void CypherAnalyzer::analyzeCountSubqueryBody(CountSubqueryExpr* count) {
    RegularQuery* body = count->getBody();

    if (body->writesToTheGraph()) {
        throwError("A COUNT subquery is read-only: its body cannot write to the graph", count);
    }

    // A UNION dedups the rows it counts on the columns they return, so it needs them named
    analyzeCorrelatedBody(body, /*dedupNamesColumns=*/true);
}

void CypherAnalyzer::analyzeCollectSubqueryBody(CollectSubqueryExpr* collect) {
    RegularQuery* body = collect->getBody();

    if (body->writesToTheGraph()) {
        throwError("A COLLECT subquery is read-only: its body cannot write to the graph", collect);
    }

    std::vector<const SinglePartQuery*> queries;
    RegularQuery::collectSingleQueries(body, queries);

    for (const SinglePartQuery* query : queries) {
        const ReturnStmt* returnStmt = query->getReturnStmt();
        if (!returnStmt) {
            throwError("A COLLECT subquery must end with a RETURN clause", query);
        }

        const size_t columnCount = returnStmt->getProjection()->items().size();
        if (columnCount != 1) {
            throwError(fmt::format("A COLLECT subquery must return exactly one column, not {}", columnCount),
                       query);
        }
    }

    analyzeCorrelatedBody(body, /*dedupNamesColumns=*/true);
}

void CypherAnalyzer::analyzeCorrelatedBody(RegularQuery* body, bool dedupNamesColumns) {
    const QueryCommand::Kind kind = body->getKind();

    if (kind == QueryCommand::Kind::SINGLE_PART_QUERY) {
        analyzeExistsBranch(static_cast<const SinglePartQuery*>(body));
    } else if (kind == QueryCommand::Kind::UNION_QUERY) {
        const UnionQuery* unionQuery = static_cast<const UnionQuery*>(body);
        const UnionQuery::Branches& branches = unionQuery->branches();

        for (const UnionQuery::Branch& branch : branches) {
            analyzeCorrelatedBody(branch._query, dedupNamesColumns);
        }

        const auto hasReturn = [](const UnionQuery::Branch& branch) {
            return branch._query->isReturning();
        };

        const bool dedupNames = dedupNamesColumns && unionQuery->getDedupedBranchCount() > 0;
        const bool namesColumns = dedupNames || std::ranges::any_of(branches, hasReturn);
        if (namesColumns) {
            for (size_t index = 1; index < branches.size(); index++) {
                analyzeUnionColumns(branches.front()._query, branches[index]._query);
            }
        }
    } else {
        ConditionalQuery* conditional = static_cast<ConditionalQuery*>(body);
        const ConditionalQuery::Branches& branches = conditional->branches();

        analyzeCorrelatedPredicates(conditional);

        for (const ConditionalQuery::Branch& branch : branches) {
            analyzeCorrelatedBody(branch._query, dedupNamesColumns);
        }

        for (size_t index = 1; index < branches.size(); index++) {
            analyzeConditionalColumns(branches.front()._query, branches[index]._query);
        }
    }
}

void CypherAnalyzer::analyzeCorrelatedPredicates(ConditionalQuery* conditional) {
    if (!_analyzedBodies.insert(conditional).second) {
        return;
    }

    DeclContext* const outer = _ctxt;
    DeclContext* const inner = conditional->getDeclContext();

    std::vector<VarDecl*> correlated;
    importCorrelatedVariables(inner, correlated);

    setScope(inner);
    analyzePredicates(conditional);
    setScope(outer);
}

void CypherAnalyzer::analyzeExistsBranch(const SinglePartQuery* body) {
    if (!_analyzedBodies.insert(body).second) {
        return;
    }

    DeclContext* const outer = _ctxt;
    DeclContext* const inner = body->getDeclContext();

    // Each variable in flight stays readable through the whole body, as what a CALL's scope
    // clause names does: a WITH inside it carries them past the barrier rather than descoping
    // them, which is what keeps a pattern below that WITH reading the row the EXISTS answers for
    std::vector<VarDecl*> correlated;
    importCorrelatedVariables(inner, correlated);

    const Projection* const orderedProjection = std::exchange(_orderedProjection, nullptr);
    DeclSet consumedVariables;

    std::swap(_subqueryImports, correlated);
    std::swap(_consumedVariables, consumedVariables);

    setScope(inner);

    analyzeQueryBody(body, /*returnRequired=*/false);

    std::swap(_subqueryImports, correlated);
    std::swap(_consumedVariables, consumedVariables);
    _orderedProjection = orderedProjection;

    // A RETURN answers for no column outside the body, but the code generator reads the
    // declarations its items publish, so they are declared in a scope nothing else holds
    if (const ReturnStmt* returnStmt = body->getReturnStmt()) {
        publishProjection(returnStmt->getProjection(), DeclContext::create(_ast, inner));
    }

    setScope(outer);
}

// EXISTS is correlated: the body reads every variable in flight, through declarations of
// its own, so the variables it binds stay inside it
void CypherAnalyzer::importCorrelatedVariables(DeclContext* inner, std::vector<VarDecl*>& correlated) {
    DeclContext* const outer = _ctxt;

    const auto importDecl = [this, inner, &correlated](std::string_view name, const VarDecl* decl) {
        VarDecl* imported = inner->getOrCreateNamedVariable(_ast, decl->getType(), name);
        imported->setExprType(decl->getExprType());
        imported->setIsQuantifiedPath(decl->isQuantifiedPath());
        imported->setImportSource(decl);
        _declSources[imported] = decl;
        _exprAnalyzer->importCSVSource(imported, decl);

        correlated.push_back(imported);
    };

    for (const VarDecl* decl : outer->decls()) {
        if (decl->isUnnamed() || _consumedVariables.contains(decl)) {
            continue;
        }

        const std::string_view name = decl->getName();

        // An alias spelling a variable's name shadows the variable once the items are analyzed
        const bool shadowed = outer->getDecl(name) != decl;

        if (shadowed) {
            continue;
        }

        importDecl(name, decl);
    }

    // So is what the scope reads through to the ones around it - what a WITH dropped, for
    // its WHERE - which the import sink records for the WITH to carry to the filter
    std::vector<const VarDecl*>* const importSink = _exprAnalyzer->getImportSink();

    const DeclContext* scope = outer;
    while (scope->readsEnclosingScope() && scope->getParent()) {
        scope = scope->getParent();

        for (const VarDecl* decl : scope->decls()) {
            const std::string_view name = decl->getName();

            const bool boundToName = !decl->isUnnamed() && scope->getDecl(name) == decl;
            const bool shadowed = inner->hasDecl(name);
            if (!boundToName || shadowed) {
                continue;
            }

            importDecl(name, decl);

            if (importSink && std::ranges::find(*importSink, decl) == importSink->end()) {
                importSink->push_back(decl);
            }
        }
    }

    if (_orderedProjection) {
        for (const Projection::ReturnItem& item : _orderedProjection->items()) {
            const auto* exprPtr = std::get_if<Expr*>(&item);
            if (!exprPtr) {
                continue;
            }

            const Expr* itemExpr = *exprPtr;
            const std::string_view alias = itemExpr->getName();
            const bool renamesAVariable = itemExpr->getKind() == Expr::Kind::SYMBOL && !alias.empty();

            if (renamesAVariable && !inner->hasDecl(alias)) {
                importDecl(alias, itemExpr->getExprVarDecl());
            }
        }
    }
}

void CypherAnalyzer::throwOnPatternPredicateVariable(const Pattern* pattern, const DeclContext* outer) const {
    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            const Symbol* symbol = entity->getSymbol();

            if (symbol && !outer->lookup(symbol->getName())) {
                throwError(fmt::format("A pattern predicate cannot introduce new variables: '{}'",
                                       symbol->getName()),
                           entity);
            }
        }
    }
}

void CypherAnalyzer::importEveryVariable(CallSubqueryStmt* subquery) const {
    for (const VarDecl* decl : _ctxt->decls()) {
        if (decl->isUnnamed()) {
            continue;
        }

        const std::string_view name = decl->getName();
        const bool shadowed = _ctxt->getDecl(name) != decl;

        if (shadowed) {
            continue;
        }

        subquery->addImport(Symbol::create(_ast, name));
    }
}

void CypherAnalyzer::importThroughLeadingWith(const SinglePartQuery* query, CallSubqueryStmt::Imports& imports) const {
    const StmtContainer* stmts = query->getStmts();
    if (!stmts || stmts->stmts().empty()) {
        return;
    }

    const Stmt* first = stmts->stmts().front();
    if (first->getKind() != Stmt::Kind::WITH) {
        return;
    }

    const WithStmt* with = static_cast<const WithStmt*>(first);
    if (with->isSegmentReturn()) {
        return;
    }

    const Projection* projection = with->getProjection();

    const bool plainProjection = !with->getWhere()
                                 && !projection->isDistinct()
                                 && !projection->hasOrderBy()
                                 && !projection->hasSkip()
                                 && !projection->hasLimit();

    if (projection->isReturningAll()) {
        throwError("An importing WITH holds plain variable references only: "
                   "WITH * names none of them",
                   with);
    } else if (!plainProjection) {
        throwError("An importing WITH holds plain variable references only, "
                   "with no WHERE, DISTINCT, ORDER BY, SKIP or LIMIT",
                   with);
    }

    for (const Projection::ReturnItem& returnItem : projection->items()) {
        const auto* exprPtr = std::get_if<Expr*>(&returnItem);
        const Expr* item = exprPtr ? *exprPtr : nullptr;

        const bool plainSymbol = item
                                 && item->getKind() == Expr::Kind::SYMBOL
                                 && item->getName().empty();

        if (!plainSymbol) {
            throwError("An importing WITH holds plain variable references only: "
                       "an expression or an alias cannot be imported",
                       with);
        }

        imports.push_back(static_cast<const SymbolExpr*>(item)->getSymbol());
    }
}

// A WITH inside a subquery body publishes the imports beside its own items, so a clause
// below it reads them as the clauses above it did. They ride the barrier as the items do:
// a cut keeps them with the rows it keeps, and a dedup reads them with the rest of the row.
bool CypherAnalyzer::carrySubqueryImports(Projection* projection,
                                          bool isAggregate,
                                          bool hasGroupingKeys) const {
    // A reduction over no grouping key answers for the rows it read with a single row,
    // even when it read none. Carrying an import would key it, and a keyed reduction over
    // no row has no group to report, so the import stops at such a barrier.
    const bool keylessAggregate = isAggregate && !hasGroupingKeys;
    if (keylessAggregate) {
        return false;
    }

    bool carried = false;

    for (const VarDecl* importDecl : _subqueryImports) {
        const std::string_view import = importDecl->getName();
        VarDecl* decl = _ctxt->getDecl(import);
        if (!decl) {
            continue;
        }

        if (projection->hasName(import)) {
            throwOnRedeclaredImport(projection, import, decl);
            continue;
        }

        projection->pushFrontDecl(decl);
        projection->setName(decl, import);

        carried = true;
    }

    return carried;
}

void CypherAnalyzer::redeclareDroppedImports(DeclContext* scope, std::vector<const VarDecl*>& redeclared) const {
    for (VarDecl* importDecl : _subqueryImports) {
        if (!scope->getDecl(importDecl->getName())) {
            scope->redeclare(importDecl);
            redeclared.push_back(importDecl);
        }
    }
}

void CypherAnalyzer::throwOnRedeclaredImport(const Projection* projection,
                                             std::string_view import,
                                             const VarDecl* decl) const {
    for (const Projection::ReturnItem& returnItem : projection->items()) {
        if (const auto* declPtr = std::get_if<VarDecl*>(&returnItem)) {
            if (projection->getName(*declPtr) != import) {
                continue;
            }

            // The variable itself, projected under its own name: the barrier publishes
            // what it already held rather than binding the name to something else
            if (*declPtr == decl) {
                return;
            }
        } else {
            const Expr* item = std::get<Expr*>(returnItem);
            if (projection->getName(item) != import) {
                continue;
            }

            const bool namesTheImport = item->getKind() == Expr::Kind::SYMBOL
                                        && static_cast<const SymbolExpr*>(item)->getDecl() == decl;
            if (namesTheImport) {
                return;
            }
        }

        throwError(fmt::format("Variable '{}' is imported by the subquery: a clause of the subquery "
                               "cannot declare it again",
                               import),
                   projection);
    }
}

void CypherAnalyzer::publishSubqueryReturn(const CallSubqueryStmt* subquery) {
    RegularQuery* body = subquery->getBody();
    const ReturnStmt* returnStmt = body->getFirstSingleQuery()->getReturnStmt();
    Projection* projection = returnStmt->getProjection();

    for (const Projection::ReturnItem& returnItem : projection->items()) {
        std::optional<std::string_view> name;
        if (const auto* declPtr = std::get_if<VarDecl*>(&returnItem)) {
            name = projection->getName(*declPtr);
        } else {
            name = projection->getName(std::get<Expr*>(returnItem));
        }

        bioassert(name.has_value(), "Returned item of a CALL subquery without a name.");

        if (_ctxt->hasDecl(*name)) {
            throwError(fmt::format("Variable '{}' already declared in the outer scope: "
                                   "a CALL subquery cannot return it, rename it with AS",
                                   *name),
                       returnStmt);
        }
    }

    publishBranchReturns(body, _ctxt);
}

void CypherAnalyzer::publishBranchReturns(RegularQuery* body, DeclContext* scope) {
    std::vector<SinglePartQuery*> branches;
    RegularQuery::collectSingleQueries(body, branches);

    Projection* projection = branches.front()->getReturnStmt()->getProjection();

    publishProjection(projection, scope);

    for (size_t branchIndex = 1; branchIndex < branches.size(); branchIndex++) {
        Projection* branchProjection = branches[branchIndex]->getReturnStmt()->getProjection();
        publishUnionBranchReturn(branchProjection, projection);
    }
}

// Every branch of a union publishes its columns under the declarations the first one
// made, typed by every branch: a null says nothing about what the column holds, and
// branches of different types leave each row its own, as the branches of a CASE do.
void CypherAnalyzer::publishUnionBranchReturn(Projection* branchProjection, const Projection* firstProjection) {
    const Projection::PublishedDecls& published = firstProjection->publishedDecls();

    size_t index = 0;
    for (const Projection::ReturnItem& returnItem : branchProjection->items()) {
        VarDecl* decl = published[index];

        const ExprType* itemType = nullptr;
        EvaluatedType itemCommonType = EvaluatedType::Invalid;

        if (const auto* declPtr = std::get_if<VarDecl*>(&returnItem)) {
            itemType = &(*declPtr)->getExprType();
            itemCommonType = (*declPtr)->getCommonType();
        } else {
            const Expr* item = std::get<Expr*>(returnItem);
            itemType = &item->getExprType();
            itemCommonType = item->getCommonType();
        }

        const EvaluatedType columnType = decl->getType();

        const EvaluatedType selected = selectedType(columnType, itemType->_type);
        ExprType column {selected};

        if (selected == EvaluatedType::Invalid) {
            throwError(fmt::format("Column '{}' cannot hold both '{}' and '{}' values",
                                   decl->getName(),
                                   EvaluatedTypeName::value(columnType),
                                   EvaluatedTypeName::value(itemType->_type)),
                       branchProjection);
        }

        const ListShape columnShape = ListShape::selected(columnType, decl->getListShape());
        const ListShape itemListShape = ListShape::selected(itemType->_type, itemType->_shape);

        column._shape = selected == EvaluatedType::List ? ListShape::unified(columnShape, itemListShape) : ListShape();
        column._commonType = commonType(decl->getCommonType(), itemCommonType);
        decl->setExprType(column);

        branchProjection->addPublishedDecl(decl);
        index++;
    }
}
