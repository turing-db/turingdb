#include "CypherAnalyzer.h"

#include <algorithm>
#include <utility>
#include <string_view>
#include <vector>

#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "EntityPattern.h"
#include "Literal.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "ReadStmtAnalyzer.h"
#include "Symbol.h"
#include "SourceManager.h"
#include "WriteStmtAnalyzer.h"
#include "ExprAnalyzer.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "LoadGraphQuery.h"
#include "CreateGraphQuery.h"
#include "LoadGMLQuery.h"
#include "LoadParquetQuery.h"
#include "LoadJsonlQuery.h"
#include "S3ConnectQuery.h"
#include "S3TransferQuery.h"
#include "CreateVectorIndexQuery.h"
#include "LoadVectorQuery.h"
#include "LoadEmbeddingQuery.h"
#include "InstallExtensionQuery.h"
#include "Projection.h"
#include "WhereClause.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "expr/EntityTypeExpr.h"
#include "expr/Expr.h"
#include "expr/ExprChildren.h"
#include "expr/ListComprehensionExpr.h"
#include "expr/PatternComprehensionExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/ReduceExpr.h"
#include "expr/AllReduceExpr.h"
#include "stmt/ShortestPathStmt.h"
#include "decl/VarDecl.h"
#include "expr/SymbolExpr.h"
#include "expr/FunctionInvocationExpr.h"
#include "FunctionInvocation.h"
#include "stmt/StmtContainer.h"
#include "stmt/ReturnStmt.h"
#include "stmt/WithStmt.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/Skip.h"
#include "stmt/CallStmt.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/NextSegmentStmt.h"
#include "stmt/Limit.h"
#include "CreateNodePropertyIndexQuery.h"
#include "CreateEdgePropertyIndexQuery.h"

#include "expr/ExistsExpr.h"
#include "expr/CountSubqueryExpr.h"
#include "expr/CollectSubqueryExpr.h"

#include "BioAssert.h"
#include "AnalyzeException.h"

using namespace db;

namespace {

const FunctionSignature* aggregateSignatureOf(const Expr* expr) {
    if (expr->getKind() != Expr::Kind::FUNCTION_INVOCATION) {
        return nullptr;
    }

    const FunctionInvocationExpr* call = static_cast<const FunctionInvocationExpr*>(expr);
    const FunctionInvocation* invocation = call->getFunctionInvocation();
    const FunctionSignature* signature = invocation->getSignature();

    if (!signature || !signature->isAggregate()) {
        return nullptr;
    }

    return signature;
}

// The variables @param expr binds per element for its child @param child to read: a list
// comprehension's element, and a fold's element and accumulator, which neither the list
// iterated nor the initial value sees
void collectElementDecls(const Expr* expr, const Expr* child, std::vector<const VarDecl*>& decls) {
    const Expr::Kind kind = expr->getKind();

    if (kind == Expr::Kind::LIST_COMPREHENSION) {
        const ListComprehensionExpr* comprehension = static_cast<const ListComprehensionExpr*>(expr);

        if (child != comprehension->getSource()) {
            decls.push_back(comprehension->getDecl());
        }
    } else if (kind == Expr::Kind::REDUCE) {
        const ReduceExpr* reduce = static_cast<const ReduceExpr*>(expr);

        if (child == reduce->getExpression()) {
            decls.push_back(reduce->getItemDecl());
            decls.push_back(reduce->getAccumulatorDecl());
        }
    } else if (kind == Expr::Kind::ALL_REDUCE) {
        const AllReduceExpr* allReduce = static_cast<const AllReduceExpr*>(expr);

        const bool readsTheFold = child == allReduce->getExpression() || child == allReduce->getPredicate();
        if (readsTheFold) {
            decls.push_back(allReduce->getItemDecl());
            decls.push_back(allReduce->getAccumulatorDecl());
        }
    }
}

constexpr std::string_view shortestPathErr = "Cannot return {} after SHORTESTPATH.";

}

CypherAnalyzer::CypherAnalyzer(CypherAST* ast, GraphView graphView, const ParameterMap* parameters)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(graphView.metadata()),
    _exprAnalyzer(std::make_unique<ExprAnalyzer>(_ast, _graphView, parameters)),
    _readAnalyzer(std::make_unique<ReadStmtAnalyzer>(_ast, _graphView)),
    _writeAnalyzer(std::make_unique<WriteStmtAnalyzer>(_ast, _graphView))
{
    _exprAnalyzer->setQueryAnalyzer(this);
    _exprAnalyzer->setReadAnalyzer(_readAnalyzer.get());
    _readAnalyzer->setExprAnalyzer(_exprAnalyzer.get());
    _writeAnalyzer->setExprAnalyzer(_exprAnalyzer.get());
}

CypherAnalyzer::~CypherAnalyzer() {
}

void CypherAnalyzer::analyze() {
    for (QueryCommand* query : _ast->queries()) {
        setScope(query->getDeclContext());

        switch (query->getKind()) {
            case QueryCommand::Kind::SINGLE_PART_QUERY:
            case QueryCommand::Kind::UNION_QUERY:
            case QueryCommand::Kind::CONDITIONAL_QUERY:
                analyzeTopLevelQuery(static_cast<RegularQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_GRAPH_QUERY:
                analyze(static_cast<const LoadGraphQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_GML_QUERY:
                analyze(static_cast<LoadGMLQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_PARQUET_QUERY:
                analyze(static_cast<LoadParquetQuery*>(query));
            break;

            case QueryCommand::Kind::CREATE_GRAPH_QUERY:
                analyze(static_cast<const CreateGraphQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_JSONL_QUERY:
                analyze(static_cast<LoadJsonlQuery*>(query));
            break;

            case QueryCommand::Kind::S3_CONNECT_QUERY:
                analyze(static_cast<const S3ConnectQuery*>(query));
            break;

            case QueryCommand::Kind::S3_TRANSFER_QUERY:
                analyze(static_cast<S3TransferQuery*>(query));
            break;

            case QueryCommand::Kind::CREATE_VECTOR_INDEX_QUERY:
                analyze(static_cast<const CreateVectorIndexQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_VECTOR_QUERY:
                analyze(static_cast<const LoadVectorQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_EMBEDDING_QUERY:
                analyze(static_cast<const LoadEmbeddingQuery*>(query));
            break;

            case QueryCommand::Kind::INSTALL_EXTENSION_QUERY:
                analyze(static_cast<const InstallExtensionQuery*>(query));
            break;

            case QueryCommand::Kind::CREATE_NODE_PROPERTY_INDEX_QUERY:
                analyze(static_cast<const CreateNodePropertyIndexQuery*>(query));
            break;

            case QueryCommand::Kind::CREATE_EDGE_PROPERTY_INDEX_QUERY:
                analyze(static_cast<const CreateEdgePropertyIndexQuery*>(query));
            break;

            // Nothing to analyze
            case QueryCommand::Kind::CHANGE_QUERY:
            case QueryCommand::Kind::COMMIT_QUERY:
            case QueryCommand::Kind::LIST_GRAPH_QUERY:
            case QueryCommand::Kind::LIST_AVAILABLE_GRAPHS_QUERY:
            case QueryCommand::Kind::SHOW_PROCEDURES_QUERY:
            case QueryCommand::Kind::DELETE_VECTOR_INDEX_QUERY:
            case QueryCommand::Kind::SHOW_VECTOR_INDEXES_QUERY:
            case QueryCommand::Kind::LOAD_COMMIT_QUERY:
            case QueryCommand::Kind::SHOW_EXTENSIONS_QUERY:
            case QueryCommand::Kind::DROP_INDEX_QUERY:
            case QueryCommand::Kind::MERGE_DATAPARTS_QUERY:
            break;

        }
    }
}

void CypherAnalyzer::analyze(const SinglePartQuery* query) {
    analyzeQueryBody(query, /*returnRequired=*/true);
}

// The branches of a UNION or a WHEN fill one result, whose columns the code generator
// binds to the declarations the first RETURN publishes
void CypherAnalyzer::analyzeTopLevelQuery(RegularQuery* query) {
    std::vector<DeclContext*> returnScopes;
    analyzeSubqueryBody(query, {}, BranchImports::SCOPE_CLAUSE, returnScopes);

    const bool composite = query->getKind() != QueryCommand::Kind::SINGLE_PART_QUERY;
    if (composite && query->isReturning()) {
        publishBranchReturns(query, DeclContext::create(_ast, _ctxt));
    }
}

void CypherAnalyzer::analyzeQueryBody(const SinglePartQuery* query, bool returnRequired) {
    const StmtContainer* stmts = query->getStmts();
    const ReturnStmt* returnStmt = query->getReturnStmt();

    bool returnMandatory = returnRequired;

    if (stmts) {
        throwOnReadAfterUpdate(stmts);

        const Stmt* previous = nullptr;

        for (Stmt* stmt : stmts->stmts()) {
            const Stmt::Kind kind = stmt->getKind();

            // A segment is a part of its own, so a part opens after it as after a WITH
            const bool followsASegment = previous && previous->getKind() == Stmt::Kind::NEXT_SEGMENT;
            if (followsASegment) {
                returnMandatory = returnRequired;
            }

            previous = stmt;

            if (kind == Stmt::Kind::NEXT_SEGMENT) {
                NextSegmentStmt* segment = static_cast<NextSegmentStmt*>(stmt);
                analyze(segment);

                if (Stmt::isUpdating(segment)) {
                    returnMandatory = false;
                }
            } else if (kind == Stmt::Kind::CALL_SUBQUERY) {
                CallSubqueryStmt* subquery = static_cast<CallSubqueryStmt*>(stmt);
                analyze(subquery);

                if (!subquery->isReturning()) {
                    returnMandatory = false;
                }
            } else if (Stmt::isUpdating(kind)) {
                returnMandatory = false;
                _writeAnalyzer->analyze(stmt);
            } else if (kind == Stmt::Kind::WITH) {
                // A part of its own opens here: `CREATE (n) WITH n MATCH (m)` ends on a
                // reading clause, and a query ending on one needs a RETURN
                returnMandatory = returnRequired;

                analyze(static_cast<WithStmt*>(stmt));
            } else {
                if (kind == Stmt::Kind::CALL && static_cast<const CallStmt*>(stmt)->isStandaloneCall()) {
                    returnMandatory = false;
                }

                _readAnalyzer->analyze(stmt);
            }
        }
    }

    if (!returnStmt && returnMandatory) {
        // Return statement is mandatory if there are no update statements
        throwError("Return statement is missing", query);
    }

    // Generate return statement
    if (returnStmt) {
        analyze(returnStmt);
    }

    analyzeShortestPathReturn(query);
}

void CypherAnalyzer::analyzeUnionColumns(const RegularQuery* first, const RegularQuery* branch) const {
    std::vector<std::string_view> firstNames;
    collectProjectionNames(unionBranchProjection(first), firstNames);

    std::vector<std::string_view> names;
    collectProjectionNames(unionBranchProjection(branch), names);

    throwOnDifferentColumns("All sub-queries of a UNION", firstNames, names, branch);
}

void CypherAnalyzer::analyzeConditionalColumns(const RegularQuery* first, const RegularQuery* branch) const {
    std::vector<std::string_view> firstNames;
    if (const ReturnStmt* firstReturn = first->getFirstSingleQuery()->getReturnStmt()) {
        collectProjectionNames(firstReturn->getProjection(), firstNames);
    }

    std::vector<std::string_view> names;
    if (const ReturnStmt* branchReturn = branch->getFirstSingleQuery()->getReturnStmt()) {
        collectProjectionNames(branchReturn->getProjection(), names);
    }

    throwOnDifferentColumns("All branches of a conditional query", firstNames, names, branch);
}

void CypherAnalyzer::throwOnDifferentColumns(std::string_view branches,
                                             std::span<const std::string_view> firstNames,
                                             std::span<const std::string_view> names,
                                             const RegularQuery* branch) const {
    if (names.size() != firstNames.size()) {
        throwError(fmt::format("{} must return the same number of columns: "
                               "this one returns {} where the first returns {}",
                               branches,
                               names.size(),
                               firstNames.size()),
                   branch);
    }

    for (size_t index = 0; index < names.size(); index++) {
        if (names[index] != firstNames[index]) {
            throwError(fmt::format("{} must return the same column names: "
                                   "column {} is '{}' where the first returns '{}'",
                                   branches,
                                   index + 1,
                                   names[index],
                                   firstNames[index]),
                       branch);
        }
    }
}

// The union emits one result table, so each branch has to say which columns it fills it
// with. A branch ending on a write or on a standalone CALL names none.
const Projection* CypherAnalyzer::unionBranchProjection(const RegularQuery* branch) const {
    const ReturnStmt* returnStmt = branch->getFirstSingleQuery()->getReturnStmt();
    if (!returnStmt) {
        throwError("Every sub-query of a UNION must end with a RETURN clause", branch);
    }

    return returnStmt->getProjection();
}

void CypherAnalyzer::collectProjectionNames(const Projection* projection,
                                            std::vector<std::string_view>& names) {
    names.clear();

    for (const Projection::ReturnItem& item : projection->items()) {
        const std::optional<std::string_view> name = std::visit([projection](auto&& projected) {
            return projection->getName(projected);
        }, item);

        names.push_back(name.value_or(std::string_view {}));
    }
}

// A query part reads then writes: once a part has written, the only clause that may read
// again is the WITH that ends it. `CREATE (n) MATCH (m)` is not a part with its clauses
// out of order, it is two parts with the cut between them left out
void CypherAnalyzer::throwOnReadAfterUpdate(const StmtContainer* stmts) const {
    bool hasWritten = false;

    for (const Stmt* stmt : stmts->stmts()) {
        const Stmt::Kind kind = stmt->getKind();

        if (kind == Stmt::Kind::WITH || kind == Stmt::Kind::NEXT_SEGMENT) {
            hasWritten = false;
            continue;
        }

        const bool isReadingClause = Stmt::isReading(stmt);
        if (isReadingClause && hasWritten) {
            throwError("A reading clause cannot follow an updating clause: separate them with a WITH",
                       stmt);
        }

        if (Stmt::writesToTheGraph(stmt)) {
            hasWritten = true;
        }
    }
}

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

void CypherAnalyzer::setScope(DeclContext* scope) {
    _ctxt = scope;
    _exprAnalyzer->setDeclContext(_ctxt);
    _readAnalyzer->setDeclContext(_ctxt);
    _writeAnalyzer->setDeclContext(_ctxt);
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

void CypherAnalyzer::analyzeDistinct(const Projection* projection, bool isAggregate) const {
    // An aggregating projection emits one row per group, so no two of its rows are equal
    // and the dedup drops nothing: its keys answer to analyzeAggregateOrderBy instead
    if (isAggregate || !projection->hasOrderBy()) {
        return;
    }

    const OrderBy* orderBy = projection->getOrderBy();

    for (const OrderByItem* item : orderBy->getItems()) {
        const Expr* keyExpr = item->getExpr();

        // The dedup keys its rows on the columns the projection carries, so a key over
        // those holds one value per row it kept - the test a grouping key answers too
        if (!isGroupWise(keyExpr, projection)) {
            throwError("ORDER BY with DISTINCT may only order by expressions over the "
                       "returned columns.",
                       keyExpr);
        }
    }
}

void CypherAnalyzer::analyzeAggregateOrderBy(const Projection* projection) const {
    if (!projection->hasOrderBy()) {
        return;
    }

    const OrderBy* orderBy = projection->getOrderBy();

    for (const OrderByItem* item : orderBy->getItems()) {
        const Expr* keyExpr = item->getExpr();

        if (!isGroupWise(keyExpr, projection)) {
            throwError("ORDER BY with an aggregate may only order by expressions over the "
                       "returned columns.",
                       keyExpr);
        }
    }
}

void CypherAnalyzer::throwOnOrderByAggregate(const Projection* projection, const Stmt* clause) const {
    const std::string_view clauseName = clause->getKind() == Stmt::Kind::WITH ? "WITH" : "RETURN";

    for (const OrderByItem* item : projection->getOrderBy()->getItems()) {
        const Expr* keyExpr = item->getExpr();

        if (keyExpr->isAggregate()) {
            throwError(fmt::format("Cannot use aggregation in ORDER BY if there are no aggregate "
                                   "expressions in the preceding {}",
                                   clauseName),
                       keyExpr);
        }
    }
}

void CypherAnalyzer::analyzeAggregateItems(const Projection* projection) const {
    for (const Projection::ReturnItem& returnItem : projection->items()) {
        const auto* exprPtr = std::get_if<Expr*>(&returnItem);
        if (!exprPtr || !(*exprPtr)->isAggregate()) {
            continue;
        }

        DeclSet elements;
        throwOnImplicitGroupingKey(*exprPtr, projection, elements);
    }
}

void CypherAnalyzer::throwOnImplicitGroupingKey(const Expr* expr,
                                                const Projection* projection,
                                                DeclSet& elements) const {
    if (aggregateSignatureOf(expr)) {
        return;
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return;
    }

    std::vector<const VarDecl*> elementDecls;
    for (const Expr* child : children) {
        elementDecls.clear();
        collectElementDecls(expr, child, elementDecls);
        elements.insert(elementDecls.begin(), elementDecls.end());

        if (child->isAggregate()) {
            throwOnImplicitGroupingKey(child, projection, elements);
        } else if (!isGroupWise(child, projection, elements)) {
            throwError("An expression beside an aggregate may only read the grouping keys: "
                       "return this one as a key of its own, or aggregate it.",
                       child);
        }

        for (const VarDecl* decl : elementDecls) {
            elements.erase(decl);
        }
    }
}

bool CypherAnalyzer::isGroupWise(const Expr* expr, const Projection* projection) const {
    DeclSet elements;

    return isGroupWise(expr, projection, elements);
}

// @param elements holds the variables the list comprehensions above this expression bound.
// Each is bound per element of its own source rather than per matched row, so it holds one
// value wherever it is read and orders the groups as a constant does.
bool CypherAnalyzer::isGroupWise(const Expr* expr,
                                 const Projection* projection,
                                 DeclSet& elements) const {
    if (!expr) {
        return true;
    }

    // A constant holds the same value in every matched row, so it holds one per group too
    if (!expr->isDynamic()) {
        return true;
    }

    // An aggregate reduces its group to one value, which is what a group-wise key is: a
    // count of the group orders the groups whether or not the projection returns it
    if (aggregateSignatureOf(expr)) {
        return true;
    }

    // A grouping key holds one value per group by construction, and so does the alias of
    // one: the projection carries that column, at whatever depth of the key it is read
    if (projection->hasItem(expr)) {
        return true;
    }

    const Expr::Kind kind = expr->getKind();

    if (kind == Expr::Kind::PROPERTY) {
        const PropertyExpr* property = static_cast<const PropertyExpr*>(expr);
        const VarDecl* const entityDecl = property->getEntityVarDecl();

        return elements.contains(entityDecl) || projection->hasItemDecl(entityDecl);
    } else if (kind == Expr::Kind::ENTITY_TYPES) {
        const EntityTypeExpr* entityType = static_cast<const EntityTypeExpr*>(expr);

        if (const Expr* operand = entityType->getOperand()) {
            return isGroupWise(operand, projection, elements);
        }

        const VarDecl* const entityDecl = entityType->getEntityVarDecl();

        return elements.contains(entityDecl) || projection->hasItemDecl(entityDecl);
    } else if (kind == Expr::Kind::SYMBOL) {
        // A returned variable, and the alias of a returned item, are both matched above:
        // a symbol reaching here names a variable the grouping consumed, unless a
        // comprehension over this key bound it
        const SymbolExpr* symbol = static_cast<const SymbolExpr*>(expr);

        return elements.contains(symbol->getDecl());
    } else if (kind == Expr::Kind::PATTERN_COMPREHENSION) {
        const PatternComprehensionExpr* comprehension = static_cast<const PatternComprehensionExpr*>(expr);

        return isGroupWise(comprehension, projection, elements);
    } else if (kind == Expr::Kind::COUNT_SUBQUERY) {
        const CountSubqueryExpr* count = static_cast<const CountSubqueryExpr*>(expr);

        return readsGroupWiseVariables(count->getBody(), projection, elements);
    } else if (kind == Expr::Kind::COLLECT_SUBQUERY) {
        const CollectSubqueryExpr* collect = static_cast<const CollectSubqueryExpr*>(expr);

        return readsGroupWiseVariables(collect->getBody(), projection, elements);
    } else if (kind == Expr::Kind::EXISTS) {
        const ExistsExpr* exists = static_cast<const ExistsExpr*>(expr);

        return readsGroupWiseVariables(exists->getBody(), projection, elements);
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return false;
    }

    std::vector<const VarDecl*> elementDecls;
    for (const Expr* child : children) {
        elementDecls.clear();
        collectElementDecls(expr, child, elementDecls);
        elements.insert(elementDecls.begin(), elementDecls.end());

        const bool childIsGroupWise = isGroupWise(child, projection, elements);

        for (const VarDecl* decl : elementDecls) {
            elements.erase(decl);
        }

        if (!childIsGroupWise) {
            return false;
        }
    }

    return true;
}

// A pattern comprehension reduces the matches of its pattern to one list, so it holds one
// value per group wherever the rows it is matched on do: the variables it joins onto have
// to be grouping keys, and the ones it binds of its own stand as an element does.
bool CypherAnalyzer::isGroupWise(const PatternComprehensionExpr* comprehension,
                                 const Projection* projection,
                                 DeclSet& elements) const {
    const PatternComprehensionExpr::OwnDecls& ownDecls = comprehension->getOwnDecls();
    elements.insert(ownDecls.begin(), ownDecls.end());

    const Pattern* const pattern = comprehension->getPattern();
    const WhereClause* const where = pattern->getWhere();
    const Expr* const predicate = where ? where->getExpr() : nullptr;

    const bool patternIsGroupWise = joinsGroupWiseVariables(pattern, projection, elements);
    const bool predicateIsGroupWise = isGroupWise(predicate, projection, elements);
    const bool projectionIsGroupWise = isGroupWise(comprehension->getProjection(), projection, elements);

    for (const VarDecl* const decl : ownDecls) {
        elements.erase(decl);
    }

    return patternIsGroupWise && predicateIsGroupWise && projectionIsGroupWise;
}

bool CypherAnalyzer::joinsGroupWiseVariables(const Pattern* pattern,
                                             const Projection* projection,
                                             DeclSet& elements) const {
    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            const MapLiteral* const properties = entity->getProperties();

            if (properties) {
                for (const auto& [key, value] : *properties) {
                    if (!isGroupWise(value, projection, elements)) {
                        return false;
                    }
                }
            }

            // An entity the pattern did not name is matched here and read nowhere else, so
            // no group can disagree on what it holds
            if (!entity->getSymbol()) {
                continue;
            }

            const VarDecl* const decl = entity->getDecl();

            if (!elements.contains(decl) && !projection->hasItemDecl(decl)) {
                return false;
            }
        }
    }

    return true;
}

bool CypherAnalyzer::readsGroupWiseVariables(const RegularQuery* body,
                                             const Projection* projection,
                                             const DeclSet& elements) const {
    DeclSet read;
    collectReadDecls(read);

    std::vector<const RegularQuery*> queries;
    RegularQuery::collectQueries(body, queries);

    for (const RegularQuery* query : queries) {
        for (const VarDecl* decl : query->getDeclContext()->decls()) {
            const auto sourceIt = _declSources.find(decl);
            const bool readsAnImport = sourceIt != _declSources.end() && read.contains(decl);

            if (!readsAnImport) {
                continue;
            }

            const VarDecl* source = sourceIt->second;

            if (!elements.contains(source) && !projection->hasItemDecl(source)) {
                return false;
            }
        }
    }

    return true;
}

void CypherAnalyzer::collectReadDecls(DeclSet& read) const {
    std::vector<const VarDecl*> pending;

    for (const Expr* expr : _ast->getExpressions()) {
        const Expr::Kind kind = expr->getKind();

        if (kind == Expr::Kind::SYMBOL) {
            const SymbolExpr* symbol = static_cast<const SymbolExpr*>(expr);
            pending.push_back(symbol->getDecl());
        } else if (kind == Expr::Kind::PROPERTY) {
            const PropertyExpr* property = static_cast<const PropertyExpr*>(expr);
            pending.push_back(property->getEntityVarDecl());
        } else if (kind == Expr::Kind::ENTITY_TYPES) {
            const EntityTypeExpr* entityType = static_cast<const EntityTypeExpr*>(expr);
            pending.push_back(entityType->getEntityVarDecl());
        }
    }

    for (const EntityPattern* entity : _ast->getEntityPatterns()) {
        pending.push_back(entity->getDecl());
    }

    while (!pending.empty()) {
        const VarDecl* decl = pending.back();
        pending.pop_back();

        if (!decl || !read.insert(decl).second) {
            continue;
        }

        const auto sourceIt = _declSources.find(decl);
        if (sourceIt != _declSources.end()) {
            pending.push_back(sourceIt->second);
        }
    }
}

void CypherAnalyzer::analyzeShortestPathReturn(const SinglePartQuery* query) const {
    const StmtContainer* stmts = query->getStmts();
    if (!stmts) {
        return;
    }

    const ShortestPathStmt* shortestPath = nullptr;
    for (const Stmt* stmt : stmts->stmts()) {
        if (stmt->getKind() == Stmt::Kind::SHORTESTPATH) {
            shortestPath = static_cast<const ShortestPathStmt*>(stmt);
            break;
        }
    }

    if (!shortestPath) {
        return;
    }

    const ReturnStmt* returnStmt = query->getReturnStmt();
    if (!returnStmt) {
        return;
    }

    const Projection* projection = returnStmt->getProjection();
    if (!projection) {
        return;
    }

    const VarDecl* distDecl = shortestPath->getDistDecl();
    const VarDecl* pathDecl = shortestPath->getPathDecl();

    for (const Projection::ReturnItem& item : projection->items()) {
        const VarDecl* consumed = nullptr;
        const void* location = shortestPath;

        VarDecl* const* declItem = std::get_if<VarDecl*>(&item);
        Expr* const* exprItem = std::get_if<Expr*>(&item);

        if (declItem) {
            const VarDecl* decl = *declItem;
            const bool isOutput = decl == distDecl || decl == pathDecl;
            if (decl && !isOutput) {
                consumed = decl;
            }
        } else if (exprItem) {
            consumed = findConsumedVariable(*exprItem, distDecl, pathDecl);
            location = *exprItem;
        }

        if (consumed) {
            throwError(fmt::format(shortestPathErr, consumed->getName()), location);
        }
    }
}

const VarDecl* CypherAnalyzer::findConsumedVariable(const Expr* expr,
                                                    const VarDecl* distDecl,
                                                    const VarDecl* pathDecl) const {
    if (!expr) {
        return nullptr;
    }

    const auto consumedDecl = [distDecl, pathDecl](const VarDecl* decl) -> const VarDecl* {
        if (!decl || decl == distDecl || decl == pathDecl) {
            return nullptr;
        }

        return decl;
    };

    const Expr::Kind kind = expr->getKind();

    if (kind == Expr::Kind::SYMBOL) {
        return consumedDecl(expr->getExprVarDecl());
    } else if (kind == Expr::Kind::PROPERTY) {
        const PropertyExpr* property = static_cast<const PropertyExpr*>(expr);
        return consumedDecl(property->getEntityVarDecl());
    } else if (kind == Expr::Kind::ENTITY_TYPES) {
        const EntityTypeExpr* entityType = static_cast<const EntityTypeExpr*>(expr);

        if (const Expr* operand = entityType->getOperand()) {
            return findConsumedVariable(operand, distDecl, pathDecl);
        }

        return consumedDecl(entityType->getEntityVarDecl());
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return nullptr;
    }

    for (const Expr* child : children) {
        const VarDecl* consumed = findConsumedVariable(child, distDecl, pathDecl);
        if (consumed) {
            return consumed;
        }
    }

    return nullptr;
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

void CypherAnalyzer::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw AnalyzeException(std::move(errorStr));
}
