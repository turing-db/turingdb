#include "CypherAnalyzer.h"

#include <spdlog/fmt/bundled/core.h>
#include <string_view>

#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "EdgePattern.h"
#include "EntityPattern.h"
#include "Literal.h"
#include "NodePattern.h"
#include "QualifiedName.h"
#include "ReadStmtAnalyzer.h"
#include "Symbol.h"
#include "SourceManager.h"
#include "SymbolChain.h"
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
#include "DeleteVectorIndexQuery.h"
#include "ShowVectorIndexesQuery.h"
#include "InstallExtensionQuery.h"
#include "Projection.h"
#include "WhereClause.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "expr/EntityTypeExpr.h"
#include "expr/Expr.h"
#include "expr/ExprChildren.h"
#include "expr/PropertyExpr.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
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
#include "stmt/Limit.h"
#include "CreateNodePropertyIndexQuery.h"
#include "CreateEdgePropertyIndexQuery.h"

#include "FunctionDecls.h"

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

}

CypherAnalyzer::CypherAnalyzer(CypherAST* ast, GraphView graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(graphView.metadata()),
    _exprAnalyzer(std::make_unique<ExprAnalyzer>(_ast, _graphView)),
    _readAnalyzer(std::make_unique<ReadStmtAnalyzer>(_ast, _graphView)),
    _writeAnalyzer(std::make_unique<WriteStmtAnalyzer>(_ast, _graphView))
{
    _readAnalyzer->setExprAnalyzer(_exprAnalyzer.get());
    _writeAnalyzer->setExprAnalyzer(_exprAnalyzer.get());
}

CypherAnalyzer::~CypherAnalyzer() {
}

void CypherAnalyzer::analyze() {
    _ast->getFunctionDecls()->initDefault();

    for (QueryCommand* query : _ast->queries()) {
        _ctxt = query->getDeclContext();

        _exprAnalyzer->setDeclContext(_ctxt);
        _readAnalyzer->setDeclContext(_ctxt);
        _writeAnalyzer->setDeclContext(_ctxt);

        switch (query->getKind()) {
            case QueryCommand::Kind::SINGLE_PART_QUERY:
                analyze(static_cast<const SinglePartQuery*>(query));
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
    const StmtContainer* readStmts = query->getReadStmts();
    const StmtContainer* updateStmts = query->getUpdateStmts();
    const ReturnStmt* returnStmt = query->getReturnStmt();

    bool returnMandatory = updateStmts == nullptr;

    // Generate read statements (optional)
    if (readStmts) {
        for (Stmt* stmt : readStmts->stmts()) {
            if (stmt->getKind() == Stmt::Kind::WITH) {
                analyze(static_cast<const WithStmt*>(stmt));
                continue;
            }

            if (stmt->getKind() == Stmt::Kind::CALL) {
                if (static_cast<const CallStmt*>(stmt)->isStandaloneCall()) {
                    returnMandatory = false;
                }
            }
            _readAnalyzer->analyze(stmt);
        }
    }

    // Generate update statements (optional)
    if (updateStmts) {
        for (const Stmt* stmt : updateStmts->stmts()) {
            _writeAnalyzer->analyze(stmt);
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
}

void CypherAnalyzer::analyze(const ReturnStmt* returnSt) {
    analyzeProjection(returnSt->getProjection(), returnSt);
}

void CypherAnalyzer::analyze(const WithStmt* withSt) {
    if (!_isV3) { // only supported by MLIR v3
        throwError("WITH not yet supported.", withSt);
    }

    Projection* projection = withSt->getProjection();

    analyzeWithAliases(projection);
    analyzeProjection(projection, withSt);
    analyzeWithOrderBy(projection);

    openWithScope(projection);

    const WhereClause* where = withSt->getWhere();
    if (!where) {
        return;
    }

    Expr* predicate = where->getExpr();
    _exprAnalyzer->analyzeRootExpr(predicate);

    if (predicate->isAggregate()) {
        throwError("Invalid use of aggregate expression in this context", predicate);
    }

    if (predicate->getType() != EvaluatedType::Bool) {
        throwError("WHERE expression must be a boolean", predicate);
    }
}

// The ORDER BY of a WITH sorts the rows the projection publishes, so its keys read the
// scope that projection opens - a variable the projection dropped is as out of reach here
// as it is in the WHERE. A RETURN is the other way round: it may order by any expression
// over the scope it ends, projected or not
void CypherAnalyzer::analyzeWithOrderBy(const Projection* projection) const {
    if (!projection->hasOrderBy()) {
        return;
    }

    for (const OrderByItem* item : projection->getOrderBy()->getItems()) {
        throwOnUnpublishedKeyVariable(item->getExpr(), projection);
    }
}

void CypherAnalyzer::throwOnUnpublishedKeyVariable(const Expr* keyExpr,
                                                   const Projection* projection) const {
    if (!keyExpr) {
        return;
    }

    // The name as the key spells it, not the one its declaration carries: an alias is a
    // second name for one declaration, and it is the alias the projection publishes
    std::string_view readName;
    if (keyExpr->getKind() == Expr::Kind::SYMBOL) {
        readName = static_cast<const SymbolExpr*>(keyExpr)->getSymbol()->getName();
    } else if (keyExpr->getKind() == Expr::Kind::PROPERTY) {
        readName = static_cast<const PropertyExpr*>(keyExpr)->getFullName()->front()->getName();
    }

    if (!readName.empty() && !projection->hasName(readName)) {
        throwError(fmt::format("Variable '{}' not found: a WITH may only order by the "
                               "columns it publishes",
                               readName),
                   keyExpr);
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(keyExpr, children)) {
        return;
    }

    for (const Expr* child : children) {
        throwOnUnpublishedKeyVariable(child, projection);
    }
}

// The names a WITH gives its columns are the whole scope of what follows, so a statement
// after the barrier resolves its items and nothing the projection dropped
void CypherAnalyzer::openWithScope(const Projection* projection) {
    DeclContext* scope = DeclContext::create(_ast, _ctxt);

    for (const Projection::ReturnItem& returnItem : projection->items()) {
        if (const auto* declPtr = std::get_if<VarDecl*>(&returnItem)) {
            const VarDecl* decl = *declPtr;
            scope->getOrCreateNamedVariable(_ast, decl->getType(), decl->getName());
            continue;
        }

        const Expr* item = std::get<Expr*>(returnItem);
        const std::optional<std::string_view> name = projection->getName(item);
        bioassert(name.has_value(), "Projected item of a WITH without a name.");

        scope->getOrCreateNamedVariable(_ast, item->getType(), *name);
    }

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

        // An item reading the alias of an aggregate is aggregate too: its value exists
        // once the group is complete, not once per row, so it groups nothing
        if (readsAnAggregateItem(item, projection)) {
            item->setAggregate();
        }

        bioassert(!name.empty(), "All declared variable must have a name.");

        // Reported before the alias is declared, so that a duplicate column name is not
        // masked by the type conflict the second declaration of that name would raise
        if (projection->hasName(name)) {
            throwError(fmt::format("Return items must have unique names; "
                                   "{} was already defined.", name), item);
        }

        if (hasExplicitAlias) {
            declareItemAlias(item, name);
        }

        projection->setName(item, name);

        isAggregate |= item->isAggregate();
        hasGroupingKeys |= !item->isAggregate();
    }

    if (projection->hasOrderBy()) {
        analyze(projection->getOrderBy(), projection);

        // An aggregate in the ORDER BY aggregates the projection as an aggregate item
        // would - RETURN a.name ORDER BY count(b) orders one row per name - so the items
        // beside it are the grouping keys, and a projection of nothing but keys is
        // aggregating after all
        for (const OrderByItem* item : projection->getOrderBy()->getItems()) {
            isAggregate |= item->getExpr()->isAggregate();
        }
    }

    if (!_isV3) {
        const bool multipleReturns = projection->items().size() != 1;
        if (isAggregate && multipleReturns) {
            throwError("Aggregates may not yet be combined with multiple return items.",
                       clause);
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

    if (projection->isDistinct()) {
        analyzeDistinct(projection, clause);
    }

    if (isAggregate) {
        projection->setAggregate();
        projection->setHasGroupingKeys(hasGroupingKeys);

        analyzeNestedAggregates(projection);
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

    // A declaration the query did not name was generated for an expression and is reached
    // through it, so an alias spelling that name is no redeclaration and takes it
    const bool takenByAVariable = declared && !declared->isUnnamed();

    if (takenByAVariable && declared != aliasedDecl) {
        throwError(fmt::format("Variable '{}' is already declared", alias), item);
    }

    _ctxt->declareAlias(alias, aliasedDecl);
}

void CypherAnalyzer::setV3() {
    _isV3 = true;
    _exprAnalyzer->setV3();
    _readAnalyzer->setV3();
}

void CypherAnalyzer::analyzeDistinct(const Projection* projection, const Stmt* clause) const {
    if (!_isV3) { // only supported by MLIR v3
        throwError("DISTINCT not yet supported.", clause);
    }

    if (!projection->hasOrderBy()) {
        return;
    }

    const OrderBy* orderBy = projection->getOrderBy();

    for (const OrderByItem* item : orderBy->getItems()) {
        const Expr* keyExpr = item->getExpr();

        // A constant key holds the same value in every row, so it orders nothing and
        // names no column the dedup could have dropped
        if (!keyExpr->isDynamic()) {
            continue;
        }

        if (!projection->hasItem(keyExpr)) {
            throwError("ORDER BY with DISTINCT may only order by returned columns.", keyExpr);
        }
    }
}

void CypherAnalyzer::analyzeNestedAggregates(const Projection* projection) const {
    for (const Projection::ReturnItem& returnItem : projection->items()) {
        const auto* exprPtr = std::get_if<Expr*>(&returnItem);
        if (!exprPtr) {
            continue;
        }

        analyzeAggregateArguments(*exprPtr, projection);
    }
}

void CypherAnalyzer::analyzeAggregateArguments(const Expr* expr, const Projection* projection) const {
    if (!expr) {
        return;
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return;
    }

    const FunctionSignature* aggregate = aggregateSignatureOf(expr);

    if (aggregate) {
        for (const Expr* argument : children) {
            if (readsAnAggregateItem(argument, projection)) {
                throwError(fmt::format("Aggregate functions may not be nested: the argument of "
                                       "'{}' names an aggregate of the same projection",
                                       aggregate->getFullName()),
                           expr);
            }
        }
    }

    for (const Expr* child : children) {
        analyzeAggregateArguments(child, projection);
    }
}

bool CypherAnalyzer::readsAnAggregateItem(const Expr* expr, const Projection* projection) const {
    if (!expr) {
        return false;
    }

    const Expr* namedItem = projection->findItemExpr(expr);
    if (namedItem && namedItem->isAggregate()) {
        return true;
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return false;
    }

    for (const Expr* child : children) {
        if (readsAnAggregateItem(child, projection)) {
            return true;
        }
    }

    return false;
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

bool CypherAnalyzer::isGroupWise(const Expr* expr, const Projection* projection) const {
    if (!expr) {
        return true;
    }

    // A constant holds the same value in every matched row, so it holds one per group too
    if (!expr->isDynamic()) {
        return true;
    }

    // An aggregate reduces its group to one value, which is what a group-wise key is: a
    // count of the group orders the groups whether or not the projection returns it
    if (expr->isAggregate()) {
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

        return projection->hasVariableItem(property->getEntityVarDecl());
    } else if (kind == Expr::Kind::ENTITY_TYPES) {
        const EntityTypeExpr* entityType = static_cast<const EntityTypeExpr*>(expr);

        return projection->hasVariableItem(entityType->getEntityVarDecl());
    } else if (kind == Expr::Kind::SYMBOL) {
        // A returned variable, and the alias of a returned item, are both matched above:
        // a symbol reaching here names a variable the grouping consumed
        return false;
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return false;
    }

    return isGroupWise(children, projection);
}

bool CypherAnalyzer::isGroupWise(std::span<const Expr* const> exprs, const Projection* projection) const {
    for (const Expr* expr : exprs) {
        if (!isGroupWise(expr, projection)) {
            return false;
        }
    }

    return true;
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

        // Only MLIR v3 sorts the groups an aggregate reduces to; the pipeline hands the
        // sort one row and the whole projection's row count, and trips over the mismatch
        if (!_isV3 && expr->isAggregate()) {
            throwError("Aggregate expressions in ORDER BY are not supported yet", orderBySt);
        }
    }
}

void CypherAnalyzer::analyze(Skip* skipSt) {
    Expr* expr = skipSt->getExpr();
    _exprAnalyzer->analyzeRootExpr(expr);

    if (expr->getType() != EvaluatedType::Integer) {
        throwError("SKIP expression must be an integer", skipSt);
    }

    if (expr->isDynamic() || expr->isAggregate()) {
        throwError("SKIP expression must be a value that can be evaluated at compile time", skipSt);
    }
}

void CypherAnalyzer::analyze(Limit* limitSt) {
    Expr* expr = limitSt->getExpr();
    _exprAnalyzer->analyzeRootExpr(expr);

    if (expr->getType() != EvaluatedType::Integer) {
        throwError("LIMIT expression must be an integer", limitSt);
    }

    if (expr->isDynamic() || expr->isAggregate()) {
        throwError("LIMIT expression must be a value that can be evaluated at compile time", limitSt);
    }
}

void CypherAnalyzer::analyze(const LoadGraphQuery* loadGraph) {
    const std::string_view graphName = loadGraph->getGraphName();
    if (graphName.empty()) {
        throwError("LOAD GRAPH should not have an empty graph name");
    }

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : graphName) {
        if (!(isalnum(c) || c == '_')) [[unlikely]] {
            throwError(fmt::format("Graph name must only contain alphanumeric characters or '_': character '{}' not allowed.", c), loadGraph);
        }
    }
}

void CypherAnalyzer::analyze(const CreateGraphQuery* createGraph) {
    const std::string_view graphName = createGraph->getGraphName();
    if (graphName.empty()) {
        throwError("CREATE GRAPH should not have an empty graph name");
        // Check that the graph name is only [A-Z0-9_]+
        for (char c : graphName) {
            if (!(isalnum(c) || c == '_')) [[unlikely]] {
                throwError(fmt::format("Graph name must only contain alphanumeric characters or '_': "
                                       "character '{}' not allowed.",
                                       c),
                           createGraph);
            }
        }
    }
}

void CypherAnalyzer::analyze(LoadJsonlQuery* loadJsonl) {
    std::string_view graphName = loadJsonl->getGraphName();
    if (graphName.empty()) {
        graphName = loadJsonl->getFilePath().basename();
    }

    loadJsonl->setGraphName(graphName);

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : graphName) {
        if (!(isalnum(c) || c == '_')) {
            throwError(fmt::format("Graph name must only contain alphanumeric characters or '_': "
                                   "character '{}' not allowed.",
                                   c),
                       loadJsonl);
        }
    }

    const auto& embDims = loadJsonl->getEmbeddingSpecs();
    const bool validDims =
        std::ranges::all_of(embDims, [](const auto& kv) { return kv.second > 1; });
    if (!validDims) {
        throwError("All embedding properties must have dimension greater than 1.");
    }
}

void CypherAnalyzer::analyze(LoadGMLQuery* loadGML) {
    std::string_view graphName = loadGML->getGraphName();
    if (graphName.empty()) {
        graphName = loadGML->getFilePath().basename();
    }

    loadGML->setGraphName(graphName);

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : graphName) {
        if (!(isalnum(c) || c == '_')) [[unlikely]] {
            throwError(fmt::format("Graph name must only contain alphanumeric characters or '_': "
                                   "character '{}' not allowed.",
                                   c),
                       loadGML);
        }
    }
}

void CypherAnalyzer::analyze(LoadParquetQuery* loadParquet) {
    std::string_view graphName = loadParquet->getGraphName();
    if (graphName.empty()) {
        graphName = loadParquet->getFilePath().basename();
    }

    loadParquet->setGraphName(graphName);

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : graphName) {
        if (!(isalnum(c) || c == '_')) {
            throwError(fmt::format(
                           "Graph name must only contain alphanumeric characters or '_': "
                           "character '{}' not allowed.",
                           c),
                       loadParquet);
        }
    }
}

void CypherAnalyzer::analyze(const S3ConnectQuery* s3Connect) {
    const std::string_view accessId = s3Connect->getAccessId();
    const std::string_view secretKey = s3Connect->getSecretKey();
    const std::string_view region = s3Connect->getRegion();

    if (accessId.empty()) {
        throwError("S3 Access ID cannot be empty", s3Connect);
    }

    if (secretKey.empty()) {
        throwError("S3 Secret Key cannot be empty", s3Connect);
    }

    if (region.empty()) {
        throwError("S3 Region cannot be empty", s3Connect);
    }
}

void CypherAnalyzer::analyze(S3TransferQuery* s3Transfer) {
    std::string_view s3URL = s3Transfer->getS3Url();

    if (s3URL.substr(0, 5) != "s3://") {
        throwError(fmt::format("Invalid S3 URL: {}", s3URL));
    }
    s3URL.remove_prefix(5);
    const auto bucketEnd = s3URL.find('/');

    if (bucketEnd == std::string_view::npos) {
        throwError(fmt::format("S3 Bucket Not Found: {}", s3URL));
    }

    s3Transfer->setS3Bucket(s3URL.substr(0, bucketEnd));
    s3URL.remove_prefix(bucketEnd + 1);

    if (s3URL.empty()) {
        throwError(fmt::format("S3 Prefix/Folder not found: {}", s3URL));
    }

    if (s3URL.back() != '/') {
        // S3 'file' resource
        s3Transfer->setS3File(s3URL);
        return;
    }

    // S3 Directory Resource
    s3Transfer->setS3Prefix(s3URL);
}

void CypherAnalyzer::analyze(const CreateVectorIndexQuery* query) {
    if (query->getDimension() == 0) {
        throwError("Vector index dimension must be greater than 0", query);
    }

    const std::string_view indexName = query->getIndexName();
    if (indexName.empty()) {
        throwError("Vector index name cannot be empty", query);
    }
}

void CypherAnalyzer::analyze(const LoadVectorQuery* query) {
    const std::string_view filePath = query->getFilePath();
    if (filePath.empty()) {
        throwError("LOAD VECTOR file path cannot be empty", query);
    }

    const std::string_view indexName = query->getIndexName();
    if (indexName.empty()) {
        throwError("LOAD VECTOR index name cannot be empty", query);
    }
}

void CypherAnalyzer::analyze(const LoadEmbeddingQuery* query) {
    const std::string_view filePath = query->getFilePath();
    if (filePath.empty()) {
        throwError("LOAD EMBEDDING file path cannot be empty", query);
    }

    const std::string_view propertyName = query->getPropertyName();
    if (propertyName.empty()) {
        throwError("LOAD EMBEDDING property name cannot be empty", query);
    }
}

void CypherAnalyzer::analyze(const InstallExtensionQuery* query) {
    const std::string_view name = query->getExtensionName();
    if (name.empty()) {
        throwError("INSTALL extension name cannot be empty", query);
    }

    for (char c : name) {
        if (!(isalnum(c) || c == '_')) [[unlikely]] {
            throwError(fmt::format("Extension name must only contain "
                                   "alphanumeric characters or '_': "
                                   "character '{}' not allowed.", c),
                       query);
        }
    }
}

void CypherAnalyzer::analyze(const CreateNodePropertyIndexQuery* query) {
    // CREATE INDEX _ FOR *(n)* ON n._
    const NodePattern* node = query->nodePattern();
    bioassert(node, "Failed to get node pattern.");

    const SymbolChain* labels = node->labels();
    const MapLiteral* properties = node->getProperties();
    const bool haveLabelConstraints = labels && !labels->empty();
    const bool havePropertyConstraints = properties && !properties->empty();
    if (haveLabelConstraints || havePropertyConstraints) {
        throwError("Constrained node indexes are not yet supported.", node);
    }
    PropertyExpr* propertyExpr = query->propertyExpr();
    bioassert(propertyExpr, "Failed to get property expression.");

    // Register an empty decl for the node of the query, so we can analyze the property
    // expression. The decl cannot be used anywhere aside from the property expr, so an
    // empty/invalid decl is fine.
    _exprAnalyzer->registerNodePatternDeclaration(node);
    _exprAnalyzer->analyzePropertyExpr(propertyExpr);

    const PropertyTypeMap& propTypes = _graphMetadata.propTypes();
    const std::string_view propName = propertyExpr->getPropName();
    const std::optional<PropertyType> maybePropType = propTypes.get(propName);
    if (!maybePropType) {
        const std::string err = fmt::format("Property {} to index does not exist.", propName);
        throwError(std::move(err), propertyExpr);
    }

    const PropertyType propType = *maybePropType;
    const GraphReader reader = _graphView.read();
    if (!reader.isNodeProperty(propType._id)) {
        throwError("Property is not a node property.", propertyExpr);
    }
}

void CypherAnalyzer::analyze(const CreateEdgePropertyIndexQuery* query) {
    // CREATE INDEX _ FOR *[e]* ON e._
    const EdgePattern* edge = query->edgePattern();
    bioassert(edge, "Failed to get edge pattern.");

    const SymbolChain* types = edge->types();
    const MapLiteral* properties = edge->getProperties();
    const bool haveTypeConstraints = types && !types->empty();
    const bool havePropertyConstraints = properties && !properties->empty();
    if (haveTypeConstraints || havePropertyConstraints) {
        throwError("Constrained edge indexes are not yet supported.", edge);
    }
    PropertyExpr* propertyExpr = query->propertyExpr();
    bioassert(propertyExpr, "Failed to get property expression.");

    // Register an empty decl for the edge of the query, so we can analyze the property
    // expression. The decl cannot be used anywhere aside from the property expr, so an
    // empty/invalid decl is fine.
    _exprAnalyzer->registerEdgePatternDeclaration(edge);
    _exprAnalyzer->analyzePropertyExpr(propertyExpr);

    const PropertyTypeMap& propTypes = _graphMetadata.propTypes();
    const std::string_view propName = propertyExpr->getPropName();
    const std::optional<PropertyType> maybePropType = propTypes.get(propName);
    if (!maybePropType) {
        const std::string err = fmt::format("Property {} to index does not exist.", propName);
        throwError(std::move(err), propertyExpr);
    }

    const PropertyType propType = *maybePropType;
    const GraphReader reader = _graphView.read();
    if (!reader.isEdgeProperty(propType._id)) {
        throwError("Property is not an edge property.", propertyExpr);
    }
}

void CypherAnalyzer::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw AnalyzeException(std::move(errorStr));
}
