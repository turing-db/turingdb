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
#include "LoadJsonlQuery.h"
#include "S3ConnectQuery.h"
#include "S3TransferQuery.h"
#include "CreateVectorIndexQuery.h"
#include "LoadVectorQuery.h"
#include "DeleteVectorIndexQuery.h"
#include "ShowVectorIndexesQuery.h"
#include "InstallExtensionQuery.h"
#include "Projection.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "expr/Expr.h"
#include "expr/PropertyExpr.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "stmt/ShortestPathStmt.h"
#include "decl/VarDecl.h"
#include "expr/SymbolExpr.h"
#include "stmt/StmtContainer.h"
#include "stmt/ReturnStmt.h"
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
    Projection* projection = returnSt->getProjection();

    if (projection->isDistinct()) {
        throwError("DISTINCT not supported", returnSt);
    }

    if (projection->hasOrderBy()) {
        analyze(projection->getOrderBy());
    }

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
        // Explicit aliases via the AS keyword
        bool hasExplicitAlias = false;

        if (auto* symbolExpr = dynamic_cast<SymbolExpr*>(item)) {
            Symbol* symbol = symbolExpr->getSymbol();
            name = symbol->getName();
        } else {
            hasExplicitAlias = !item->getName().empty();
            name = item->getName();
            if (name.empty()) {
                const std::string_view name =
                    srcMan->getStringRepr(std::bit_cast<std::uintptr_t>(item));
                if (name.empty()) [[unlikely]] {
                    throwError("Failed to generate name for projection item", item);
                }

                item->setName(name);
            }

            name = item->getName();
        }

        _exprAnalyzer->analyzeRootExpr(item);

        bioassert(!name.empty(), "All declared variable must have a name.");

        // For exprs with explicit AS aliases, update their decl to be named
        if (hasExplicitAlias) {
            const EvaluatedType type = item->getType();
            const VarDecl* namedDecl = _ctxt->getOrCreateNamedVariable(_ast, type, name);
            item->setExprVarDecl(namedDecl);
        }

        if (projection->hasName(name)) {
            throwError(fmt::format("Return items must have unique names; "
                                   "{} was already defined.", name), item);
        }

        projection->setName(item, name);

        isAggregate |= item->isAggregate();
        hasGroupingKeys |= !item->isAggregate();
    }

    const bool multipleReturns = projection->items().size() != 1;
    if (isAggregate && multipleReturns)  {
        throwError("Aggregates may not yet be combined with multiple return items.",
                   returnSt);
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
        }
    }

    if (isAggregate) {
        projection->setAggregate();
        projection->setHasGroupingKeys(hasGroupingKeys);
    }
}

void CypherAnalyzer::analyze(OrderBy* orderBySt) {
    for (OrderByItem* item : orderBySt->getItems()) {
        Expr* expr = item->getExpr();
        _exprAnalyzer->analyzeRootExpr(expr);

        if (expr->isAggregate()) {
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
