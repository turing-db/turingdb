#include "CypherAnalyzer.h"

#include <spdlog/fmt/bundled/core.h>

#include "CypherAST.h"
#include "DiagnosticsManager.h"
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
#include "LoadNeo4jQuery.h"
#include "LoadJsonlQuery.h"
#include "S3ConnectQuery.h"
#include "S3TransferQuery.h"
#include "Projection.h"
#include "decl/DeclContext.h"
#include "expr/Expr.h"
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
    for (QueryCommand* query : _ast->queries()) {
        _currentQuery = query;
        DeclContext* ctxt = query->getDeclContext();

        _exprAnalyzer->setDeclContext(ctxt);
        _readAnalyzer->setDeclContext(ctxt);
        _writeAnalyzer->setDeclContext(ctxt);

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

            case QueryCommand::Kind::LOAD_NEO4J_QUERY:
                analyze(static_cast<LoadNeo4jQuery*>(query));
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

            // Nothing to analyze
            case QueryCommand::Kind::CHANGE_QUERY:
            case QueryCommand::Kind::COMMIT_QUERY:
            case QueryCommand::Kind::LIST_GRAPH_QUERY:
            case QueryCommand::Kind::SHOW_PROCEDURES_QUERY:
            break;

            default:
                 throwError("Unsupported query type", query);
            break;
        }
    }
}

void CypherAnalyzer::analyze(const SinglePartQuery* query) {
    DeclContext* ctxt = query->getDeclContext();

    const StmtContainer* readStmts = query->getReadStmts();
    const StmtContainer* updateStmts = query->getUpdateStmts();
    const ShortestPathStmt* shortestPathStmt = query->getShortestPathStmt();
    const ReturnStmt* returnStmt = query->getReturnStmt();

    bool returnMandatory = updateStmts == nullptr;

    // Generate read statements (optional)
    if (readStmts) {
        for (const Stmt* stmt : readStmts->stmts()) {
            if (stmt->getKind() == Stmt::Kind::CALL) {
                if (static_cast<const CallStmt*>(stmt)->isStandaloneCall()) {
                    returnMandatory = false;
                }
            }
            _readAnalyzer->analyze(stmt);
        }
    }

    if (shortestPathStmt) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();
        auto propName = shortestPathStmt->getEdgeProperty()->getName();

        const std::optional<PropertyType> propType = propTypeMap.get(propName);
        if (!propType) {
            throwError(fmt::format("Unknown property: {}", propName));
        }
        ctxt->getOrCreateNamedVariable(_ast,
                                       EvaluatedType::Integer,
                                       shortestPathStmt->getDistVar()->getName());
        ctxt->getOrCreateNamedVariable(_ast,
                                       EvaluatedType::GraphPath,
                                       shortestPathStmt->getPathVar()->getName());
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

    if (projection->isReturningAll()) {
        // Return all variables defined in the current query

        const DeclContext* ctxt = _currentQuery->getDeclContext();
        bioassert(ctxt, "Query context is invalid");

        for (VarDecl* decl : *ctxt) {
            if (decl->isUnnamed()) {
                continue;
            }

            // Push at the front since only since '*' is only allowed the beginning of the return statement
            projection->pushFrontDecl(decl);
            projection->setName(decl, decl->getName());
        }
    }

    const SourceManager* srcMan = _ast->getSourceManager();

    for (const Projection::ReturnItem& returnItem : projection->items()) {
        const auto* exprPtr = std::get_if<Expr*>(&returnItem);
        if (!exprPtr) {
            continue;
        }

        Expr* item = *exprPtr;
        std::string_view name;

        if (auto* symbol = dynamic_cast<SymbolExpr*>(item)) {
            name = symbol->getName();
        } else {
            name = srcMan->getStringRepr(std::bit_cast<std::uintptr_t>(item));
        }

        _exprAnalyzer->analyzeRootExpr(item);

        if (name.empty()) {
            continue;
        }

        if (projection->hasName(name)) {
            continue; // Already added this name (added by wildcard)
        }

        projection->setName(item, name);

        isAggregate |= item->isAggregate();
        hasGroupingKeys |= !item->isAggregate();

        const bool isBinary = item->getKind() == Expr::Kind::BINARY;
        if (isBinary) {
            throw AnalyzeException(
                "Binary expressions in RETURN clauses are not yet supported.");
        }

        const bool isUnary = item->getKind() == Expr::Kind::UNARY;
        if (isUnary) {
            throw AnalyzeException(
                "Unary expressions in RETURN clauses are not yet supported.");
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

void CypherAnalyzer::analyze(LoadNeo4jQuery* loadNeo4j) {
    std::string_view graphName = loadNeo4j->getGraphName();
    if (graphName.empty()) {
        graphName = loadNeo4j->getFilePath().basename();
    }

    loadNeo4j->setGraphName(graphName);

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : graphName) {
        if (!(isalnum(c) || c == '_')) {
            throwError(fmt::format("Graph name must only contain alphanumeric characters or '_': "
                                   "character '{}' not allowed.",
                                   c),
                       loadNeo4j);
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

void CypherAnalyzer::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw AnalyzeException(std::move(errorStr));
}
