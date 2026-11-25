#include "CypherAnalyzer.h"

#include <utility>
#include <string_view>
#include <vector>

#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "ReadStmtAnalyzer.h"
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
#include "decl/DeclContext.h"
#include "expr/EntityTypeExpr.h"
#include "expr/Expr.h"
#include "expr/ExprChildren.h"
#include "expr/PropertyExpr.h"
#include "stmt/ShortestPathStmt.h"
#include "decl/VarDecl.h"
#include "stmt/StmtContainer.h"
#include "stmt/ReturnStmt.h"
#include "stmt/WithStmt.h"
#include "stmt/CallStmt.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/NextSegmentStmt.h"
#include "CreateNodePropertyIndexQuery.h"
#include "CreateEdgePropertyIndexQuery.h"

#include "AnalyzeException.h"

using namespace db;

namespace {

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

void CypherAnalyzer::setScope(DeclContext* scope) {
    _ctxt = scope;
    _exprAnalyzer->setDeclContext(_ctxt);
    _readAnalyzer->setDeclContext(_ctxt);
    _writeAnalyzer->setDeclContext(_ctxt);
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

void CypherAnalyzer::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw AnalyzeException(std::move(errorStr));
}
