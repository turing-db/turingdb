#include "PlanGraphGenerator.h"

#include "BioAssert.h"
#include "FunctionInvocation.h"
#include "Projection.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "decl/DeclContext.h"
#include "expr/BinaryExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"
#include "ExprDependencies.h"
#include "nodes/ChangeNode.h"
#include "nodes/CommitNode.h"
#include "stmt/Limit.h"
#include "stmt/OrderBy.h"
#include "stmt/ReturnStmt.h"
#include "stmt/ShortestPathStmt.h"
#include "stmt/Skip.h"
#include "stmt/StmtContainer.h"
#include "views/GraphView.h"

#include "DiagnosticsManager.h"
#include "CypherAST.h"
#include "PlanGraphVariables.h"
#include "ReadStmtGenerator.h"
#include "WriteStmtGenerator.h"

#include "nodes/OrderByNode.h"
#include "nodes/LimitNode.h"
#include "nodes/SkipNode.h"
#include "nodes/WriteNode.h"
#include "nodes/AggregateEvalNode.h"
#include "nodes/FuncEvalNode.h"
#include "nodes/ProduceResultsNode.h"
#include "nodes/GetEntityTypeNode.h"
#include "nodes/GetPropertyWithNullNode.h"
#include "nodes/LoadGraphNode.h"
#include "nodes/ListGraphNode.h"
#include "nodes/CreateGraphNode.h"
#include "nodes/LoadGMLNode.h"
#include "nodes/LoadJsonlNode.h"
#include "nodes/S3ConnectNode.h"
#include "nodes/S3TransferNode.h"
#include "nodes/ShowProceduresNode.h"
#include "nodes/ShortestPathNode.h"
#include "nodes/ExprEvalNode.h"
#include "nodes/CreateVectorIndexNode.h"
#include "nodes/LoadVectorNode.h"
#include "nodes/DeleteVectorIndexNode.h"
#include "nodes/ShowVectorIndexesNode.h"

#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "ChangeQuery.h"
#include "CommitQuery.h"
#include "ListGraphQuery.h"
#include "CreateGraphQuery.h"
#include "S3ConnectQuery.h"
#include "S3TransferQuery.h"
#include "ShowProceduresQuery.h"
#include "LoadGraphQuery.h"
#include "LoadGMLQuery.h"
#include "LoadJsonlQuery.h"
#include "CreateVectorIndexQuery.h"
#include "LoadVectorQuery.h"
#include "DeleteVectorIndexQuery.h"
#include "ShowVectorIndexesQuery.h"

#include "decl/VarDecl.h"
#include "decl/PatternData.h"

#include "PlannerException.h"

using namespace db;

PlanGraphGenerator::PlanGraphGenerator(const CypherAST& ast,
                                       const GraphView& view)
    : _ast(&ast),
    _view(view),
    _variables(std::make_unique<PlanGraphVariables>(&_tree))
{
}

PlanGraphGenerator::~PlanGraphGenerator() {
}

void PlanGraphGenerator::generate(const QueryCommand* query) {
    switch (query->getKind()) {
        case QueryCommand::Kind::SINGLE_PART_QUERY:
            generateSinglePartQuery(static_cast<const SinglePartQuery*>(query));
        break;

        case QueryCommand::Kind::LOAD_GRAPH_QUERY:
            generateLoadGraphQuery(static_cast<const LoadGraphQuery*>(query));
        break;
        
        case QueryCommand::Kind::LIST_GRAPH_QUERY:
            generateListGraphQuery(static_cast<const ListGraphQuery*> (query));
        break;

        case QueryCommand::Kind::LOAD_JSONL_QUERY:
            generateLoadJsonlQuery(static_cast<const LoadJsonlQuery*>(query));
        break;

        case QueryCommand::Kind::CHANGE_QUERY:
            generateChangeQuery(static_cast<const ChangeQuery*>(query));
        break;

        case QueryCommand::Kind::COMMIT_QUERY:
            generateCommitQuery(static_cast<const CommitQuery*>(query));
        break;

        case QueryCommand::Kind::CREATE_GRAPH_QUERY:
            generateCreateGraphQuery(static_cast<const CreateGraphQuery*> (query));
        break;

        case QueryCommand::Kind::LOAD_GML_QUERY:
            generateLoadGMLQuery(static_cast<const LoadGMLQuery*>(query));
        break;

        case QueryCommand::Kind::S3_CONNECT_QUERY:
            generateS3ConnectQuery(static_cast<const S3ConnectQuery*>(query));
        break;

        case QueryCommand::Kind::S3_TRANSFER_QUERY:
            generateS3TransferQuery(static_cast<const S3TransferQuery*>(query));
        break;

        case QueryCommand::Kind::SHOW_PROCEDURES_QUERY:
            generateShowProceduresQuery(static_cast<const ShowProceduresQuery*>(query));
        break;

        case QueryCommand::Kind::CREATE_VECTOR_INDEX_QUERY:
            generateCreateVectorIndexQuery(static_cast<const CreateVectorIndexQuery*>(query));
        break;

        case QueryCommand::Kind::LOAD_VECTOR_QUERY:
            generateLoadVectorQuery(static_cast<const LoadVectorQuery*>(query));
        break;

        case QueryCommand::Kind::DELETE_VECTOR_INDEX_QUERY:
            generateDeleteVectorIndexQuery(static_cast<const DeleteVectorIndexQuery*>(query));
        break;

        case QueryCommand::Kind::SHOW_VECTOR_INDEXES_QUERY:
            generateShowVectorIndexesQuery(static_cast<const ShowVectorIndexesQuery*>(query));
        break;

        default:
            throwError(fmt::format("Unsupported query command of type {}", (uint64_t)query->getKind()), query);
        break;
    }

    _tree.removeIsolatedNodes();
}

void PlanGraphGenerator::generateChangeQuery(const ChangeQuery* query) {
    auto* n = _tree.create<ChangeNode>(query->getOp());
    _tree.newOut<ProduceResultsNode>(n);
}

void PlanGraphGenerator::generateCommitQuery(const CommitQuery* query) {
    auto* n = _tree.create<CommitNode>();
    _tree.newOut<ProduceResultsNode>(n);
}

void PlanGraphGenerator::generateSinglePartQuery(const SinglePartQuery* query) {
    const StmtContainer* readStmts = query->getReadStmts();
    const StmtContainer* updateStmts = query->getUpdateStmts();
    const ShortestPathStmt* shortestPathStmt = query->getShortestPathStmt();
    const ReturnStmt* returnStmt = query->getReturnStmt();

    PlanGraphNode* currentNode = nullptr;

    // Generate read statements (optional)
    if (readStmts) {
        ReadStmtGenerator readGenerator(_ast, _view, &_tree, _variables.get());

        for (const Stmt* stmt : readStmts->stmts()) {
            readGenerator.generateStmt(stmt);
        }

        // Place joins on vars that have more than one input
        readGenerator.placeJoinsOnVars();

        // Place joins based on predicates
        readGenerator.placePredicateJoins();

        // Place joins based on procedures calls
        readGenerator.placeJoinsOnProcedures();

        // Insert ShortestPath Node
        if (shortestPathStmt) {
            const auto* declContext = query->getDeclContext();
            auto sourceName = shortestPathStmt->getSource()->getName();
            auto targetName = shortestPathStmt->getTarget()->getName();
            const auto propertyType = _view.metadata().propTypes().get(shortestPathStmt->getEdgeProperty()->getName()).value();
            auto distName = shortestPathStmt->getDistVar()->getName();
            auto pathName = shortestPathStmt->getPathVar()->getName();

            const auto* sourceDecl = declContext->getDecl(sourceName);
            const auto* targetDecl = declContext->getDecl(targetName);
            const auto* distDecl = declContext->getDecl(distName);
            const auto* pathDecl = declContext->getDecl(pathName);

            auto* sourceNode = _variables->getVarNode(sourceDecl);
            auto* targetNode = _variables->getVarNode(targetDecl);

            readGenerator.insertShortestPathNode(sourceNode,
                                                 targetNode,
                                                 propertyType,
                                                 distDecl,
                                                 pathDecl);
        }

        // Place joins that generate the endpoint, and retrieve it
        currentNode = readGenerator.generateEndpoint();
    }

    // Generate update statements (optional)
    if (updateStmts) {
        WriteStmtGenerator writeGenerator(_ast, &_tree, _variables.get());

        for (const Stmt* stmt : updateStmts->stmts()) {
            currentNode = writeGenerator.generateStmt(stmt, currentNode);
        }
    }

    // Generate return statement
    if (returnStmt) {
        currentNode = generateReturnStmt(returnStmt, currentNode);
    } else {
        // Generate an empty ProduceResults if we have no return
        // and we have updateStmts.
        // CALL standalone already handles its own ProduceResults
        // and readStmts alone must be followed by a RETURN as per grammar
        if (updateStmts) {
            currentNode = generateReturnNone(currentNode);
        }
    }

    bioassert(dynamic_cast<ProduceResultsNode*>(currentNode),
              "last node of PlanGraph is not a ProduceResultsNode");
}

void PlanGraphGenerator::generateLoadGraphQuery(const LoadGraphQuery* query) {
    LoadGraphNode* loadGraphNode = _tree.create<LoadGraphNode>(query->getGraphName());
    _tree.newOut<ProduceResultsNode>(loadGraphNode);
}

void PlanGraphGenerator::generateListGraphQuery(const ListGraphQuery* query) {
    ListGraphNode* listGraphNode = _tree.create<ListGraphNode>();
    _tree.newOut<ProduceResultsNode>(listGraphNode);
}

void PlanGraphGenerator::generateCreateGraphQuery(const CreateGraphQuery* query) {
    CreateGraphNode* createGraphNode = _tree.create<CreateGraphNode>(query->getGraphName());
    _tree.newOut<ProduceResultsNode>(createGraphNode);
}

void PlanGraphGenerator::generateLoadGMLQuery(const LoadGMLQuery* loadGML) {
    LoadGMLNode* loadGMLNode = _tree.create<LoadGMLNode>(loadGML->getGraphName(), loadGML->getFilePath());
    _tree.newOut<ProduceResultsNode>(loadGMLNode);
}

void PlanGraphGenerator::generateLoadJsonlQuery(const LoadJsonlQuery* query) {
    LoadJsonlNode* n = _tree.create<LoadJsonlNode>(query->getFilePath(), query->getGraphName());
    _tree.newOut<ProduceResultsNode>(n);
}

void PlanGraphGenerator::generateS3ConnectQuery(const S3ConnectQuery* query) {
    S3ConnectNode* s3ConnectNode = _tree.create<S3ConnectNode>(query->getAccessId(),
                                                               query->getSecretKey(),
                                                               query->getRegion());
    _tree.newOut<ProduceResultsNode>(s3ConnectNode);
}

void PlanGraphGenerator::generateS3TransferQuery(const S3TransferQuery* query) {
    const S3TransferNode::Direction direction = (query->getDirection() == S3TransferQuery::Direction::PULL)
        ? S3TransferNode::Direction::PULL
        : S3TransferNode::Direction::PUSH;

    S3TransferNode* s3TransferNode = _tree.create<S3TransferNode>(direction,
                                                                  query->getS3Bucket(),
                                                                  query->getS3Prefix(),
                                                                  query->getS3File(),
                                                                  query->getLocalPath());
    _tree.newOut<ProduceResultsNode>(s3TransferNode);
}

void PlanGraphGenerator::generateShowProceduresQuery(const ShowProceduresQuery* query) {
    ShowProceduresNode* showProceduresNode = _tree.create<ShowProceduresNode>();
    _tree.newOut<ProduceResultsNode>(showProceduresNode);
}

void PlanGraphGenerator::generateCreateVectorIndexQuery(const CreateVectorIndexQuery* query) {
    CreateVectorIndexNode* node = _tree.create<CreateVectorIndexNode>(
        query->getIndexName(),
        query->getDimension(),
        query->getMetric());
    _tree.newOut<ProduceResultsNode>(node);
}

void PlanGraphGenerator::generateLoadVectorQuery(const LoadVectorQuery* query) {
    LoadVectorNode* node = _tree.create<LoadVectorNode>(
        query->getFilePath(),
        query->getIndexName());
    _tree.newOut<ProduceResultsNode>(node);
}

void PlanGraphGenerator::generateDeleteVectorIndexQuery(const DeleteVectorIndexQuery* query) {
    DeleteVectorIndexNode* node = _tree.create<DeleteVectorIndexNode>(query->getIndexName());
    _tree.newOut<ProduceResultsNode>(node);
}

void PlanGraphGenerator::generateShowVectorIndexesQuery(const ShowVectorIndexesQuery* query) {
    ShowVectorIndexesNode* node = _tree.create<ShowVectorIndexesNode>();
    _tree.newOut<ProduceResultsNode>(node);
}

PlanGraphNode* PlanGraphGenerator::generateReturnStmt(const ReturnStmt* stmt, PlanGraphNode* prevNode) {
    const Projection* proj = stmt->getProjection();

    if (proj->isDistinct()) {
        throwError("DISTINCT not supported", stmt);
    }

    FuncEvalNode* funcEval = _tree.create<FuncEvalNode>();
    AggregateEvalNode* aggregateEval = _tree.create<AggregateEvalNode>();
    ExprEvalNode* exprEval = _tree.create<ExprEvalNode>();

    GetPropertyCache& getPropertyCache = _tree.getGetPropertyCache();
    GetEntityTypeCache& getEntityTypeCache = _tree.getGetEntityTypeCache();

    for (const Projection::ReturnItem& returnItem : proj->items()) {
        const auto* exprPtr = std::get_if<Expr*>(&returnItem);
        if (!exprPtr) {
            continue;
        }

        Expr* item = *exprPtr;

        if (ExprEvalNode::needsEvaluation(item)) {
            exprEval->addExpr(item);
        }

        ExprDependencies deps;
        deps.genExprDependencies(*_variables, item);

        for (ExprDependencies::VarDependency& dep : deps.getVarDeps()) {
            bioassert(prevNode, "Expression had dependencies, but no previous node to provide them.");

            if (auto* expr = dynamic_cast<PropertyExpr*>(dep._expr)) {
                const VarDecl* entityDecl = expr->getEntityVarDecl();
                const VarDecl* exprDecl = expr->getExprVarDecl();

                if (!exprDecl) [[unlikely]] {
                    throwError("Property expression does not have an expression variable declaration", expr);
                }

                if (!entityDecl) [[unlikely]] {
                    throwError("Property expression does not have an entity variable declaration", expr);
                }

                const auto* cached = getPropertyCache.cacheOrRetrieve(entityDecl, exprDecl, expr->getPropName());

                if (cached) {
                    // GetProperty is already present in the cache. Map the existing expr to the current one
                    if (!cached->_exprDecl) [[unlikely]] {
                        throwError("GetProperty expression does not have an expression variable declaration", expr);
                    }

                    expr->setExprVarDecl(cached->_exprDecl);
                    continue;
                }

                GetPropertyWithNullNode* n = _tree.newOut<GetPropertyWithNullNode>(prevNode, expr->getPropName());
                n->setExpr(expr);
                n->setEntityVarDecl(entityDecl);
                prevNode = n;

            } else if (auto* expr = dynamic_cast<EntityTypeExpr*>(dep._expr)) {
                const VarDecl* entityDecl = expr->getEntityVarDecl();
                const VarDecl* exprDecl = expr->getExprVarDecl();

                if (!exprDecl) [[unlikely]] {
                    throwError("Entity type expression does not have an expression variable declaration", expr);
                }

                if (!entityDecl) [[unlikely]] {
                    throwError("Entity type expression does not have an entity variable declaration", expr);
                }

                const auto* cached = getEntityTypeCache.cacheOrRetrieve(entityDecl, exprDecl);

                if (cached) {
                    // GetEntityType is already present in the cache. Map the existing expr to the current one

                    if (!cached->_exprDecl) [[unlikely]] {
                        throwError("GetEntityType expression does not have an expression variable declaration", expr);
                    }

                    expr->setExprVarDecl(cached->_exprDecl);
                    continue;
                }

                GetEntityTypeNode* n = _tree.newOut<GetEntityTypeNode>(prevNode);
                n->setExpr(expr);
                n->setEntityVarDecl(entityDecl);
                prevNode = n;

            } else if (dynamic_cast<const SymbolExpr*>(dep._expr)) {
                // Symbol value should already be in a column in a block, no need to change anything
            } else {
                throwError("Expression dependency could not be handled in the predicate evaluation");
            }
        }

        // Functions may have expressions which need be evaluated prior to the functions
        // evaluation, e.g. COUNT(5 + 5).
        for (const ExprDependencies::FuncDependency& dep : deps.getFuncDeps()) {
            const FunctionInvocationExpr* funcExpr = dep._expr;
            const FunctionInvocation* funcInvok = funcExpr->getFunctionInvocation();
            const FunctionSignature* signature = funcInvok->getSignature();

            const ExprChain* arguments = funcInvok->getArguments();
            for (const Expr* argument : *arguments) {
                if (ExprEvalNode::needsEvaluation(argument)) {
                    exprEval->addExpr(argument);
                }
            }

            if (signature->isAggregate()) {
                aggregateEval->addFunc(dep._expr);
            } else {
                funcEval->addFunc(dep._expr);
            }
        }

        if (proj->isAggregate() && !item->isAggregate()) {
            const Expr::Kind kind = item->getKind();

            if (kind != Expr::Kind::SYMBOL
                && kind != Expr::Kind::PROPERTY) {
                throwError("Complex grouping keys are not supported yet. Only variables (e.g. n), "
                           "or property expression (e.g. n.name) are allowed",
                           proj);
            }

            aggregateEval->addGroupByKey(item);
        }
    }

    ProduceResultsNode* results = _tree.create<ProduceResultsNode>();

    // Expressions, functions and aggregates do not require a previous input.
    // e.g. RETURN sum(sqrt(5))
    // @ref prevNode will be nullptr in the above, but still requires evaluation
    // FIXME: There is a limitation which prevents queries such as
    // MATCH (n) RETURN 5 + COUNT(n)
    // because this requires the @ref aggregateEval node to be placed before the @ref
    // exprEval node. However this is currently guarded by an exception raised when
    // @ref Expr::Kind::FUNCTION_INVOCATION is an argument of @ref ExprProgram
    if (!exprEval->getExprs().empty()) {
        if (prevNode) {
            prevNode->connectOut(exprEval);
        }
        prevNode = exprEval;
    }

    if (!funcEval->getFuncs().empty()) {
        if (prevNode) {
            prevNode->connectOut(funcEval);
        }
        prevNode = funcEval;
    }

    if (!aggregateEval->getFuncs().empty()) {
        if (prevNode) {
            prevNode->connectOut(aggregateEval);
        }
        prevNode = aggregateEval;
    }

    // ORDER BY, SKIP, LIMIT require a previous input, `LIMIT 10` is not a valid query,
    // but `MATCH (n) LIMIT 10` is (because it has SCAN NODES as a previous input), and so
    // is `RETURN 5 LIMIT 10` (it has EXPR EVAL as a previous input). Therefore, we can
    // only add thse projection properties if @ref prevNode is valid.
    if (prevNode && proj->hasOrderBy()) {
        OrderByNode* orderBy = _tree.newOut<OrderByNode>(prevNode);
        orderBy->setItems(proj->getOrderBy()->getItems());
        prevNode = orderBy;
    }

    if (prevNode && proj->hasSkip()) {
        SkipNode* skip = _tree.newOut<SkipNode>(prevNode);
        skip->setExpr(proj->getSkip()->getExpr());
        prevNode = skip;
    }

    if (prevNode && proj->hasLimit()) {
        LimitNode* limit = _tree.newOut<LimitNode>(prevNode);
        limit->setExpr(proj->getLimit()->getExpr());
        prevNode = limit;
    }

    prevNode->connectOut(results);

    results->setProjection(proj);

    return results;
}

PlanGraphNode* PlanGraphGenerator::generateReturnNone(PlanGraphNode* prevNode) {
    ProduceResultsNode* prodResults = _tree.newOut<ProduceResultsNode>(prevNode);
    prodResults->setProduceNone(true);
    return prodResults;
}

void PlanGraphGenerator::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
