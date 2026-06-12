#include "PlanGraphGenerator.h"

#include "BioAssert.h"
#include "EmbeddingsSpec.h"
#include "FunctionInvocation.h"
#include "GetPropertyCache.h"
#include "NodePattern.h"
#include "Projection.h"
#include "QualifiedName.h"
#include "ReturnStmtGenerator.h"
#include "Symbol.h"
#include "decl/DeclContext.h"
#include "expr/BinaryExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"
#include "ExprDependencies.h"
#include "nodes/ChangeNode.h"
#include "nodes/CommitNode.h"
#include "Literal.h"
#include "stmt/Limit.h"
#include "stmt/ReturnStmt.h"
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
#include "nodes/LoadCommitNode.h"
#include "nodes/ExprEvalNode.h"
#include "nodes/CreateVectorIndexNode.h"
#include "nodes/LoadVectorNode.h"
#include "nodes/LoadEmbeddingNode.h"
#include "nodes/DeleteVectorIndexNode.h"
#include "nodes/ShowVectorIndexesNode.h"
#include "nodes/InstallExtensionNode.h"
#include "nodes/ShowExtensionsNode.h"
#include "nodes/CreatePropertyIndexNode.h"
#include "nodes/DropIndexNode.h"
#include "nodes/MergeDataPartsNode.h"

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
#include "LoadEmbeddingQuery.h"
#include "DeleteVectorIndexQuery.h"
#include "ShowVectorIndexesQuery.h"
#include "LoadCommitQuery.h"
#include "InstallExtensionQuery.h"
#include "ShowExtensionsQuery.h"
#include "CreateNodePropertyIndexQuery.h"
#include "CreateEdgePropertyIndexQuery.h"
#include "DropIndexQuery.h"
#include "MergeDataPartsQuery.h"

#include "decl/VarDecl.h"
#include "decl/PatternData.h"

#include "PlannerException.h"

using namespace db;

namespace {

int64_t getLimit(const Projection* proj) {
    const Expr* limitExpr = proj->getLimit()->getExpr();

    const bool isLiteral = limitExpr->getKind() == Expr::Kind::LITERAL;
    bioassert(isLiteral, "Planner failed to stop non-literal LIMIT.");

    const auto* litExpr = static_cast<const LiteralExpr*>(limitExpr);
    const Literal* lit = litExpr->getLiteral();

    const bool isIntegral = lit->getKind() == Literal::Kind::INTEGER;
    bioassert(isIntegral, "Planned failed to stop non-integral LIMIT.");

    const auto* intLit = static_cast<const IntegerLiteral*>(lit);
    const int64_t limit = intLit->getValue();

    return limit;
}

}

PlanGraphGenerator::PlanGraphGenerator(const PlanGenConfig* config,
                                       const CypherAST& ast,
                                       const GraphView& view)
    : _ast(&ast),
    _view(view),
    _config(config),
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

        case QueryCommand::Kind::LOAD_EMBEDDING_QUERY:
            generateLoadEmbeddingQuery(static_cast<const LoadEmbeddingQuery*>(query));
        break;

        case QueryCommand::Kind::DELETE_VECTOR_INDEX_QUERY:
            generateDeleteVectorIndexQuery(static_cast<const DeleteVectorIndexQuery*>(query));
        break;

        case QueryCommand::Kind::SHOW_VECTOR_INDEXES_QUERY:
            generateShowVectorIndexesQuery(static_cast<const ShowVectorIndexesQuery*>(query));
        break;

        case QueryCommand::Kind::LOAD_COMMIT_QUERY:
            generateLoadCommitQuery(static_cast<const LoadCommitQuery*>(query));
        break;

        case QueryCommand::Kind::INSTALL_EXTENSION_QUERY:
            generateInstallExtensionQuery(static_cast<const InstallExtensionQuery*>(query));
        break;

        case QueryCommand::Kind::SHOW_EXTENSIONS_QUERY:
            generateShowExtensionsQuery(static_cast<const ShowExtensionsQuery*>(query));
        break;

        case QueryCommand::Kind::CREATE_NODE_PROPERTY_INDEX_QUERY:
            generateCreateNodePropertyIndexQuery(static_cast<const CreateNodePropertyIndexQuery*>(query));
        break;

        case QueryCommand::Kind::CREATE_EDGE_PROPERTY_INDEX_QUERY:
            generateCreateEdgePropertyIndexQuery(static_cast<const CreateEdgePropertyIndexQuery*>(query));
        break;

        case QueryCommand::Kind::DROP_INDEX_QUERY:
            generateDropIndexQuery(static_cast<const DropIndexQuery*>(query));
        break;

        case QueryCommand::Kind::MERGE_DATAPARTS_QUERY:
            generateMergeDataPartsQuery(static_cast<const MergeDataPartsQuery*>(query));
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
    const ReturnStmt* returnStmt = query->getReturnStmt();
    const DeclContext* declCtxt = query->getDeclContext();

    PlanGraphNode* currentNode = nullptr;

    // Generate read statements (optional)
    if (readStmts) {
        ReadStmtGenerator readGenerator(_ast, _view, _config, &_tree, _variables.get(), declCtxt);

        // Pass literal LIMIT to the read generator for join planning
        if (returnStmt) {
            const Projection* proj = returnStmt->getProjection();
            if (proj && proj->hasLimit()) {
                const int64_t limit = getLimit(proj);
                readGenerator.setQueryLimit(limit);
            }
        }

        for (const Stmt* stmt : readStmts->stmts()) {
            readGenerator.generateStmt(stmt);
        }

        // Place joins on vars that have more than one input
        readGenerator.placeJoinsOnVars();

        // Place joins based on predicates
        readGenerator.placePredicates();

        // Place joins based on procedures calls
        readGenerator.placeJoinsOnProcedures();

        // Place joins that generate the endpoint, and retrieve it
        currentNode = readGenerator.generateEndpoint();
    }

    // Generate update statements (optional)
    if (updateStmts) {
        WriteStmtGenerator writeGenerator(_ast, &_tree, _variables.get(), currentNode);

        for (const Stmt* stmt : updateStmts->stmts()) {
            currentNode = writeGenerator.generateStmt(stmt);
            // Keep the write stmt generator _prevNode up to date
            writeGenerator.setPrevNode(currentNode);
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
    EmbeddingsSpec specs(query->getEmbeddingSpecs());
    LoadJsonlNode* n = _tree.create<LoadJsonlNode>(query->getFilePath(), query->getGraphName(), std::move(specs));
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

void PlanGraphGenerator::generateLoadEmbeddingQuery(const LoadEmbeddingQuery* query) {
    LoadEmbeddingNode* node = _tree.create<LoadEmbeddingNode>(
        query->getFilePath(),
        query->getPropertyName());
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


void PlanGraphGenerator::generateLoadCommitQuery(const LoadCommitQuery* query) {
    LoadCommitNode* node = _tree.create<LoadCommitNode>(query->getHashStr());
    _tree.newOut<ProduceResultsNode>(node);
}

void PlanGraphGenerator::generateInstallExtensionQuery(const InstallExtensionQuery* query) {
    auto* node = _tree.create<InstallExtensionNode>(query->getExtensionName());
    _tree.newOut<ProduceResultsNode>(node);
}

void PlanGraphGenerator::generateShowExtensionsQuery(const ShowExtensionsQuery* query) {
    ShowExtensionsNode* node = _tree.create<ShowExtensionsNode>();
    _tree.newOut<ProduceResultsNode>(node);
}

PlanGraphNode* PlanGraphGenerator::generateReturnStmt(const ReturnStmt* stmt,
                                                      PlanGraphNode* prevNode) {
    GetPropertyCache& propCache = _tree.getGetPropertyCache();

    ReturnStmtGenerator stmtGen(_ast,
                                _variables.get(),
                                stmt,
                                &_tree,
                                prevNode,
                                propCache);

    PlanGraphNode* returnProjectionNode = stmtGen.generateReturnStmt();

    return returnProjectionNode;
}

PlanGraphNode* PlanGraphGenerator::generateReturnNone(PlanGraphNode* prevNode) {
    ProduceResultsNode* prodResults = _tree.newOut<ProduceResultsNode>(prevNode);
    prodResults->setProduceNone(true);
    return prodResults;
}

void PlanGraphGenerator::generateCreateNodePropertyIndexQuery(const CreateNodePropertyIndexQuery* query) {
    const std::string_view indexName = query->indexName();
    const PropertyExpr* propExpr = query->propertyExpr();

    auto* node = _tree.create<CreatePropertyIndexNode>(indexName, IndexEntityKind::Node, propExpr);

    _tree.newOut<ProduceResultsNode>(node);
}

void PlanGraphGenerator::generateCreateEdgePropertyIndexQuery(const CreateEdgePropertyIndexQuery* query) {
    const std::string_view indexName = query->indexName();
    const PropertyExpr* propExpr = query->propertyExpr();

    auto* node = _tree.create<CreatePropertyIndexNode>(indexName, IndexEntityKind::Edge, propExpr);

    _tree.newOut<ProduceResultsNode>(node);
}

void PlanGraphGenerator::generateDropIndexQuery(const DropIndexQuery* query) {
    const std::string_view indexName = query->indexName();

    auto* node = _tree.create<DropIndexNode>(indexName);

    _tree.newOut<ProduceResultsNode>(node);
}

void PlanGraphGenerator::generateMergeDataPartsQuery(const MergeDataPartsQuery* query) {
    auto* n = _tree.create<MergeDataPartsNode>();
    _tree.newOut<ProduceResultsNode>(n);
}

void PlanGraphGenerator::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
