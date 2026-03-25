#pragma once

#include <string_view>

#include "PlanGraph.h"

namespace db {
class GraphView;
class PlanGenConfig;
}

namespace db {

class CypherAST;
class PlanGraph;
class PlanGraphVariables;
class SinglePartQuery;
class ChangeQuery;
class CommitQuery;
class ReturnStmt;
class ShortestPathStmt;
class LoadGraphQuery;
class ListGraphQuery;
class CreateGraphQuery;
class LoadGMLQuery;
class LoadJsonlQuery;
class LoadCommitQuery;
class S3ConnectQuery;
class S3TransferQuery;
class ShowProceduresQuery;
class CreateVectorIndexQuery;
class LoadVectorQuery;
class DeleteVectorIndexQuery;
class ShowVectorIndexesQuery;
class InstallExtensionQuery;
class ShowExtensionsQuery;
class QueryCommand;
class CreatePropertyIndexQuery;

class PlanGraphGenerator {
public:
    PlanGraphGenerator(const CypherAST& ast,
                       const GraphView& view,
                       const PlanGenConfig* config);
    ~PlanGraphGenerator();

    PlanGraph& getPlanGraph() { return _tree; }

    void generate(const QueryCommand* query);

private:
    const CypherAST* _ast {nullptr};
    const GraphView& _view;
    const PlanGenConfig* _config {nullptr};
    PlanGraph _tree;
    std::unique_ptr<PlanGraphVariables> _variables;

    void generateChangeQuery(const ChangeQuery* query);
    void generateCommitQuery(const CommitQuery* query);
    void generateSinglePartQuery(const SinglePartQuery* query);
    void generateShortestPathStmt(const ShortestPathStmt* stmt, PlanGraphNode* prevNode);
    PlanGraphNode* generateReturnStmt(const ReturnStmt* stmt, PlanGraphNode* prevNode);
    PlanGraphNode* generateReturnNone(PlanGraphNode* prevNode);
    void generateLoadGraphQuery(const LoadGraphQuery* query);
    void generateListGraphQuery(const ListGraphQuery* query);
    void generateCreateGraphQuery(const CreateGraphQuery* query);
    void generateLoadGMLQuery(const LoadGMLQuery* query);
    void generateLoadJsonlQuery(const LoadJsonlQuery* query);
    void generateS3ConnectQuery(const S3ConnectQuery* query);
    void generateS3TransferQuery(const S3TransferQuery* query);
    void generateShowProceduresQuery(const ShowProceduresQuery* query);
    void generateCreateVectorIndexQuery(const CreateVectorIndexQuery* query);
    void generateLoadVectorQuery(const LoadVectorQuery* query);
    void generateDeleteVectorIndexQuery(const DeleteVectorIndexQuery* query);
    void generateShowVectorIndexesQuery(const ShowVectorIndexesQuery* query);
    void generateLoadCommitQuery(const LoadCommitQuery* query);
    void generateInstallExtensionQuery(const InstallExtensionQuery* query);
    void generateShowExtensionsQuery(const ShowExtensionsQuery* query);
    void generateCreatePropertyIndexQuery(const CreatePropertyIndexQuery* query);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};
}
