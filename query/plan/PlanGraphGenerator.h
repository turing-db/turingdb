#pragma once

#include <string_view>

#include "PlanGraph.h"

namespace db {
class GraphView;
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
class LoadNeo4jQuery;
class LoadJsonlQuery;
class S3ConnectQuery;
class S3TransferQuery;
class ShowProceduresQuery;
class QueryCommand;

class PlanGraphGenerator {
public:
    PlanGraphGenerator(const CypherAST& ast,
                       const GraphView& view);
    ~PlanGraphGenerator();

    PlanGraph& getPlanGraph() { return _tree; }

    void generate(const QueryCommand* query);

private:
    const CypherAST* _ast {nullptr};
    const GraphView& _view;
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
    void generateLoadNeo4jQuery(const LoadNeo4jQuery* query);
    void generateLoadJsonlQuery(const LoadJsonlQuery* query);
    void generateS3ConnectQuery(const S3ConnectQuery* query);
    void generateS3TransferQuery(const S3TransferQuery* query);
    void generateShowProceduresQuery(const ShowProceduresQuery* query);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};
}
