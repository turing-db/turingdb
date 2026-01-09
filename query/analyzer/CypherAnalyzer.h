#pragma once

#include "views/GraphView.h"

namespace db {

class CypherAST;
class ReadStmtAnalyzer;
class WriteStmtAnalyzer;
class ExprAnalyzer;
class QueryCommand;
class SinglePartQuery;
class LoadGraphQuery;
class CreateGraphQuery;
class ChangeQuery;
class LoadGMLQuery;
class LoadNeo4jQuery;
class LoadJsonlQuery;
class ChangeQuery;
class S3ConnectQuery;
class S3TransferQuery;
class OrderBy;
class Skip;
class Limit;
class ReturnStmt;

class CypherAnalyzer {
public:
    CypherAnalyzer(CypherAST* ast, GraphView graphView);
    ~CypherAnalyzer();

    CypherAnalyzer(const CypherAnalyzer&) = delete;
    CypherAnalyzer(CypherAnalyzer&&) = delete;
    CypherAnalyzer& operator=(const CypherAnalyzer&) = delete;
    CypherAnalyzer& operator=(CypherAnalyzer&&) = delete;

    CypherAST* getAST() const { return _ast; }

    void analyze();

    // Query types
    void analyze(const SinglePartQuery* query);
    void analyze(const ReturnStmt* returnSt);
    void analyze(const LoadGraphQuery* loadGraph);
    void analyze(const CreateGraphQuery* createGraph);
    void analyze(LoadGMLQuery* loadGML);
    void analyze(LoadNeo4jQuery* loadNeo4j);
    void analyze(LoadJsonlQuery* loadJsonl);
    void analyze(const S3ConnectQuery* s3Connect);
    void analyze(S3TransferQuery* s3Transfer);

    // Sub-statements
    void analyze(OrderBy* orderBySt);
    void analyze(Skip* skipSt);
    void analyze(Limit* limitSt);

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    const GraphMetadata& _graphMetadata;
    const QueryCommand* _currentQuery {nullptr};

    std::unique_ptr<ExprAnalyzer> _exprAnalyzer;
    std::unique_ptr<ReadStmtAnalyzer> _readAnalyzer;
    std::unique_ptr<WriteStmtAnalyzer> _writeAnalyzer;

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
