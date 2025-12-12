#pragma once

#include "views/GraphView.h"

namespace db::v2 {

class CypherAST;
class ReadStmtAnalyzer;
class WriteStmtAnalyzer;
class ExprAnalyzer;
class SinglePartQuery;
class LoadGraphQuery;
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

    // Sub-statements
    void analyze(OrderBy* orderBySt);
    void analyze(Skip* skipSt);
    void analyze(Limit* limitSt);

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    const GraphMetadata& _graphMetadata;

    std::unique_ptr<ExprAnalyzer> _exprAnalyzer;
    std::unique_ptr<ReadStmtAnalyzer> _readAnalyzer;
    std::unique_ptr<WriteStmtAnalyzer> _writeAnalyzer;

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
