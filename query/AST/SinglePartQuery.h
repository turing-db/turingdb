#pragma once

#include "QueryCommand.h"

namespace db {

class CypherAST;
class StmtContainer;
class ReturnStmt;
class DeclContext;
class ShortestPathStmt;

class SinglePartQuery : public QueryCommand {
public:
    static SinglePartQuery* create(CypherAST* ast);

    Kind getKind() const override { return Kind::SINGLE_PART_QUERY; }

    const StmtContainer* getReadStmts() const { return _readStmts; }
    const StmtContainer* getUpdateStmts() const { return _updateStmts; }
    const ShortestPathStmt* getShortestPathStmt() const {
        return _shortestPathStmt;
    }
    const ReturnStmt* getReturnStmt() const { return _returnStmt; }

    void setReadStmts(StmtContainer* stmts) { _readStmts = stmts; }
    void setUpdateStmts(StmtContainer* stmts) { _updateStmts = stmts; }
    void setShortestPathStmt(ShortestPathStmt* stmt) { _shortestPathStmt = stmt; }
    void setReturnStmt(ReturnStmt* stmt) { _returnStmt = stmt; }

private:
    StmtContainer* _readStmts {nullptr};
    StmtContainer* _updateStmts {nullptr};
    ShortestPathStmt* _shortestPathStmt {nullptr};
    ReturnStmt* _returnStmt {nullptr};

    SinglePartQuery(DeclContext* declContext);
    ~SinglePartQuery() override;
};
} // namespace db
