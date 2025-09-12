#pragma once

#include "QueryCommand.h"

namespace db::v2 {

class CypherAST;
class StmtContainer;
class ReturnStmt;
class DeclContext;

class SinglePartQuery : public QueryCommand {
public:
    static SinglePartQuery* create(CypherAST* ast);

    const StmtContainer* getReadStmts() const { return _readStmts; }
    const StmtContainer* getUpdateStmts() const { return _updateStmts; }
    const ReturnStmt* getReturnStmt() const { return _returnStmt; }

    void setReadStmts(StmtContainer* stmts) { _readStmts = stmts; }
    void setUpdateStmts(StmtContainer* stmts) { _updateStmts = stmts; }
    void setReturnStmt(ReturnStmt* stmt) { _returnStmt = stmt; }

private:
    StmtContainer* _readStmts {nullptr};
    StmtContainer* _updateStmts {nullptr};
    ReturnStmt* _returnStmt {nullptr};

    SinglePartQuery(DeclContext* declContext);
    ~SinglePartQuery() override;
};

}
