#pragma once

#include "QueryCommand.h"

namespace db {

class CypherAST;
class StmtContainer;
class ReturnStmt;
class DeclContext;
class ShortestPathStmt;
class Stmt;

class SinglePartQuery : public QueryCommand {
public:
    static SinglePartQuery* create(CypherAST* ast);

    Kind getKind() const override { return Kind::SINGLE_PART_QUERY; }

    const StmtContainer* getStmts() const { return _stmts; }
    const ReturnStmt* getReturnStmt() const { return _returnStmt; }

    void setStmts(StmtContainer* stmts) { _stmts = stmts; }
    void addStmt(Stmt* stmt);

    void setReturnStmt(ReturnStmt* stmt) { _returnStmt = stmt; }

private:
    StmtContainer* _stmts {nullptr};
    ReturnStmt* _returnStmt {nullptr};

    SinglePartQuery(DeclContext* declContext);
    ~SinglePartQuery() override;
};
} // namespace db
