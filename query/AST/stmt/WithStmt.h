#pragma once

#include "Stmt.h"

namespace db {

class Projection;
class WhereClause;
class CypherAST;

// A projection that does not end the query but replaces its scope: the statements after
// it read the columns it publishes and nothing else
class WithStmt final : public Stmt {
public:
    static WithStmt* create(CypherAST* ast, Projection* projection);

    Kind getKind() const final { return Kind::WITH; }

    Projection* getProjection() const { return _projection; }
    const WhereClause* getWhere() const { return _where; }

    void setWhere(WhereClause* where) { _where = where; }

private:
    Projection* _projection {nullptr};
    WhereClause* _where {nullptr};

    explicit WithStmt(Projection* projection);
    ~WithStmt() final;
};

}
