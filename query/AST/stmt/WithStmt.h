#pragma once

#include "Stmt.h"

namespace db {

class Projection;
class WhereClause;
class CypherAST;

// A WITH clause: a projection that does not end the query but replaces its scope. The
// statements after it read the columns this projection publishes and nothing else, and
// its WHERE filters those rows once the projection - aggregation, DISTINCT, ORDER BY,
// SKIP and LIMIT included - has produced them.
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
