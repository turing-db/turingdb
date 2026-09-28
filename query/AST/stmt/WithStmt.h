#pragma once

#include <vector>

#include "Stmt.h"

namespace db {

class Projection;
class WhereClause;
class CypherAST;
class VarDecl;

// A projection that does not end the query but replaces its scope: the statements after
// it read the columns it publishes and nothing else
class WithStmt final : public Stmt {
public:
    static WithStmt* create(CypherAST* ast, Projection* projection);

    Kind getKind() const final { return Kind::WITH; }

    Projection* getProjection() const { return _projection; }
    const WhereClause* getWhere() const { return _where; }

    // The variables the WHERE reads that the projection drops
    const std::vector<const VarDecl*>& filterImports() const { return _filterImports; }

    void setWhere(WhereClause* where) { _where = where; }
    void addFilterImport(const VarDecl* decl) { _filterImports.push_back(decl); }

private:
    Projection* _projection {nullptr};
    WhereClause* _where {nullptr};
    std::vector<const VarDecl*> _filterImports;

    explicit WithStmt(Projection* projection);
    ~WithStmt() final;
};

}
