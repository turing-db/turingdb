#pragma once

#include <vector>

#include "Stmt.h"

namespace db {

class CypherAST;
class SinglePartQuery;
class Symbol;

// CALL (a, b) { ... }: a query of its own, run once per row in flight, reading the
// variables it imports and nothing else of the scope around it
class CallSubqueryStmt final : public Stmt {
public:
    using Imports = std::vector<const Symbol*>;

    static CallSubqueryStmt* create(CypherAST* ast, SinglePartQuery* body);

    Kind getKind() const final { return Kind::CALL_SUBQUERY; }

    SinglePartQuery* getBody() const { return _body; }
    const Imports& imports() const { return _imports; }

    bool hasScopeClause() const { return _hasScopeClause; }
    bool isOptional() const { return _optional; }

    // A body ending on RETURN adds its columns to the rows in flight; one ending on an
    // updating clause passes them through unchanged
    bool isReturning() const;

    void addImport(const Symbol* symbol) { _imports.push_back(symbol); }
    void setHasScopeClause(bool hasScopeClause) { _hasScopeClause = hasScopeClause; }
    void setOptional(bool optional) { _optional = optional; }

private:
    SinglePartQuery* _body {nullptr};
    Imports _imports;
    bool _hasScopeClause {false};
    bool _optional {false};

    explicit CallSubqueryStmt(SinglePartQuery* body);
    ~CallSubqueryStmt() final;
};

}
