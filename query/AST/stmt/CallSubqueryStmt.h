#pragma once

#include <vector>

#include "Stmt.h"

namespace db {

class CypherAST;
class RegularQuery;
class Symbol;

// CALL (a, b) { ... }: a query of its own, run once per row in flight, reading the
// variables it imports and nothing else of the scope around it
class CallSubqueryStmt final : public Stmt {
public:
    using Imports = std::vector<const Symbol*>;

    static CallSubqueryStmt* create(CypherAST* ast, RegularQuery* body);

    Kind getKind() const final { return Kind::CALL_SUBQUERY; }

    RegularQuery* getBody() const { return _body; }

    // What the scope clause names, which every query of the body imports. A body with no
    // scope clause imports through the leading WITH of each of its single queries.
    const Imports& getImports() const { return _imports; }

    bool hasScopeClause() const { return _hasScopeClause; }
    bool importsEverything() const { return _importsEverything; }
    bool isOptional() const { return _optional; }

    // A body ending on RETURN adds its columns to the rows in flight; one ending on an
    // updating clause passes them through unchanged
    bool isReturning() const;

    bool writesToTheGraph() const;
    bool readsTheGraph() const;

    void addImport(const Symbol* symbol) { _imports.push_back(symbol); }

    void setHasScopeClause(bool hasScopeClause) { _hasScopeClause = hasScopeClause; }
    void setImportsEverything(bool importsEverything) { _importsEverything = importsEverything; }
    void setOptional(bool optional) { _optional = optional; }

private:
    RegularQuery* _body {nullptr};
    Imports _imports;
    bool _hasScopeClause {false};
    bool _importsEverything {false};
    bool _optional {false};

    explicit CallSubqueryStmt(RegularQuery* body);
    ~CallSubqueryStmt() final;
};

}
