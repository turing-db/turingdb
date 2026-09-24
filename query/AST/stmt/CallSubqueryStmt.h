#pragma once

#include <stddef.h>
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

    // One query of the body - the whole body, or one side of a UNION in it - with the
    // operator joining it to the ones before it and the variables it imports. Each branch
    // imports through a leading WITH of its own, so the imports are held per branch.
    struct Branch {
        SinglePartQuery* _query {nullptr};
        bool _all {false};
        Imports _imports;
    };

    using Branches = std::vector<Branch>;

    static CallSubqueryStmt* create(CypherAST* ast, const Branches& branches);

    Kind getKind() const final { return Kind::CALL_SUBQUERY; }

    const Branches& branches() const { return _branches; }
    Branches& branches() { return _branches; }

    bool isUnion() const { return _branches.size() > 1; }

    // How many leading branches dedup against one another, as UnionQuery counts them
    size_t getDedupedBranchCount() const;

    bool hasScopeClause() const { return _hasScopeClause; }
    bool isOptional() const { return _optional; }

    // A body ending on RETURN adds its columns to the rows in flight; one ending on an
    // updating clause passes them through unchanged
    bool isReturning() const;

    bool writesToTheGraph() const;
    bool readsTheGraph() const;

    // What the scope clause names is imported by every branch
    void addImport(const Symbol* symbol);

    void setHasScopeClause(bool hasScopeClause) { _hasScopeClause = hasScopeClause; }
    void setOptional(bool optional) { _optional = optional; }

private:
    Branches _branches;
    bool _hasScopeClause {false};
    bool _optional {false};

    explicit CallSubqueryStmt(const Branches& branches);
    ~CallSubqueryStmt() final;
};

}
