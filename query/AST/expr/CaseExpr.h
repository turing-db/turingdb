#pragma once

#include "Expr.h"

#include <vector>

namespace db {

class CypherAST;

// A Cypher CASE, in both of its forms. The generic form tests a predicate per branch
// (CASE WHEN c THEN r ... END); the simple form compares a subject against a value per
// branch (CASE s WHEN v THEN r ... END), which is the same thing with s = v as the
// predicate. An unmatched row takes the ELSE branch, and null when there is none.
class CaseExpr : public Expr {
public:
    struct Branch {
        Expr* _when {nullptr};
        Expr* _then {nullptr};
    };

    using Branches = std::vector<Branch>;

    Expr* getSubject() const { return _subject; }
    Expr* getElseExpr() const { return _elseExpr; }

    const Branches& getBranches() const { return _branches; }

    void setSubject(Expr* subject) { _subject = subject; }
    void setElseExpr(Expr* elseExpr) { _elseExpr = elseExpr; }

    void addBranch(Expr* when, Expr* then);

    static CaseExpr* create(CypherAST* ast);

private:
    Branches _branches;
    Expr* _subject {nullptr};
    Expr* _elseExpr {nullptr};

    CaseExpr();
    ~CaseExpr() override;
};

}
