#pragma once

#include "Expr.h"
#include "Operators.h"

#include <stdint.h>
#include <vector>

namespace db {

class CypherAST;

// A Cypher CASE, in each of its forms. The generic form tests a predicate per branch
// (CASE WHEN c THEN r ... END); the simple form compares a subject against a value per
// branch (CASE s WHEN v THEN r ... END), which is the same thing with s = v as the
// predicate; the extended simple form lets a branch carry its own comparator instead of
// that equality (CASE s WHEN < v THEN r ... END). A branch holds one test per value it
// lists and is taken when any of them holds, the ELSE when none does, and null when there
// is no ELSE.
class CaseExpr : public Expr {
public:
    enum class TestKind : uint8_t {
        Value,
        Comparison,
        IsNull,
        IsNotNull,
    };

    // One WHEN value and how the subject is compared against it. The generic form has no
    // subject, and carries its predicate as a Value test.
    struct Test {
        Expr* _value {nullptr};
        BinaryOperator _operator {BinaryOperator::Equal};
        TestKind _kind {TestKind::Value};
    };

    using Tests = std::vector<Test>;

    struct Branch {
        Tests _tests;
        Expr* _then {nullptr};
    };

    using Branches = std::vector<Branch>;

    Expr* getSubject() const { return _subject; }
    Expr* getElseExpr() const { return _elseExpr; }

    const Branches& getBranches() const { return _branches; }

    void setSubject(Expr* subject) { _subject = subject; }
    void setElseExpr(Expr* elseExpr) { _elseExpr = elseExpr; }

    void addBranch(const Tests& tests, Expr* then);

    static CaseExpr* create(CypherAST* ast);

private:
    Branches _branches;
    Expr* _subject {nullptr};
    Expr* _elseExpr {nullptr};

    CaseExpr();
    ~CaseExpr() override;
};

}
