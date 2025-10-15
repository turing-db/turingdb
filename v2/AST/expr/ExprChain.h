#pragma once

#include <vector>

namespace db::v2 {

class CypherAST;
class Expr;

class ExprChain {
public:
    using ExprVector = std::vector<Expr*>;

    ~ExprChain();

    static ExprChain* create(CypherAST* ast);

    void add(Expr* expr) { _exprs.push_back(expr); }

    const ExprVector& getExprs() const { return _exprs; }

    ExprVector::const_iterator begin() const { return _exprs.begin(); }
    ExprVector::const_iterator end() const { return _exprs.end(); }

private:
    ExprVector _exprs;

    ExprChain();
};

}
