#pragma once

#include <vector>

namespace db::v2 {

class CypherAST;
class Expr;

class ExprChain {
public:
    ~ExprChain();

    static ExprChain* create(CypherAST* ast);

    void add(Expr* expr) { _exprs.push_back(expr); }

    const std::vector<Expr*>& getExprs() const { return _exprs; }

    std::vector<Expr*>::const_iterator begin() const { return _exprs.begin(); }
    std::vector<Expr*>::const_iterator end() const { return _exprs.end(); }

private:
    std::vector<Expr*> _exprs;

    ExprChain();
};

}
