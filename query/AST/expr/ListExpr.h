#pragma once

#include "Expr.h"

#include <vector>

namespace db {

class CypherAST;

class ListExpr : public Expr {
public:
    static ListExpr* create(CypherAST* ast);

    void addItem(Expr* expr) { _elements.push_back(expr); }

    const std::vector<Expr*>& getElements() const { return _elements; }
    size_t size() const { return _elements.size(); }
    bool empty() const { return _elements.empty(); }

    std::vector<Expr*>::const_iterator begin() const { return _elements.begin(); }
    std::vector<Expr*>::const_iterator end() const { return _elements.end(); }

private:
    std::vector<Expr*> _elements;

    ListExpr()
        : Expr(Kind::LIST)
    {
    }

    ~ListExpr() override;
};

}
