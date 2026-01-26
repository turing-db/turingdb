#pragma once

#include "Expr.h"

#include <vector>

namespace db {

class CypherAST;

class ListExpr : public Expr {
public:
    using Elements = std::vector<Expr*>;

    void addItem(Expr* expr) { _elements.push_back(expr); }

    const Elements& getElements() const { return _elements; }
    size_t size() const { return _elements.size(); }
    bool empty() const { return _elements.empty(); }

    static ListExpr* create(CypherAST* ast);

    Elements::const_iterator begin() const { return _elements.begin(); }
    Elements::const_iterator end() const { return _elements.end(); }

private:
    Elements _elements;

    ListExpr()
        : Expr(Kind::LIST)
    {
    }

    ~ListExpr() override;
};

}
