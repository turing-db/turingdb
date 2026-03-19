#pragma once

#include "Expr.h"

#include <vector>

namespace db {

class CypherAST;

class ListExpr : public Expr {
public:
    using Elements = std::vector<Expr*>;

    size_t size() const { return _elements.size(); }
    bool empty() const { return _elements.empty(); }

    const Elements& getElements() const { return _elements; }

    Elements::const_iterator begin() const { return _elements.begin(); }
    Elements::const_iterator end() const { return _elements.end(); }

    void addItem(Expr* expr);

    static ListExpr* create(CypherAST* ast);

private:
    Elements _elements;

    ListExpr();
    ~ListExpr() override;
};

}
