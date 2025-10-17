#pragma once

#include <stdint.h>

namespace db::v2 {

enum class OrderByType : uint8_t {
    ASC = 0,
    DESC
};

class CypherAST;
class Expr;

class OrderByItem {
public:
    static OrderByItem* create(CypherAST* ast, Expr* expr, OrderByType type = OrderByType::ASC);

    Expr* getExpr() const { return _expr; }

private:
    Expr* _expr {nullptr};
    OrderByType _type {OrderByType::ASC};

    OrderByItem(Expr* expr, OrderByType type)
        : _expr(expr),
        _type(type)
    {
    }
};

}
