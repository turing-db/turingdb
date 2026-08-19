#pragma once

#include <vector>

namespace db {

class Expr;

class ExprChildren {
public:
    ExprChildren() = delete;
    ~ExprChildren() = delete;

    static bool collect(const Expr* expr, std::vector<const Expr*>& children);
};

}
