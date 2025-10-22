#pragma once

#include <vector>
#include <stdint.h>

namespace db::v2 {

class CypherAST;
class Expr;

class ExprTree {
public:
    enum class Flags : uint8_t {
        None = 0,
        Aggregate = 1 << 0,
        Dynamic = 1 << 1,
    };

    static ExprTree* create(CypherAST* ast, Expr* expr);

    void addExpr(Expr* expr) {
        _exprs.push_back(expr);
    }

    [[nodiscard]] bool isAggregate() const {
        return ((uint8_t)_flags & (uint8_t)Flags::Aggregate) != 0;
    }

    [[nodiscard]] bool isDynamic() const {
        return ((uint8_t)_flags & (uint8_t)Flags::Dynamic) != 0;
    }

    void setAggregate() {
        _flags = (Flags)((uint8_t)_flags | (uint8_t)Flags::Aggregate);
    }

    void setDynamic() {
        _flags = (Flags)((uint8_t)_flags | (uint8_t)Flags::Dynamic);
    }

private:
    friend CypherAST;

    std::vector<Expr*> _exprs;
    Flags _flags {Flags::None};

    ExprTree() = default;
    ~ExprTree() = default;
};

}
