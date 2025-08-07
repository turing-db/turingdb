#pragma once

#include <memory>

#include "Expression.h"
#include "types/Literal.h"
#include "types/Symbol.h"

namespace db {

class VarDecl;

class SymbolExpression : public Expression {
public:
    ~SymbolExpression() override = default;

    explicit SymbolExpression(const Symbol& symbol)
        : Expression(ExpressionType::Symbol),
        _symbol(symbol)
    {
    }

    SymbolExpression(const SymbolExpression&) = delete;
    SymbolExpression(SymbolExpression&&) = delete;
    SymbolExpression& operator=(const SymbolExpression&) = delete;
    SymbolExpression& operator=(SymbolExpression&&) = delete;

    static std::unique_ptr<SymbolExpression> create(const Symbol& symbol) {
        return std::make_unique<SymbolExpression>(symbol);
    }

    bool hasVar() const {
        return _var != nullptr;
    }

    const Symbol& symbol() const {
        return _symbol;
    }

    const VarDecl& var() const {
        return *_var;
    }

    VarDecl& var() {
        return *_var;
    }

    void setDecl(VarDecl& var) {
        _var = &var;
    }

private:
    Symbol _symbol;
    VarDecl* _var {nullptr};
};

}
