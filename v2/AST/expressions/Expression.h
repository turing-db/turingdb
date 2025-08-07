#pragma once

#include "decl/EvaluatedType.h"

namespace db {

enum class ExpressionType {
    Binary = 0,
    Unary,
    String,
    NodeLabel,
    Property,
    Path,
    Symbol,
    Literal,
    Parameter,
};

class Expression {
public:
    explicit Expression(ExpressionType type)
        : _exprKind(type)
    {
    }

    Expression() = delete;
    virtual ~Expression() = 0;

    Expression(const Expression&) = delete;
    Expression(Expression&&) = delete;
    Expression& operator=(const Expression&) = delete;
    Expression& operator=(Expression&&) = delete;

    ExpressionType kind() const { return _exprKind; }

    template <typename T>
    T* as() {
        return dynamic_cast<T*>(this);
    }

    template <typename T>
    const T* as() const {
        return dynamic_cast<const T*>(this);
    }

    void setType(EvaluatedType type) { _valueType = type; }

    EvaluatedType type() const { return _valueType; }

private:
    ExpressionType _exprKind {};
    EvaluatedType _valueType {};
};

inline Expression::~Expression() = default;

}
