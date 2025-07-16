#pragma once

namespace db {

enum class ExpressionType {
    Binary = 0,
    Unary,
    String,
    NodeLabel,
    Property,
    Atom
};


class Expression {
public:
    explicit Expression(ExpressionType type)
        : _type(type) {}

    Expression() = delete;
    virtual ~Expression() = default;

    Expression(const Expression&) = delete;
    Expression(Expression&&) = delete;
    Expression& operator=(const Expression&) = delete;
    Expression& operator=(Expression&&) = delete;

    ExpressionType type() const { return _type; }

private:
    ExpressionType _type {};
};

}
