#pragma once

#include <memory>
#include <variant>

#include "Literal.h"
#include "Parameter.h"
#include "Symbol.h"
#include "Expression.h"

namespace db {

template <typename T>
concept IsAtomExpression = std::is_same_v<T, Symbol>
                        || std::is_same_v<T, Literal>
                        || std::is_same_v<T, Parameter>;

class AtomExpression : public Expression {
public:
    using ValueType = std::variant<Symbol, Literal, Parameter>;

    AtomExpression() = delete;
    ~AtomExpression() override = default;

    explicit AtomExpression(ValueType&& symbol)
        : Expression(ExpressionType::Atom),
          _value(std::move(symbol)) {
    }

    AtomExpression(const AtomExpression&) = delete;
    AtomExpression(AtomExpression&&) = delete;
    AtomExpression& operator=(const AtomExpression&) = delete;
    AtomExpression& operator=(AtomExpression&&) = delete;

    static std::unique_ptr<AtomExpression> create(ValueType&& v) {
        return std::make_unique<AtomExpression>(std::move(v));
    }

    const ValueType& value() const {
        return _value;
    }

    ValueType& value() {
        return _value;
    }

private:
    ValueType _value;
};

}
