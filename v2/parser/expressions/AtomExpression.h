#pragma once

#include <memory>
#include <variant>

#include "Expression.h"
#include "types/Literal.h"
#include "types/Parameter.h"
#include "types/Symbol.h"

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

    explicit AtomExpression(const ValueType& symbol)
        : Expression(ExpressionType::Atom),
          _value(symbol)
    {
    }

    AtomExpression(const AtomExpression&) = delete;
    AtomExpression(AtomExpression&&) = delete;
    AtomExpression& operator=(const AtomExpression&) = delete;
    AtomExpression& operator=(AtomExpression&&) = delete;

    static std::unique_ptr<AtomExpression> create(const ValueType& v) {
        return std::make_unique<AtomExpression>(v);
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
