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
    using AtomType = std::variant<Symbol, Literal, Parameter>;

    AtomExpression() = delete;
    ~AtomExpression() override = default;

    explicit AtomExpression(const AtomType& atom)
        : Expression(ExpressionType::Atom),
        _atom(atom)
    {
    }

    AtomExpression(const AtomExpression&) = delete;
    AtomExpression(AtomExpression&&) = delete;
    AtomExpression& operator=(const AtomExpression&) = delete;
    AtomExpression& operator=(AtomExpression&&) = delete;

    static std::unique_ptr<AtomExpression> create(const AtomType& atom) {
        return std::make_unique<AtomExpression>(atom);
    }

    const AtomType& value() const {
        return _atom;
    }

    AtomType& atom() {
        return _atom;
    }

private:
    AtomType _atom;
};

}
