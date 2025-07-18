#pragma once

#include <memory>

#include "Expression.h"
#include "types/QualifiedName.h"

namespace db {

class PropertyExpression : public Expression {
public:
    PropertyExpression() = delete;
    ~PropertyExpression() override = default;

    PropertyExpression(QualifiedName&& name)
        : Expression(ExpressionType::Property),
          _name(std::move(name))
    {
    }

    PropertyExpression(const PropertyExpression&) = delete;
    PropertyExpression(PropertyExpression&&) = delete;
    PropertyExpression& operator=(const PropertyExpression&) = delete;
    PropertyExpression& operator=(PropertyExpression&&) = delete;

    const QualifiedName& name() { return _name; }

    static std::unique_ptr<PropertyExpression> create(QualifiedName&& name) {
        return std::make_unique<PropertyExpression>(std::move(name));
    }

private:
    QualifiedName _name;
};

}
