#pragma once

#include "AtomicExpression.h"
#include <optional>
#include <string>

namespace db {

class PropertyExpression : public AtomicExpression {
public:
    PropertyExpression(ExpressionType type)
        : AtomicExpression(type) {}

    ~PropertyExpression() override = default;

    PropertyExpression(const PropertyExpression&) = delete;
    PropertyExpression(PropertyExpression&&) = delete;
    PropertyExpression& operator=(const PropertyExpression&) = delete;
    PropertyExpression& operator=(PropertyExpression&&) = delete;

private:
    std::optional<std::string> _nodeLabels;
};

}
