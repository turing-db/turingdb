#pragma once

#include <memory>

#include "Expression.h"
#include "types/Literal.h"
#include "types/Parameter.h"

namespace db::v2 {

class ParameterExpression : public Expression {
public:
    ParameterExpression() = delete;
    ~ParameterExpression() override = default;

    explicit ParameterExpression(const Parameter& param)
        : Expression(ExpressionType::Parameter),
        _param(param)
    {
    }

    ParameterExpression(const ParameterExpression&) = delete;
    ParameterExpression(ParameterExpression&&) = delete;
    ParameterExpression& operator=(const ParameterExpression&) = delete;
    ParameterExpression& operator=(ParameterExpression&&) = delete;

    static std::unique_ptr<ParameterExpression> create(const Parameter& param) {
        return std::make_unique<ParameterExpression>(param);
    }

    const Parameter& param() const {
        return _param;
    }

private:
    Parameter _param;
};

}
