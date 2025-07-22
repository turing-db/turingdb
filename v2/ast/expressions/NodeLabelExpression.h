#pragma once

#include <memory>
#include <vector>

#include "Expression.h"
#include "types/Symbol.h"

namespace db {

class NodeLabelExpression : public Expression {
public:
    NodeLabelExpression() = delete;
    ~NodeLabelExpression() override = default;

    NodeLabelExpression(const NodeLabelExpression&) = delete;
    NodeLabelExpression(NodeLabelExpression&&) = delete;
    NodeLabelExpression& operator=(const NodeLabelExpression&) = delete;
    NodeLabelExpression& operator=(NodeLabelExpression&&) = delete;

    NodeLabelExpression(const Symbol& symbol, std::vector<std::string_view>&& labels)
        : Expression(ExpressionType::NodeLabel),
        _symbol(symbol),
        _labels(std::move(labels))
    {
    }

    const Symbol& symbol() const { return _symbol; }
    const std::vector<std::string_view>& labels() const { return _labels; }

    static std::unique_ptr<NodeLabelExpression> create(const Symbol& symbol,
                                                       std::vector<std::string_view>&& labels) {
        return std::make_unique<NodeLabelExpression>(symbol, std::move(labels));
    }

private:
    Symbol _symbol;
    std::vector<std::string_view> _labels;
};

}
