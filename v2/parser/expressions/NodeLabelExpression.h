#pragma once

#include <memory>
#include <vector>

#include "Expression.h"
#include "Symbol.h"

namespace db {

class NodeLabelExpression : public Expression {
public:
    NodeLabelExpression() = delete;
    ~NodeLabelExpression() override = default;

    NodeLabelExpression(const NodeLabelExpression&) = delete;
    NodeLabelExpression(NodeLabelExpression&&) = delete;
    NodeLabelExpression& operator=(const NodeLabelExpression&) = delete;
    NodeLabelExpression& operator=(NodeLabelExpression&&) = delete;

    NodeLabelExpression(Symbol&& symbol, std::vector<std::string>&& labels)
        : Expression(ExpressionType::NodeLabel),
          _symbol(std::move(symbol)),
          _labels(std::move(labels))
    {
    }

    const Symbol& symbol() const { return _symbol; }
    const std::vector<std::string>& labels() const { return _labels; }

    static std::unique_ptr<NodeLabelExpression> create(Symbol&& symbol,
                                              std::vector<std::string>&& labels) {
        return std::make_unique<NodeLabelExpression>(std::move(symbol), std::move(labels));
    }

private:
    Symbol _symbol;
    std::vector<std::string> _labels;
};

}
