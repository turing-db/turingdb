#pragma once

#include <memory>
#include <vector>

#include "Expression.h"
#include "metadata/LabelSet.h"
#include "types/Symbol.h"

namespace db {

class VarDecl;

class NodeLabelExpression : public Expression {
public:
    using LabelVector = std::vector<std::string_view>;

    NodeLabelExpression() = delete;
    ~NodeLabelExpression() override = default;

    NodeLabelExpression(const NodeLabelExpression&) = delete;
    NodeLabelExpression(NodeLabelExpression&&) = delete;
    NodeLabelExpression& operator=(const NodeLabelExpression&) = delete;
    NodeLabelExpression& operator=(NodeLabelExpression&&) = delete;

    NodeLabelExpression(const Symbol& symbol, LabelVector&& labels)
        : Expression(ExpressionType::NodeLabel),
        _symbol(symbol),
        _labelVector(std::move(labels))
    {
    }

    bool hasDecl() const { return _decl != nullptr; }

    const VarDecl& decl() const { return *_decl; }
    const Symbol& symbol() const { return _symbol; }
    const LabelVector& labelNames() const { return _labelVector; }
    const LabelSet& labels() const { return _labelset; }
    LabelSet& labels() { return _labelset; }

    void setDecl(const VarDecl* decl) { _decl = decl; }

    static std::unique_ptr<NodeLabelExpression> create(const Symbol& symbol,
                                                       LabelVector&& labels) {
        return std::make_unique<NodeLabelExpression>(symbol, std::move(labels));
    }

private:
    Symbol _symbol;
    LabelVector _labelVector;
    LabelSet _labelset;
    const VarDecl* _decl {nullptr};
};

}
