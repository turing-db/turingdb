#pragma once

#include <memory>

#include "Expression.h"
#include "types/QualifiedName.h"

namespace db::v2 {

class VarDecl;

class PropertyExpression : public Expression {
public:
    PropertyExpression() = delete;
    ~PropertyExpression() override = default;

    PropertyExpression(const QualifiedName& name)
        : Expression(ExpressionType::Property),
        _name(name)
    {
    }

    PropertyExpression(const PropertyExpression&) = delete;
    PropertyExpression(PropertyExpression&&) = delete;
    PropertyExpression& operator=(const PropertyExpression&) = delete;
    PropertyExpression& operator=(PropertyExpression&&) = delete;

    bool hasDecl() const { return _decl != nullptr; }

    const VarDecl& decl() const { return *_decl; }
    const QualifiedName& name() const { return _name; }

    void setDecl(const VarDecl* decl) { _decl = decl; }

    static std::unique_ptr<PropertyExpression> create(const QualifiedName& name) {
        return std::make_unique<PropertyExpression>(name);
    }

private:
    QualifiedName _name;
    const VarDecl* _decl {nullptr};
};

}
