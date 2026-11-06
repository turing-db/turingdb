#pragma once

#include <optional>

#include "Expr.h"

#include "metadata/PropertyType.h"
#include "metadata/ValueComponent.h"

namespace db {

class CypherAST;

// A property read off the value of an expression rather than off a variable:
// startNode(r).name, or datetime('...').year.
class PropertyLookupExpr : public Expr {
public:
    static PropertyLookupExpr* create(CypherAST* ast, Expr* base, std::string_view propName);

    Expr* getBase() const { return _base; }
    std::string_view getPropName() const { return _propName; }

    ValueType getValueType() const { return _valueType; }
    void setValueType(ValueType valueType) { _valueType = valueType; }

    const std::optional<ValueComponent>& getComponent() const { return _component; }
    void setComponent(ValueComponent component) { _component = component; }

    bool readsAMapKey() const { return _readsAMapKey; }
    void setReadsAMapKey();

private:
    Expr* _base {nullptr};
    std::string_view _propName;
    ValueType _valueType {ValueType::Invalid};
    std::optional<ValueComponent> _component;
    bool _readsAMapKey {false};

    PropertyLookupExpr(Expr* base, std::string_view propName);

    ~PropertyLookupExpr() override;
};

}
