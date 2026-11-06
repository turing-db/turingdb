#pragma once

#include <optional>
#include <string_view>

#include "decl/EvaluatedType.h"
#include "decl/ListShape.h"

namespace db {

class CypherAST;
class ParameterValue;

// One per parameter name in a query: every $name reads it, and it holds the type of
// the value bound to the parameter and, for a list, the shape of that list
class ParameterDecl {
public:
    friend CypherAST;

    static ParameterDecl* create(CypherAST* ast, std::string_view name, const ParameterValue& value, const ListShape& listShape);

    // The type the analyzer gives @param value: Null for a null, which names no value type
    static EvaluatedType evaluatedTypeOf(const ParameterValue& value);

    std::string_view getName() const { return _name; }

    const ParameterValue& getValue() const { return *_value; }
    std::optional<ValueType> getValueType() const;
    EvaluatedType getType() const { return _type; }
    const ListShape& getListShape() const { return _listShape; }

private:
    std::string_view _name;
    const ParameterValue* _value {nullptr};
    EvaluatedType _type {EvaluatedType::Invalid};
    ListShape _listShape;

    ParameterDecl(std::string_view name, const ParameterValue& value, const ListShape& listShape);
    ~ParameterDecl();
};

}
