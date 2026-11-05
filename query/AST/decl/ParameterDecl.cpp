#include "ParameterDecl.h"

#include "CypherAST.h"
#include "ParameterValue.h"

using namespace db;

ParameterDecl::ParameterDecl(std::string_view name, const ParameterValue& value)
    : _name(name),
    _value(&value),
    _type(evaluatedTypeOf(value))
{
}

ParameterDecl::~ParameterDecl() {
}

ParameterDecl* ParameterDecl::create(CypherAST* ast, std::string_view name, const ParameterValue& value) {
    ParameterDecl* decl = new ParameterDecl(name, value);
    ast->addParameterDecl(decl);

    return decl;
}

std::optional<ValueType> ParameterDecl::getValueType() const {
    if (_value->isNull()) {
        return std::nullopt;
    }

    return _value->getType();
}

EvaluatedType ParameterDecl::evaluatedTypeOf(const ParameterValue& value) {
    return value.isNull() ? EvaluatedType::Null : *toEvaluatedType(value.getType());
}
