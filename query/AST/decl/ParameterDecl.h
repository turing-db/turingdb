#pragma once

#include <optional>
#include <string_view>

#include "decl/EvaluatedType.h"

namespace db {

class CypherAST;
class ParameterValue;

// One per parameter name in a query: every $name reads it, so a use that narrows the
// types the parameter can take narrows them for all the others
class ParameterDecl {
public:
    friend CypherAST;

    static ParameterDecl* create(CypherAST* ast, std::string_view name, const ParameterValue& value);

    // The type the analyzer gives @param value: Null for a null, which names no value type
    static EvaluatedType evaluatedTypeOf(const ParameterValue& value);

    std::string_view getName() const { return _name; }

    const ParameterValue& getValue() const { return *_value; }
    std::optional<ValueType> getValueType() const;
    EvaluatedType getType() const { return _type; }

    EvaluatedTypeSet getCandidateTypes() const { return _candidateTypes; }

    void narrow(EvaluatedTypeSet accepted) { _candidateTypes &= accepted; }

private:
    std::string_view _name;
    const ParameterValue* _value {nullptr};
    EvaluatedType _type {EvaluatedType::Invalid};
    EvaluatedTypeSet _candidateTypes {PARAMETER_VALUE_TYPES};

    ParameterDecl(std::string_view name, const ParameterValue& value);
    ~ParameterDecl();
};

}
