#pragma once

#include <string_view>

#include "decl/EvaluatedType.h"

namespace db {

class CypherAST;

// One per parameter name in a query: every $name reads it, so a use that narrows the
// types the parameter can take narrows them for all the others
class ParameterDecl {
public:
    friend CypherAST;

    static ParameterDecl* create(CypherAST* ast, std::string_view name);

    std::string_view getName() const { return _name; }

    EvaluatedTypeSet getCandidateTypes() const { return _candidateTypes; }

    void narrow(EvaluatedTypeSet accepted) { _candidateTypes &= accepted; }

private:
    std::string_view _name;
    EvaluatedTypeSet _candidateTypes {PARAMETER_VALUE_TYPES};

    explicit ParameterDecl(std::string_view name);
    ~ParameterDecl();
};

}
