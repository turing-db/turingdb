#pragma once

#include <vector>

#include "decl/EvaluatedType.h"

namespace db::v2 {

struct FunctionReturnType {
    EvaluatedType _type;
    std::string_view _name;
};

struct FunctionSignature {
    std::string_view _fullName;
    std::vector<EvaluatedType> _argumentTypes;
    std::vector<FunctionReturnType> _returnTypes;
    bool _isAggregate {false};
    bool _isDatabaseProcedure {false};
};

}

