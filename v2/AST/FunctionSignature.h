#pragma once

#include "decl/EvaluatedType.h"

namespace db::v2 {

struct FunctionSignature {
    std::string_view _fullName;
    std::vector<EvaluatedType> _argumentTypes;
    EvaluatedType _returnType {};
    bool _isAggregate {false};
};

}

