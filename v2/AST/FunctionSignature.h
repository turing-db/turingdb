#pragma once

#include <vector>

#include "decl/EvaluatedType.h"

namespace db::v2 {

struct FunctionSignature {
    std::string_view _fullName;
    std::vector<EvaluatedType> _argumentTypes;
    std::vector<EvaluatedType> _returnTypes;
    bool _isAggregate {false};
    bool _isDatabaseProcedure {false};
};

}

