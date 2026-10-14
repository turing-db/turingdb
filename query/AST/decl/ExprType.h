#pragma once

#include "decl/EvaluatedType.h"
#include "decl/ListShape.h"

namespace db {

// What the analyzer knows about the value of an expression or a variable. The candidates
// are the types a Parameter-typed value can take once its parameters are bound. The common
// type is the one every value of an Any converts to without loss, Any where none does.
struct ExprType {
    EvaluatedType _type {EvaluatedType::Invalid};
    EvaluatedTypeSet _candidates;
    ListShape _shape;
    EvaluatedType _commonType {EvaluatedType::Any};
};

}
