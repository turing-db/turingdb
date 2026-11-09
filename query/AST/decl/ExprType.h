#pragma once

#include "decl/EvaluatedType.h"
#include "decl/EvaluatedTypeSet.h"
#include "decl/ListShape.h"

namespace db {

// What the analyzer knows about the value of an expression or a variable. The common type
// is the one every value of an Any converts to without loss, Any where none does. The held
// types are the ones its values can have, for an Any a selection made its branches' types.
struct ExprType {
    EvaluatedType _type {EvaluatedType::Invalid};
    ListShape _shape;
    EvaluatedType _commonType {EvaluatedType::Any};
    EvaluatedTypeSet _heldTypes {EvaluatedType::Any};

    EvaluatedTypeSet getHeldTypes() const { return _type == EvaluatedType::Any ? _heldTypes : EvaluatedTypeSet(_type); }
};

}
