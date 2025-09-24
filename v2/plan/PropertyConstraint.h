#pragma once

#include "ExprDependencies.h"
#include "ID.h"

namespace db::v2 {

class Expr;

struct PropertyConstraint {
    VarNode* var {nullptr};
    PropertyTypeID type;
    Expr* expr {nullptr};
    ExprDependencies dependencies;
};

}
