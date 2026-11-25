#pragma once

#include <vector>

#include "Projection.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"

namespace db {

// The variable a projection item publishes as it stands, or null for an item that computes
// a value from one: only the first keeps an entity, the second leaves it behind. A wildcard
// expands to the declarations themselves, anything written out to a bare symbol expression
const VarDecl* projectedVariable(const Projection::ReturnItem& item);

void flattenConjuncts(const Expr* expr, std::vector<const Expr*>& conjuncts);

}
