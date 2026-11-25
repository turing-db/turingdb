#pragma once

#include "mlir/IR/Value.h"

#include "DBOps.h"

namespace mlir::db {

// The unwind factor of a cross product whose elements reach nothing but one equality
// against a column of the other factor, and the ops that equality is rebuilt from.
struct UnwindEqualityCross {
    CrossProduct _product {nullptr};
    UnwindConst _unwind {nullptr};
    Region* _relationFactor {nullptr};
    bool _unwindOnTheLeft {false};
    bool _readsTheIDs {false};
    bool _readsTheElement {false};
    Value _unwoundColumn;
    Value _comparedColumn;
    EntityIDOf _converted {nullptr};
    EqOp _equality {nullptr};
    FilterOp _filter {nullptr};
};

bool matchUnwindEqualityCross(CrossProduct product, UnwindEqualityCross& match);

}
