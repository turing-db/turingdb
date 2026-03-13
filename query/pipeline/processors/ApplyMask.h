#pragma once

#include "columns/AllowedKinds.h"
#include "columns/ColumnCombinations.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/MaskOperators.h"

namespace db {

namespace {

struct Eval {
    Column* _res {nullptr};
    
    template <typename T, typename U>
    void operator()(const T* arg, const U* mask) {
        bioassert(_res && arg && mask, "Invalid operands to evaluate mask");

        using ResultType = ColumnCombination<Apply, T, U>::ResultColumnType;
        auto* result = dynamic_cast<ResultType*>(_res);
        MaskApplicator::apply(result, arg, mask);
    }
};

}

class Column;

class ApplyMask {
public:
    static void eval(Column* res, const Column* arg, const Column* mask);
};

inline void ApplyMask::eval(Column* res, const Column* arg, const Column* mask) {
    using Pairs = MaskedPairs;
    Eval fn {res};
    using Dispatcher = ColumnDoubleDispatcher<Pairs::Allowed,
                                              Pairs::AllowedMixed,
                                              Eval,
                                              Pairs::Excluded>;
    Dispatcher::dispatch(arg, mask, fn);
}

}
