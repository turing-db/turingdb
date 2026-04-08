#include "ApplyMask.h"

#include "columns/AllowedKinds.h"
#include "columns/ColumnCombinations.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/MaskOperators.h"

using namespace db;

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

void ApplyMask::eval(Column* res, const Column* arg, const Column* mask) {
    using Pairs = MaskedPairs;
    using Dispatcher = ColumnDoubleDispatcher<Pairs::Allowed,
                                              Pairs::AllowedMixed,
                                              Eval,
                                              Pairs::Excluded>;
    Eval fn {res};
    Dispatcher::dispatch(arg, mask, fn);
}
