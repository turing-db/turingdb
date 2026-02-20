#pragma once

#include <algorithm>
#include <numeric>
#include <vector>

#include <range/v3/view/transform.hpp>

#include "BioAssert.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"
#include "range/v3/algorithm/sort.hpp"

namespace db {

namespace rg = ranges;
namespace rv = rg::views;

struct SortedRun {
    size_t _start {0};
    size_t _size {0};
};

struct CompareInner {
    size_t _i {0};
    size_t _j {0};
    int& _res;

    template <typename T>
    void operator()(ColumnVector<T>* col) {
        auto cmp = col->operator[](_i) <=> col->operator[](_j);
        if (cmp < 0) {
            _res = -1;
        }
        if (cmp > 0) {
            _res = 1;
        }
        _res = 0;
    }
};

using Compare = ColumnSingleDispatcher<OrderedTypes::Allowed, CompareInner>;

using NamedCols = std::vector<NamedColumn*>;

inline void merge(std::vector<size_t>& indices, NamedCols& ncols, SortedRun& run1, SortedRun& run2) {
    bioassert(run1._start == 0, "run1 did not start from start.");
    bioassert(run1._start + run1._size == run2._size,
              "run2 did not start from end of run1");

    auto cols = ncols
                | rv::transform([&](NamedColumn* ncol) { return ncol->getColumn(); });

    auto rowLess = [&](size_t i, size_t j) -> bool {
        int comparisonResult = 0;
        CompareInner cmp{._i = i, ._j = j, ._res = comparisonResult};
        for (Column* col : cols) {
            Compare::dispatch(col, cmp);

            const bool equalInThisCol = comparisonResult != 0;
            if (!equalInThisCol) {
                return comparisonResult < 0;
            }
        }
        // All rows equal
        return false;
    };

    auto run1Start = begin(indices) + run1._start;
    auto run1End = run1Start + run1._size;

    auto run2Start = begin(indices) + run2._start;
    auto run2End = run2Start + run2._size;

    std::inplace_merge(run1Start, run1End, run2End, rowLess);
}

}
