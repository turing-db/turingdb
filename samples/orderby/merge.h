#pragma once

#include <algorithm>
#include <vector>

#include <range/v3/view/transform.hpp>

#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"

#include "BioAssert.h"
#include "range/v3/view/drop.hpp"

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
    void operator()(const ColumnVector<T>* col) {
        const auto cmp = col->operator[](_i) <=> col->operator[](_j);
        if (cmp < 0) {
            _res = -1;
        } else if (cmp > 0) {
            _res = 1;
        } else {
            _res = 0;
        }
    }
};

using Compare = ColumnSingleDispatcher<OrderedTypes::Allowed, CompareInner, OrderedTypes::Excluded>;

using NamedCols = std::vector<NamedColumn*>;

inline void mergeAdj(std::vector<size_t>& indices,
                     const NamedCols& ncols,
                     const SortedRun& run1,
                     const SortedRun& run2) {
    bioassert(run1._start == 0, "run1 did not start from start.");
    bioassert(run1._start + run1._size == run2._start,
              "run2 did not start from end of run1");

    auto cols = ncols
                | rv::transform([&](NamedColumn* ncol) { return ncol->getColumn(); });

    auto rowLess = [&](size_t i, size_t j) -> bool {
        int comparisonResult = 0;
        CompareInner cmp{._i = i, ._j = j, ._res = comparisonResult};
        for (Column* col : cols) {
            Compare::dispatch(col, cmp);

            if (comparisonResult != 0) {
                return comparisonResult < 0;
            }
            // If equal, check next column
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

inline void merge(std::vector<size_t>& indices, const NamedCols& ncols, const std::vector<SortedRun>& runs) {
    if (runs.empty()) {
        return;
    }

    SortedRun merged = runs.front();

    for (const SortedRun& run : runs | rv::drop(1)) {
        const size_t thisRunSize = run._size;

        mergeAdj(indices, ncols, merged, run);

        merged._size += thisRunSize;
    }
}

}
